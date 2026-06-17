#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/planner/filter/optional_filter.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>

#include "pgddb/catalog/pgddb_table.hpp"
#include "pgddb/scan/postgres_scan.hpp"
#include "pgddb/scan/postgres_table_reader.hpp"
#include "pgddb/pgddb_types.hpp"
#include "pgddb/pgddb_utils.hpp"
#include "pgddb/pg/memory.hpp"
#include "pgddb/pg/relations.hpp"

#include "pgddb/pgddb_process_lock.hpp"
#include "pgddb/logger.hpp"

#include <numeric>
#include <optional>

namespace pgddb {

int duckdb_threads_for_postgres_scan = 2;

namespace {

inline std::optional<duckdb::string>
UnsupportedExpression(const char *reason, const duckdb::Expression &expr) {
	pd_log(DEBUG1, "Unsupported %s: %s", reason, expr.ToString().c_str());
	return std::nullopt;
}

std::optional<duckdb::string> ExpressionToString(const duckdb::Expression &expr, const duckdb::string &column_name);

duckdb::string
FilterJoin(duckdb::vector<duckdb::string> &filters, duckdb::string &&delimiter) {
	return std::accumulate(filters.begin() + 1, filters.end(), filters[0],
	                       [&delimiter](duckdb::string l, duckdb::string r) { return l + delimiter + r; });
}

// " ASC|DESC [NULLS FIRST|NULLS LAST]" suffix for one ORDER BY key. The direction
// is always spelled out so the emitted SQL and the EXPLAIN label are unambiguous.
duckdb::string
OrderSuffix(duckdb::OrderType order_type, duckdb::OrderByNullType null_order) {
	duckdb::string out = order_type == duckdb::OrderType::DESCENDING ? " DESC" : " ASC";
	if (null_order == duckdb::OrderByNullType::NULLS_FIRST) {
		out += " NULLS FIRST";
	} else if (null_order == duckdb::OrderByNullType::NULLS_LAST) {
		out += " NULLS LAST";
	}
	return out;
}

std::optional<duckdb::string>
FuncArgToLikeString(const duckdb::string &func_name, const duckdb::Expression &expr) {
	if (expr.type != duckdb::ExpressionType::VALUE_CONSTANT) {
		// Needle must be a literal VARCHAR so we can append the '%' wildcard for a PG LIKE pattern.
		return std::nullopt;
	}

	auto &value = expr.Cast<duckdb::BoundConstantExpression>().value;
	if (value.IsNull()) {
		return "NULL";
	} else if (value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
		return std::nullopt;
	}

	auto str_val = duckdb::StringUtil::Replace(value.ToString(), "'", "''");
	str_val = duckdb::StringUtil::Replace(str_val, "\\", "\\\\");
	str_val = duckdb::StringUtil::Replace(str_val, "%", "\\%");
	str_val = duckdb::StringUtil::Replace(str_val, "_", "\\_");
	if (func_name == "contains") {
		return "'%" + str_val + "%'";
	} else if (func_name == "suffix") {
		return "'%" + str_val + "'";
	} else if (func_name == "prefix") {
		return "'" + str_val + "%'";
	} else {
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, "Unsupported function: '" + func_name + "'");
	}
}

std::optional<duckdb::string>
FuncToLikeString(const duckdb::string &func_name, const duckdb::BoundFunctionExpression &func_expr,
                 const duckdb::string &column_name) {
	if (func_expr.children.size() < 2 || func_expr.children.size() > 3) {
		return UnsupportedExpression("function arg count", func_expr);
	}

	auto &haystack = *func_expr.children[0];
	auto &needle = *func_expr.children[1];

	if (haystack.return_type != duckdb::LogicalTypeId::VARCHAR) {
		return UnsupportedExpression("type for haystack", haystack);
	}

	auto haystack_str = ExpressionToString(haystack, column_name);
	if (!haystack_str) {
		return UnsupportedExpression("haystack expression", haystack);
	}

	auto needle_str = func_name == "like_escape" || func_name == "ilike_escape"
	                      ? ExpressionToString(needle, column_name)
	                      : FuncArgToLikeString(func_name, needle);
	if (!needle_str) {
		return UnsupportedExpression("needle expression", needle);
	}

	std::ostringstream oss;
	oss << *haystack_str;
	if (func_name == "ilike_escape") {
		oss << " ILIKE ";
	} else {
		oss << " LIKE ";
	}

	oss << *needle_str;

	if (func_expr.children.size() == 3) {
		auto &escape_char = *func_expr.children[2];
		auto escape_str = ExpressionToString(escape_char, column_name);
		if (!escape_str) {
			return UnsupportedExpression("escape character expression", escape_char);
		} else if (*escape_str != "'\\'") {
			oss << " ESCAPE " << *escape_str;
		}
	}
	return oss.str();
}

std::optional<duckdb::string>
ExpressionToString(const duckdb::Expression &expr, const duckdb::string &column_name) {
	switch (expr.type) {
	case duckdb::ExpressionType::OPERATOR_NOT: {
		auto &not_expr = expr.Cast<duckdb::BoundOperatorExpression>();
		auto arg_str = ExpressionToString(*not_expr.children[0], column_name);
		if (!arg_str) {
			return UnsupportedExpression("child expression in", expr);
		}
		return "NOT (" + *arg_str + ")";
	}

	case duckdb::ExpressionType::OPERATOR_IS_NULL:
	case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL: {
		auto &is_null_expr = expr.Cast<duckdb::BoundOperatorExpression>();
		auto arg_str = ExpressionToString(*is_null_expr.children[0], column_name);
		if (!arg_str) {
			return UnsupportedExpression("child expression in", expr);
		}
		auto operator_str = (expr.type == duckdb::ExpressionType::OPERATOR_IS_NULL ? "IS NULL" : "IS NOT NULL");
		return "(" + *arg_str + ") " + operator_str;
	}

	case duckdb::ExpressionType::COMPARE_EQUAL:
	case duckdb::ExpressionType::COMPARE_NOTEQUAL:
	case duckdb::ExpressionType::COMPARE_LESSTHAN:
	case duckdb::ExpressionType::COMPARE_GREATERTHAN:
	case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case duckdb::ExpressionType::COMPARE_DISTINCT_FROM:
	case duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM: {
		auto &comp_expr = expr.Cast<duckdb::BoundComparisonExpression>();
		auto arg0_str = ExpressionToString(*comp_expr.left, column_name);
		auto arg1_str = ExpressionToString(*comp_expr.right, column_name);
		if (!arg0_str || !arg1_str) {
			return UnsupportedExpression("child expression in", expr);
		}
		return "(" + *arg0_str + " " + duckdb::ExpressionTypeToOperator(expr.type) + " " + *arg1_str + ")";
	}

	case duckdb::ExpressionType::COMPARE_BETWEEN: {
		auto &between_expr = expr.Cast<duckdb::BoundBetweenExpression>();
		auto input_str = ExpressionToString(*between_expr.input, column_name);
		auto lower_str = ExpressionToString(*between_expr.lower, column_name);
		auto upper_str = ExpressionToString(*between_expr.upper, column_name);
		if (!input_str || !lower_str || !upper_str) {
			return UnsupportedExpression("child expression in", expr);
		}

		// Can't reuse BoundBetweenExpression's helpers here: they are non-const (duckdb/duckdb#17773).
		auto lower_comp = between_expr.lower_inclusive ? duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO
		                                               : duckdb::ExpressionType::COMPARE_GREATERTHAN;
		auto upper_comp = between_expr.upper_inclusive ? duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO
		                                               : duckdb::ExpressionType::COMPARE_LESSTHAN;

		return "((" + *input_str + " " + duckdb::ExpressionTypeToOperator(lower_comp) + " " + *lower_str + ") AND (" +
		       *input_str + " " + duckdb::ExpressionTypeToOperator(upper_comp) + " " + *upper_str + "))";
	}

		// IN/NOT IN are omitted on purpose: DuckDB rewrites IN into a hash join, so the expression never reaches here.

	case duckdb::ExpressionType::CONJUNCTION_AND:
	case duckdb::ExpressionType::CONJUNCTION_OR: {
		auto &comp_expr = expr.Cast<duckdb::BoundConjunctionExpression>();
		std::string query_filters;

		for (auto &child : comp_expr.children) {
			auto child_str = ExpressionToString(*child, column_name);
			if (!child_str) {
				return UnsupportedExpression("child expression in", expr);
			}
			if (!query_filters.empty()) {
				query_filters += " " + duckdb::ExpressionTypeToOperator(expr.type) + " ";
			}
			query_filters += *child_str;
		}
		return "(" + query_filters + ")";
	}

	case duckdb::ExpressionType::BOUND_FUNCTION: {
		auto &func_expr = expr.Cast<duckdb::BoundFunctionExpression>();
		const auto &func_name = func_expr.function.name;
		if (func_name == "contains" || func_name == "suffix" || func_name == "prefix" || func_name == "like_escape" ||
		    func_name == "ilike_escape") {
			return FuncToLikeString(func_name, func_expr, column_name);
		}

		if (func_name == "lower" || func_name == "upper") {
			auto child_str = ExpressionToString(*func_expr.children[0], column_name);
			if (!child_str) {
				return UnsupportedExpression("child expression in", expr);
			}
			return func_name + "(" + *child_str + ")";
		}

		if ((func_name == "~~" || func_name == "!~~") && func_expr.children.size() == 2 && func_expr.is_operator) {
			auto &haystack = *func_expr.children[0];
			if (haystack.return_type != duckdb::LogicalTypeId::VARCHAR) {
				return UnsupportedExpression("type for haystack", expr);
			}
			auto child_str0 = ExpressionToString(*func_expr.children[0], column_name);
			auto child_str1 = ExpressionToString(*func_expr.children[1], column_name);
			if (!child_str0 || !child_str1) {
				return UnsupportedExpression("child expression in", expr);
			}
			return child_str0->append(" OPERATOR(pg_catalog." + func_name + ") ").append(*child_str1);
		}

		return UnsupportedExpression("function", expr);
	}

	case duckdb::ExpressionType::BOUND_REF:
	case duckdb::ExpressionType::BOUND_COLUMN_REF:
		return column_name;

	case duckdb::ExpressionType::VALUE_CONSTANT: {
		auto &value = expr.Cast<duckdb::BoundConstantExpression>().value;
		if (value.IsNull()) {
			return "NULL";
		} else if (value.type().id() == duckdb::LogicalTypeId::VARCHAR) {
			return value.ToSQLString();
		} else {
			// Only VARCHAR constants are supported; that suffices for the LIKE and lower/upper pushdowns we do.
			return UnsupportedExpression("constant expression", expr);
		}
	}

	default:
		return UnsupportedExpression("expression type", expr);
	}
}

} // namespace

int
PostgresScanGlobalState::ExtractQueryFilters(duckdb::TableFilter *filter, const char *column_name,
                                             duckdb::string &query_filters, bool is_inside_optional_filter) {
	switch (filter->filter_type) {
	case duckdb::TableFilterType::CONSTANT_COMPARISON:
	case duckdb::TableFilterType::IS_NULL:
	case duckdb::TableFilterType::IS_NOT_NULL:
	case duckdb::TableFilterType::IN_FILTER: {
		query_filters += filter->ToString(column_name).c_str();
		return 1;
	}
	case duckdb::TableFilterType::CONJUNCTION_OR:
	case duckdb::TableFilterType::CONJUNCTION_AND: {
		auto conjuction_filter = reinterpret_cast<duckdb::ConjunctionFilter *>(filter);
		bool is_or = filter->filter_type == duckdb::TableFilterType::CONJUNCTION_OR;
		duckdb::vector<std::string> conjuction_child_filters;
		for (idx_t i = 0; i < conjuction_filter->child_filters.size(); i++) {
			std::string child_filter;
			if (ExtractQueryFilters(conjuction_filter->child_filters[i].get(), column_name, child_filter,
			                        is_inside_optional_filter)) {
				conjuction_child_filters.emplace_back(child_filter);
			} else if (is_or) {
				// Dropping one OR child would make the filter more restrictive; drop the whole OR
				// (duckdb/pg_duckdb#1025).
				pd_log(DEBUG1, "(DuckDB/ExtractQueryFilters) Dropping OR filter: %s",
				       filter->ToString(column_name).c_str());
				return 0;
			}
		}
		duckdb::string conjuction_delimiter = is_or ? " OR " : " AND ";
		if (conjuction_child_filters.size()) {
			query_filters += "(" + FilterJoin(conjuction_child_filters, std::move(conjuction_delimiter)) + ")";
		}
		return conjuction_child_filters.size();
	}
	case duckdb::TableFilterType::OPTIONAL_FILTER: {
		auto optional_filter = reinterpret_cast<duckdb::OptionalFilter *>(filter);
		return ExtractQueryFilters(optional_filter->child_filter.get(), column_name, query_filters, true);
	}
	case duckdb::TableFilterType::EXPRESSION_FILTER: {
		auto &expression_filter = filter->Cast<duckdb::ExpressionFilter>();
		query_filters += *ExpressionToString(*expression_filter.expr, column_name);
		return 1;
	}
	// DYNAMIC_FILTER (topN pushdown) and STRUCT_EXTRACT (struct_extract use) plus any future filter land here.
	case duckdb::TableFilterType::DYNAMIC_FILTER:
	case duckdb::TableFilterType::STRUCT_EXTRACT:
	default: {
		if (is_inside_optional_filter) {
			pd_log(DEBUG1, "(DuckDB/ExtractQueryFilters) Unsupported optional filter: %s",
			       filter->ToString(column_name).c_str());
			return 0;
		}
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR,
		                        "Invalid Filter Type: " + filter->ToString(column_name));
	}
	}
}

void
PostgresScanGlobalState::ConstructTableScanQuery(const duckdb::TableFunctionInitInput &input) {
	const auto &bind_data = input.bind_data->Cast<PostgresScanFunctionData>();
	if (input.column_ids.size() == 1 && input.column_ids[0] == UINT64_MAX) {
		scan_query << "SELECT COUNT(*) FROM " << pgddb::GenerateQualifiedRelationName(rel);
		count_tuples_only = true;
		return;
	}
	duckdb::vector<AttrNumber> col_idx_to_attno;
	auto natts = GetTupleDescNatts(table_tuple_desc);
	for (int i = 0; i < natts; i++) {
		if (AttIsDropped(GetAttr(table_tuple_desc, i))) {
			continue;
		}
		col_idx_to_attno.emplace_back(static_cast<AttrNumber>(i + 1));
	}
	// Read tuples in PG column order but output in DuckDB order; the map keys on attno to give us PG order.
	duckdb::map<AttrNumber, duckdb::idx_t> pg_column_order;
	duckdb::idx_t scan_index = 0;
	for (const auto &col_id : input.column_ids) {
		auto pg_column = col_idx_to_attno[col_id];
		pg_column_order[pg_column] = scan_index++;
	}

	auto table_filters = input.filters.get();

	std::vector<duckdb::pair<AttrNumber, duckdb::idx_t>> columns_to_scan;
	std::vector<duckdb::TableFilter *> column_filters(input.column_ids.size(), 0);
	duckdb::vector<duckdb::string> scan_column_names(input.column_ids.size());

	for (auto const &[att_num, duckdb_scanned_index] : pg_column_order) {
		columns_to_scan.emplace_back(att_num, duckdb_scanned_index);

		auto name_attr = pgddb::GetAttr(table_tuple_desc, att_num - 1);
		scan_column_names[duckdb_scanned_index] = pgddb::QuoteIdentifier(pgddb::GetAttName(name_attr));

		if (!table_filters) {
			continue;
		}

		auto column_filter_it = table_filters->filters.find(duckdb_scanned_index);
		if (column_filter_it != table_filters->filters.end()) {
			column_filters[duckdb_scanned_index] = column_filter_it->second.get();
		}
	}

	// Use projection_ids when filter-only columns can be dropped, else column_ids so all read columns flow up.
	if (input.CanRemoveFilterColumns()) {
		for (const auto &projection_id : input.projection_ids) {
			output_columns.emplace_back(col_idx_to_attno[input.column_ids[projection_id]]);
		}
	} else {
		for (const auto &column_id : input.column_ids) {
			output_columns.emplace_back(col_idx_to_attno[column_id]);
		}
	}

	scan_query << "SELECT ";

	bool first = true;
	for (auto const &attr_num : output_columns) {
		if (!first) {
			scan_query << ", ";
		}
		first = false;
		auto attr = pgddb::GetAttr(table_tuple_desc, attr_num - 1);
		scan_query << pgddb::QuoteIdentifier(pgddb::GetAttName(attr));
	}

	scan_query << " FROM " << pgddb::GenerateQualifiedRelationName(rel);

	duckdb::vector<duckdb::string> query_filters;
	for (auto const &[attr_num, duckdb_scanned_index] : columns_to_scan) {
		auto filter = column_filters[duckdb_scanned_index];
		if (!filter) {
			continue;
		}
		duckdb::string column_query_filters;
		const duckdb::string &col = scan_column_names[duckdb_scanned_index];
		if (ExtractQueryFilters(filter, col.c_str(), column_query_filters, false)) {
			query_filters.emplace_back(column_query_filters);
		}
	}

	if (query_filters.size()) {
		scan_query << " WHERE ";
		scan_query << FilterJoin(query_filters, " AND ");
	}

	if (!bind_data.order_bys.empty()) {
		scan_query << " ORDER BY ";
		bool first_order = true;
		for (auto const &order_spec : bind_data.order_bys) {
			if (order_spec.column_index >= scan_column_names.size()) {
				throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR,
				                        "Invalid ORDER BY column index for Postgres scan");
			}
			if (!first_order) {
				scan_query << ", ";
			}
			first_order = false;
			scan_query << scan_column_names[order_spec.column_index]
			           << OrderSuffix(order_spec.order_type, order_spec.null_order);
		}

		// A Top-N pushdown also carries LIMIT/OFFSET; only meaningful alongside ORDER BY.
		if (bind_data.limit.IsValid()) {
			scan_query << " LIMIT " << bind_data.limit.GetIndex();
			if (bind_data.offset > 0) {
				scan_query << " OFFSET " << bind_data.offset;
			}
		}
	}
}

PostgresScanGlobalState::PostgresScanGlobalState(Snapshot _snapshot, Relation _rel,
                                                 const duckdb::TableFunctionInitInput &input)
    : snapshot(_snapshot), rel(_rel), table_tuple_desc(pgddb::RelationGetDescr(rel)), count_tuples_only(false),
      output_columns(), total_row_count(0), registered_local_states(0), scan_query(),
      table_reader_global_state(nullptr), duckdb_scan_memory_ctx(nullptr), max_threads(1) {
	ConstructTableScanQuery(input);
	table_reader_global_state = duckdb::make_shared_ptr<PostgresTableReader>();
	table_reader_global_state->Init(scan_query.str().c_str(), count_tuples_only);
	// Dedicated PG memory context for temporary type-conversion allocations during scans.
	duckdb_scan_memory_ctx = pgddb::pg::MemoryContextCreate(CurrentMemoryContext, "DuckdbScanContext");

	// Only let DuckDB use multiple consumer threads when PG launched parallel workers and this isn't a count-only scan.
	if (table_reader_global_state->NumWorkersLaunched() > 0 && !count_tuples_only) {
		max_threads = duckdb_threads_for_postgres_scan;
	}

	pd_log(DEBUG1, "(DuckDB/PostgresSeqScanGlobalState) Running %" PRIu64 " threads: '%s'", (uint64_t)MaxThreads(),
	       scan_query.str().c_str());
}

bool
PostgresScanGlobalState::RegisterLocalState() {
	if (registered_local_states < 0) {
		return false;
	}
	registered_local_states++;
	return true;
}

void
PostgresScanGlobalState::UnregisterLocalState() {
	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	registered_local_states--;
	// Once the last local state is gone, clean up the reader and mark negative so none can register again.
	if (registered_local_states == 0) {
		registered_local_states = -1;
		table_reader_global_state->Cleanup();
	}
}

PostgresScanGlobalState::~PostgresScanGlobalState() {
}

PostgresScanLocalState::PostgresScanLocalState(PostgresScanGlobalState *_global_state)
    : global_state(_global_state), output_vector_size(0), exhausted_scan(false) {
	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	bool registered = global_state->RegisterLocalState();
	if (!registered) {
		return;
	}
	// Both single-thread and parallel scans stage tuples through these slots before the batched converter.
	for (int i = 0; i < LOCAL_STATE_SLOT_BATCH_SIZE; i++) {
		slots[i] = global_state->table_reader_global_state->InitTupleSlot();
	}
}

PostgresScanLocalState::~PostgresScanLocalState() {
}

PostgresScanFunctionData::PostgresScanFunctionData(Relation _rel, uint64_t _cardinality, Snapshot _snapshot)
    : complex_filters(), order_bys(), limit(), offset(0), rel(_rel), cardinality(_cardinality), snapshot(_snapshot) {
}

PostgresScanFunctionData::~PostgresScanFunctionData() {
}

static bool
PostgresScanPushdownExpression(duckdb::ClientContext &, const duckdb::LogicalGet &, duckdb::Expression &expr) {
	return ExpressionToString(expr, "dummy") != std::nullopt;
}

PostgresScanTableFunction::PostgresScanTableFunction()
    : TableFunction("pgduckdb_postgres_scan", {}, PostgresScanFunction, nullptr, PostgresScanInitGlobal,
                    PostgresScanInitLocal) {
	named_parameters["cardinality"] = duckdb::LogicalType::UBIGINT;
	named_parameters["relid"] = duckdb::LogicalType::UINTEGER;
	named_parameters["snapshot"] = duckdb::LogicalType::POINTER;
	projection_pushdown = true;
	filter_pushdown = true;
	filter_prune = true;
	cardinality = PostgresScanCardinality;
	pushdown_expression = PostgresScanPushdownExpression;
	to_string = ToString;
}

duckdb::InsertionOrderPreservingMap<duckdb::string>
PostgresScanTableFunction::ToString(duckdb::TableFunctionToStringInput &input) {
	auto &bind_data = input.bind_data->Cast<PostgresScanFunctionData>();
	duckdb::InsertionOrderPreservingMap<duckdb::string> result;
	result["Table"] = pgddb::GetRelationName(bind_data.rel);
	if (!bind_data.order_bys.empty()) {
		duckdb::vector<duckdb::string> order_descriptions;
		order_descriptions.reserve(bind_data.order_bys.size());
		for (auto const &order_spec : bind_data.order_bys) {
			duckdb::string description = order_spec.column_name.empty()
			                                 ? duckdb::string("#") + std::to_string(order_spec.column_index)
			                                 : order_spec.column_name;
			description += OrderSuffix(order_spec.order_type, order_spec.null_order);
			order_descriptions.push_back(std::move(description));
		}
		result["Order By"] = duckdb::StringUtil::Join(order_descriptions, ", ");
	}
	if (bind_data.limit.IsValid()) {
		duckdb::string limit_desc = std::to_string(bind_data.limit.GetIndex());
		if (bind_data.offset > 0) {
			limit_desc += " OFFSET " + std::to_string(bind_data.offset);
		}
		result["Limit"] = limit_desc;
	}
	return result;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
PostgresScanTableFunction::PostgresScanInitGlobal(duckdb::ClientContext &, duckdb::TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<PostgresScanFunctionData>();
	return duckdb::make_uniq<PostgresScanGlobalState>(bind_data.snapshot, bind_data.rel, input);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState>
PostgresScanTableFunction::PostgresScanInitLocal(duckdb::ExecutionContext &, duckdb::TableFunctionInitInput &,
                                                 duckdb::GlobalTableFunctionState *gstate) {
	auto global_state = reinterpret_cast<PostgresScanGlobalState *>(gstate);
	return duckdb::make_uniq<PostgresScanLocalState>(global_state);
}
void
SetOutputCardinality(duckdb::DataChunk &output, PostgresScanLocalState &local_state) {
	idx_t output_cardinality =
	    local_state.output_vector_size <= STANDARD_VECTOR_SIZE ? local_state.output_vector_size : STANDARD_VECTOR_SIZE;
	output.SetCardinality(output_cardinality);
	local_state.output_vector_size -= output_cardinality;
}

void
PostgresScanTableFunction::PostgresScanFunction(duckdb::ClientContext &, duckdb::TableFunctionInput &data,
                                                duckdb::DataChunk &output) {
	auto &local_state = data.local_state->Cast<PostgresScanLocalState>();
	auto &global_state = *local_state.global_state;

	if (local_state.exhausted_scan) {
		SetOutputCardinality(output, local_state);
		return;
	}

	local_state.output_vector_size = 0;

	D_ASSERT(STANDARD_VECTOR_SIZE % LOCAL_STATE_SLOT_BATCH_SIZE == 0);
	const size_t num_batches = STANDARD_VECTOR_SIZE / LOCAL_STATE_SLOT_BATCH_SIZE;
	auto &table_reader = *global_state.table_reader_global_state;

	if (global_state.count_tuples_only) {
		// COUNT(*): accumulate the partial counts into output cardinality; no columns to convert.
		std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
		uint64_t count = 0;
		while (table_reader.GetNextCount(&count)) {
			global_state.total_row_count += count;
			local_state.output_vector_size += count;
		}
		local_state.exhausted_scan = true;
	} else if (table_reader.NumWorkersLaunched() > 0) {
		// Parallel workers feed several DuckDB consumer threads: hold the lock only to copy a batch of worker
		// tuples (whose memory lives in transient shared queues) into per-slot buffers, then convert outside it.
		for (size_t batch_idx = 0; batch_idx < num_batches; batch_idx++) {
			size_t valid_slots = 0;
			{
				std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
				for (size_t i = 0; i < LOCAL_STATE_SLOT_BATCH_SIZE; i++) {
					if (!table_reader.GetNextMinimalWorkerTuple(local_state.minimal_tuple_buffer[i])) {
						local_state.exhausted_scan = true;
						break;
					}
					++valid_slots;
				}
			}
			for (size_t i = 0; i < valid_slots; i++) {
				MinimalTuple minimal_tuple = reinterpret_cast<MinimalTuple>(local_state.minimal_tuple_buffer[i].data());
				local_state.slots[i] = pgddb::ExecStoreMinimalTupleUnsafe(minimal_tuple, local_state.slots[i], false);
			}
			InsertTuplesIntoChunk(output, local_state, local_state.slots, valid_slots);
			if (local_state.exhausted_scan) {
				break;
			}
		}
	} else {
		// In-process scan: single consumer, so take the lock once for the whole chunk. Do NOT switch memory
		// contexts around the fetch -- ExecProcNode allocates per-tuple executor state (e.g. index scans) in the
		// caller context that must survive across calls (issues 796, 802); InsertTuplesIntoChunk switches into
		// the scratchpad itself where needed.
		std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
		for (size_t batch_idx = 0; batch_idx < num_batches; batch_idx++) {
			int valid_slots = table_reader.GetNextInProcessTuples(local_state.slots, LOCAL_STATE_SLOT_BATCH_SIZE);
			InsertTuplesIntoChunk(output, local_state, local_state.slots, valid_slots);
			if (valid_slots < LOCAL_STATE_SLOT_BATCH_SIZE) {
				local_state.exhausted_scan = true;
				break;
			}
		}
	}

	if (local_state.exhausted_scan) {
		global_state.UnregisterLocalState();
	}
	SetOutputCardinality(output, local_state);
}

duckdb::unique_ptr<duckdb::NodeStatistics>
PostgresScanTableFunction::PostgresScanCardinality(duckdb::ClientContext &, const duckdb::FunctionData *data) {
	auto &bind_data = data->Cast<PostgresScanFunctionData>();
	return duckdb::make_uniq<duckdb::NodeStatistics>(bind_data.cardinality, bind_data.cardinality);
}

} // namespace pgddb
