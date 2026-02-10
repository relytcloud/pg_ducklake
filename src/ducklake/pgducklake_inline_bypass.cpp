/*
 * Inline Bypass for DuckLake INSERT...SELECT UNNEST pattern
 *
 * This module optimizes the common ETL pattern:
 *   INSERT INTO ducklake_table SELECT UNNEST($1::type[]), UNNEST($2::type[]), ...
 *
 * Instead of going through DuckDB execution (which involves converting parameters
 * to DuckDB values, executing UNNEST, then converting back to SQL literals for
 * the inlined data table), we directly insert into the PostgreSQL inlined data
 * table using SPI with the original array Datums.
 */

/* DuckDB headers must come BEFORE postgres headers to avoid macro conflicts */
#include "pgduckdb/ducklake/pgducklake_inline_bypass.hpp"
#include "pgduckdb/ducklake/pgducklake_defs.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
}

namespace pgduckdb {

/* Custom scan methods for inline bypass */
static CustomScanMethods inline_bypass_scan_methods;
static CustomExecMethods inline_bypass_exec_methods;

/* State for the inline bypass custom scan */
struct InlineBypassState {
	CustomScanState css; /* must be first field */
	InlineBypassInfo info;
	bool executed;
	int64_t rows_inserted;
};

/*
 * Check if an expression is UNNEST(Param) where Param is an array type.
 * Returns the param index (0-based) if it matches, -1 otherwise.
 */
static int
GetUnnestParamIndex(Node *node, Oid *array_type_out, Oid *elem_type_out) {
	/* We're looking for a FuncExpr that is UNNEST */
	if (!IsA(node, FuncExpr)) {
		return -1;
	}

	FuncExpr *func = (FuncExpr *)node;

	/* Check if this is an UNNEST function - it should have a single array argument */
	char *funcname = get_func_name(func->funcid);
	if (!funcname || strcmp(funcname, "unnest") != 0) {
		pfree(funcname);
		return -1;
	}
	pfree(funcname);

	/* UNNEST should have exactly one argument */
	if (list_length(func->args) != 1) {
		return -1;
	}

	Node *arg = (Node *)linitial(func->args);

	/* The argument should be a Param (possibly with a type coercion) */
	Param *param = nullptr;

	if (IsA(arg, Param)) {
		param = (Param *)arg;
	} else if (IsA(arg, CoerceViaIO) || IsA(arg, ArrayCoerceExpr) || IsA(arg, RelabelType)) {
		/* Handle type coercions - extract the underlying Param */
		Node *inner = arg;
		while (inner) {
			if (IsA(inner, Param)) {
				param = (Param *)inner;
				break;
			} else if (IsA(inner, CoerceViaIO)) {
				inner = (Node *)((CoerceViaIO *)inner)->arg;
			} else if (IsA(inner, ArrayCoerceExpr)) {
				inner = (Node *)((ArrayCoerceExpr *)inner)->arg;
			} else if (IsA(inner, RelabelType)) {
				inner = (Node *)((RelabelType *)inner)->arg;
			} else {
				break;
			}
		}
	}

	if (!param || param->paramkind != PARAM_EXTERN) {
		return -1;
	}

	/* Verify the parameter is an array type */
	Oid param_type = param->paramtype;
	Oid elem_type = get_element_type(param_type);
	if (!OidIsValid(elem_type)) {
		/* Not an array type */
		return -1;
	}

	*array_type_out = param_type;
	*elem_type_out = elem_type;

	/* paramid is 1-based, return 0-based index */
	return param->paramid - 1;
}

/*
 * Get the data_inlining_row_limit for a DuckLake table.
 * Returns 0 if inlining is disabled or the table is not a DuckLake table.
 */
static uint64_t
GetDataInliningRowLimit(Oid /*relid*/, const char *schema_name, const char *table_name) {
	/* Query DuckDB to get the option value */
	auto connection = DuckDBManager::GetConnection();

	std::string query = duckdb::StringUtil::Format(
	    "SELECT value FROM %s.options('%s.%s') WHERE option_name = 'data_inlining_row_limit'", PGDUCKLAKE_DB_NAME,
	    schema_name, table_name);

	auto result = connection->context->Query(query, false);
	if (result->HasError()) {
		return 0;
	}

	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return 0;
	}

	try {
		return chunk->GetValue(0, 0).GetValue<uint64_t>();
	} catch (...) {
		return 0;
	}
}

std::optional<InlineBypassInfo>
DetectInlineBypassPattern(Query *query) {
	/* Must be an INSERT command */
	if (query->commandType != CMD_INSERT) {
		return std::nullopt;
	}

	/* Must have a result relation */
	if (query->resultRelation == 0) {
		return std::nullopt;
	}

	/* Get the target table RTE */
	RangeTblEntry *result_rte = rt_fetch(query->resultRelation, query->rtable);
	if (!result_rte || result_rte->rtekind != RTE_RELATION) {
		return std::nullopt;
	}

	Oid target_relid = result_rte->relid;

	/* Check if it's a DuckLake table */
	Relation rel = RelationIdGetRelation(target_relid);
	if (!rel) {
		return std::nullopt;
	}

	bool is_ducklake = IsDucklakeTable(rel);
	const char *table_name = pstrdup(RelationGetRelationName(rel));
	Oid namespace_oid = RelationGetNamespace(rel);
	RelationClose(rel);

	if (!is_ducklake) {
		return std::nullopt;
	}

	const char *schema_name = get_namespace_name(namespace_oid);
	if (!schema_name) {
		return std::nullopt;
	}

	/* Check if data inlining is enabled for this table */
	uint64_t row_limit = GetDataInliningRowLimit(target_relid, schema_name, table_name);
	if (row_limit == 0) {
		return std::nullopt;
	}

	/* Now analyze the target list to see if it's all UNNEST(Param) expressions */
	InlineBypassInfo info;
	info.target_table_oid = target_relid;
	info.data_inlining_row_limit = row_limit;
	info.schema_name = schema_name;
	info.table_name = table_name;

	ListCell *lc;
	foreach (lc, query->targetList) {
		TargetEntry *te = (TargetEntry *)lfirst(lc);

		/* Skip junk columns (e.g., ctid for UPDATE) */
		if (te->resjunk) {
			continue;
		}

		Oid array_type, elem_type;
		int param_idx = GetUnnestParamIndex((Node *)te->expr, &array_type, &elem_type);

		if (param_idx < 0) {
			/* Not an UNNEST(Param) expression - can't use bypass */
			return std::nullopt;
		}

		info.param_indices.push_back(param_idx);
		info.array_types.push_back(array_type);
		info.array_elem_types.push_back(elem_type);
	}

	/* Must have at least one column */
	if (info.param_indices.empty()) {
		return std::nullopt;
	}

	elog(DEBUG1, "[PGDuckLake] Detected inline bypass pattern for %s.%s with %zu columns", schema_name, table_name,
	     info.param_indices.size());

	return info;
}

/* Forward declarations for custom scan callbacks */
static Node *InlineBypass_CreateCustomScanState(CustomScan *cscan);
static void InlineBypass_BeginCustomScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *InlineBypass_ExecCustomScan(CustomScanState *node);
static void InlineBypass_EndCustomScan(CustomScanState *node);
static void InlineBypass_ReScanCustomScan(CustomScanState *node);
static void InlineBypass_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es);

static Node *
InlineBypass_CreateCustomScanState(CustomScan *cscan) {
	InlineBypassState *state = (InlineBypassState *)newNode(sizeof(InlineBypassState), T_CustomScanState);
	state->css.methods = &inline_bypass_exec_methods;
	state->executed = false;
	state->rows_inserted = 0;

	/* Deserialize the InlineBypassInfo from custom_private */
	/* For now we store it as a list of Consts */
	List *private_data = cscan->custom_private;
	if (list_length(private_data) >= 5) {
		state->info.target_table_oid = intVal(linitial(private_data));
		state->info.data_inlining_row_limit = intVal(lsecond(private_data));
		state->info.schema_name = strVal(lthird(private_data));
		state->info.table_name = strVal(lfourth(private_data));

		/* Parse param_indices and array_types from remaining elements */
		List *param_list = (List *)list_nth(private_data, 4);
		List *array_type_list = (List *)list_nth(private_data, 5);
		List *elem_type_list = (List *)list_nth(private_data, 6);

		ListCell *lc1, *lc2, *lc3;
		forthree(lc1, param_list, lc2, array_type_list, lc3, elem_type_list) {
			state->info.param_indices.push_back(intVal(lfirst(lc1)));
			state->info.array_types.push_back(intVal(lfirst(lc2)));
			state->info.array_elem_types.push_back(intVal(lfirst(lc3)));
		}
	}

	return (Node *)state;
}

/*
 * Get the inlined data table name for a DuckLake table.
 * Creates the table if it doesn't exist.
 */
static std::string
GetOrCreateInlinedDataTable(const InlineBypassInfo &info, int64_t /*snapshot_id*/) {
	auto connection = DuckDBManager::GetConnection();

	/* Query for existing inlined data table */
	std::string query = duckdb::StringUtil::Format(
	    R"(
		SELECT table_name
		FROM %s.ducklake_inlined_data_tables t
		JOIN %s.ducklake_table dt ON t.table_id = dt.table_id
		WHERE dt.table_name = '%s'
		  AND dt.table_id IN (
		    SELECT table_id FROM %s.ducklake_table
		    JOIN %s.ducklake_schema USING (schema_id)
		    WHERE ducklake_schema.schema_name = '%s'
		  )
		ORDER BY schema_version DESC
		LIMIT 1
	)",
	    PGDUCKLAKE_DB_NAME, PGDUCKLAKE_DB_NAME, info.table_name, PGDUCKLAKE_DB_NAME, PGDUCKLAKE_DB_NAME,
	    info.schema_name);

	auto result = connection->context->Query(query, false);
	if (result->HasError()) {
		return "";
	}

	auto chunk = result->Fetch();
	if (chunk && chunk->size() > 0) {
		return chunk->GetValue(0, 0).ToString();
	}

	/* No existing table - we need to create one via DuckLake's mechanism */
	/* For now, fall back to the normal path by returning empty */
	/* TODO: Implement table creation logic */
	return "";
}

/*
 * Execute the inline bypass insert.
 * Returns the number of rows inserted.
 */
static int64_t
ExecuteInlineBypass(InlineBypassState *state, ParamListInfo params) {
	InlineBypassInfo &info = state->info;

	if (!params || params->numParams == 0) {
		elog(WARNING, "[PGDuckLake] Inline bypass: no parameters provided");
		return 0;
	}

	/* Get array lengths and validate they're all the same */
	int num_cols = info.param_indices.size();
	int64_t num_rows = -1;

	std::vector<Datum> array_datums(num_cols);
	std::vector<bool> array_nulls(num_cols);

	for (int i = 0; i < num_cols; i++) {
		int param_idx = info.param_indices[i];
		if (param_idx >= params->numParams) {
			elog(ERROR, "[PGDuckLake] Inline bypass: parameter $%d not provided", param_idx + 1);
		}

		ParamExternData *param;
		ParamExternData tmp;
		if (params->paramFetch) {
			param = params->paramFetch(params, param_idx + 1, false, &tmp);
		} else {
			param = &params->params[param_idx];
		}

		if (param->isnull) {
			array_nulls[i] = true;
			continue;
		}

		array_datums[i] = param->value;
		array_nulls[i] = false;

		/* Get array length */
		ArrayType *arr = DatumGetArrayTypeP(param->value);
		int arr_len = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

		if (num_rows < 0) {
			num_rows = arr_len;
		} else if (arr_len != num_rows) {
			elog(ERROR, "[PGDuckLake] Inline bypass: array length mismatch");
		}
	}

	if (num_rows <= 0) {
		return 0;
	}

	/* Check if we exceed the row limit - if so, fall back to normal path */
	if ((uint64_t)num_rows > info.data_inlining_row_limit) {
		elog(DEBUG1, "[PGDuckLake] Inline bypass: row count exceeds limit, falling back");
		/* Return -1 to signal fallback needed */
		return -1;
	}

	/* Get the inlined data table name */
	/* For now, we get the current snapshot from DuckDB */
	auto connection = DuckDBManager::GetConnection();
	auto snapshot_result = connection->context->Query(
	    duckdb::StringUtil::Format("SELECT current_snapshot FROM %s.current_snapshot()", PGDUCKLAKE_DB_NAME), false);

	if (snapshot_result->HasError()) {
		elog(WARNING, "[PGDuckLake] Failed to get current snapshot");
		return -1;
	}

	auto snapshot_chunk = snapshot_result->Fetch();
	if (!snapshot_chunk || snapshot_chunk->size() == 0) {
		elog(WARNING, "[PGDuckLake] Failed to get current snapshot (no data)");
		return -1;
	}

	int64_t snapshot_id = snapshot_chunk->GetValue(0, 0).GetValue<int64_t>();

	std::string inlined_table = GetOrCreateInlinedDataTable(info, snapshot_id);
	if (inlined_table.empty()) {
		elog(DEBUG1, "[PGDuckLake] Inline bypass: no inlined data table available, falling back");
		return -1;
	}

	/* Get the next row_id */
	std::string row_id_query = duckdb::StringUtil::Format("SELECT COALESCE(MAX(row_id), 0) FROM ducklake.%s",
	                                                      duckdb::KeywordHelper::WriteQuoted(inlined_table).c_str());

	/* Execute SPI insert */
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);

	/* Get max row_id from existing data */
	int ret = SPI_execute(row_id_query.c_str(), true, 0);
	int64_t start_row_id = 1;
	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		bool isnull;
		Datum max_row_id = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
		if (!isnull) {
			start_row_id = DatumGetInt64(max_row_id) + 1;
		}
	}

	/* Build the INSERT query with UNNEST */
	StringInfoData query_buf;
	initStringInfo(&query_buf);

	appendStringInfo(&query_buf, "INSERT INTO ducklake.%s (row_id, begin_snapshot, end_snapshot",
	                 quote_identifier(inlined_table.c_str()));

	/* Add column names - we need to get them from the table */
	Relation target_rel = RelationIdGetRelation(info.target_table_oid);
	TupleDesc tupdesc = RelationGetDescr(target_rel);
	for (int i = 0; i < tupdesc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (attr->attisdropped) {
			continue;
		}
		appendStringInfo(&query_buf, ", %s", quote_identifier(NameStr(attr->attname)));
	}
	RelationClose(target_rel);

	appendStringInfo(&query_buf, ") SELECT row_number() OVER () + $1 - 1, $2, NULL");

	/* Add UNNEST for each column */
	for (int i = 0; i < num_cols; i++) {
		appendStringInfo(&query_buf, ", unnest($%d)", i + 3);
	}

	/* Build the argument arrays for SPI */
	int nargs = 2 + num_cols;
	Oid *argtypes = (Oid *)palloc(sizeof(Oid) * nargs);
	Datum *values = (Datum *)palloc(sizeof(Datum) * nargs);
	char *nulls = (char *)palloc(sizeof(char) * nargs);

	argtypes[0] = INT8OID;
	values[0] = Int64GetDatum(start_row_id);
	nulls[0] = ' ';

	argtypes[1] = INT8OID;
	values[1] = Int64GetDatum(snapshot_id);
	nulls[1] = ' ';

	for (int i = 0; i < num_cols; i++) {
		argtypes[i + 2] = info.array_types[i];
		values[i + 2] = array_datums[i];
		nulls[i + 2] = array_nulls[i] ? 'n' : ' ';
	}

	/* Execute the insert */
	ret = SPI_execute_with_args(query_buf.data, nargs, argtypes, values, nulls, false, 0);

	int64_t rows_affected = 0;
	if (ret == SPI_OK_INSERT) {
		rows_affected = SPI_processed;
	} else {
		elog(WARNING, "[PGDuckLake] Inline bypass insert failed: %s", SPI_result_code_string(ret));
	}

	AtEOXact_GUC(false, save_nestlevel);
	PopActiveSnapshot();
	SPI_finish();

	pfree(query_buf.data);
	pfree(argtypes);
	pfree(values);
	pfree(nulls);

	elog(DEBUG1, "[PGDuckLake] Inline bypass: inserted rows into %s", inlined_table.c_str());

	return rows_affected;
}

static void
InlineBypass_BeginCustomScan(CustomScanState *node, EState * /*estate*/, int /*eflags*/) {
	InlineBypassState *state = (InlineBypassState *)node;
	state->executed = false;
	state->rows_inserted = 0;
}

static TupleTableSlot *
InlineBypass_ExecCustomScan_Cpp(CustomScanState *node) {
	InlineBypassState *state = (InlineBypassState *)node;

	if (state->executed) {
		return nullptr;
	}

	state->executed = true;

	/* Get parameters from the executor state */
	ParamListInfo params = node->ss.ps.state->es_param_list_info;

	int64_t result = ExecuteInlineBypass(state, params);

	if (result < 0) {
		/* Fallback needed - this shouldn't happen in normal flow */
		elog(ERROR, "[PGDuckLake] Inline bypass fallback not implemented in executor");
	}

	state->rows_inserted = result;
	node->ss.ps.state->es_processed = result;

	return nullptr;
}

static TupleTableSlot *
InlineBypass_ExecCustomScan(CustomScanState *node) {
	return InvokeCPPFunc(InlineBypass_ExecCustomScan_Cpp, node);
}

static void
InlineBypass_EndCustomScan(CustomScanState * /*node*/) {
	/* Nothing to clean up */
}

static void
InlineBypass_ReScanCustomScan(CustomScanState *node) {
	InlineBypassState *state = (InlineBypassState *)node;
	state->executed = false;
}

static void
InlineBypass_ExplainCustomScan(CustomScanState *node, List * /*ancestors*/, ExplainState *es) {
	InlineBypassState *state = (InlineBypassState *)node;
	ExplainPropertyText("DuckLake Inline Bypass", "true", es);
	ExplainPropertyText("Target Table",
	                    psprintf("%s.%s", state->info.schema_name, state->info.table_name), es);
	ExplainPropertyInteger("Columns", NULL, state->info.param_indices.size(), es);
	ExplainPropertyInteger("Row Limit", NULL, state->info.data_inlining_row_limit, es);
}

PlannedStmt *
CreateInlineBypassPlan(const InlineBypassInfo &info, Query *query) {
	CustomScan *cscan = makeNode(CustomScan);

	/* Store the bypass info in custom_private */
	List *param_list = NIL;
	List *array_type_list = NIL;
	List *elem_type_list = NIL;

	for (size_t i = 0; i < info.param_indices.size(); i++) {
		param_list = lappend(param_list, makeInteger(info.param_indices[i]));
		array_type_list = lappend(array_type_list, makeInteger(info.array_types[i]));
		elem_type_list = lappend(elem_type_list, makeInteger(info.array_elem_types[i]));
	}

	cscan->custom_private = list_make5(makeInteger(info.target_table_oid),
	                                   makeInteger(info.data_inlining_row_limit), makeString(pstrdup(info.schema_name)),
	                                   makeString(pstrdup(info.table_name)), param_list);
	cscan->custom_private = lappend(cscan->custom_private, array_type_list);
	cscan->custom_private = lappend(cscan->custom_private, elem_type_list);

	cscan->methods = &inline_bypass_scan_methods;
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = NIL;

	/* Create the PlannedStmt */
	PlannedStmt *result = makeNode(PlannedStmt);
	result->commandType = query->commandType;
	result->queryId = query->queryId;
	result->hasReturning = false;
	result->hasModifyingCTE = false;
	result->canSetTag = query->canSetTag;
	result->transientPlan = false;
	result->planTree = (Plan *)cscan;
	result->rtable = query->rtable;

	/* Set up the result relation */
	result->resultRelations = list_make1_int(query->resultRelation);

	return result;
}

void
InitInlineBypassNode(void) {
	/* Setup scan methods */
	memset(&inline_bypass_scan_methods, 0, sizeof(inline_bypass_scan_methods));
	inline_bypass_scan_methods.CustomName = "DuckLakeInlineBypass";
	inline_bypass_scan_methods.CreateCustomScanState = InlineBypass_CreateCustomScanState;
	RegisterCustomScanMethods(&inline_bypass_scan_methods);

	/* Setup exec methods */
	memset(&inline_bypass_exec_methods, 0, sizeof(inline_bypass_exec_methods));
	inline_bypass_exec_methods.CustomName = "DuckLakeInlineBypass";
	inline_bypass_exec_methods.BeginCustomScan = InlineBypass_BeginCustomScan;
	inline_bypass_exec_methods.ExecCustomScan = InlineBypass_ExecCustomScan;
	inline_bypass_exec_methods.EndCustomScan = InlineBypass_EndCustomScan;
	inline_bypass_exec_methods.ReScanCustomScan = InlineBypass_ReScanCustomScan;
	inline_bypass_exec_methods.ExplainCustomScan = InlineBypass_ExplainCustomScan;
}

} // namespace pgduckdb
