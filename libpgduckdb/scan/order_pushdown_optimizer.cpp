#include "pgddb/scan/order_pushdown_optimizer.hpp"

#include "pgddb/logger.hpp"
#include "pgddb/scan/postgres_scan.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

extern "C" {
#include "postgres.h"

#include "access/genam.h"
#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_index.h"
#include "nodes/pg_list.h"
#include "storage/lockdefs.h"
#include "utils/rel.h"
}

namespace pgddb {

using duckdb::BoundColumnRefExpression;
using duckdb::BoundOrderByNode;
using duckdb::BoundReferenceExpression;
using duckdb::Expression;
using duckdb::ExpressionClass;
using duckdb::ExpressionType;
using duckdb::LogicalGet;
using duckdb::LogicalOperator;
using duckdb::LogicalOperatorType;
using duckdb::LogicalOrder;
using duckdb::LogicalProjection;
using duckdb::LogicalTopN;
using duckdb::OptimizerExtension;
using duckdb::OptimizerExtensionInput;
using duckdb::optional_idx;
using duckdb::unique_ptr;

static bool
IndexSupportsOrder(Relation rel, const duckdb::vector<AttrNumber> &order_attrs,
                   const duckdb::vector<PostgresOrderBySpec> &orders) {
	if (order_attrs.empty()) {
		return false;
	}
	List *index_list = RelationGetIndexList(rel);
	if (index_list == NIL) {
		return false;
	}
	bool supported = false;
	ListCell *lc;
	foreach (lc, index_list) {
		Oid index_oid = lfirst_oid(lc);
		Relation index_rel = index_open(index_oid, AccessShareLock);
		// Cheap structural rejects first: a valid btree covering all order keys, not partial.
		// (A partial index can never satisfy a whole-table ORDER BY.)
		if (!index_rel->rd_index || !index_rel->rd_index->indisvalid || index_rel->rd_rel->relam != BTREE_AM_OID ||
		    static_cast<duckdb::idx_t>(index_rel->rd_index->indnkeyatts) < order_attrs.size() ||
		    static_cast<duckdb::idx_t>(index_rel->rd_index->indkey.dim1) < order_attrs.size() ||
		    RelationGetIndexPredicate(index_rel) != NIL) {
			index_close(index_rel, AccessShareLock);
			continue;
		}
		bool attr_match = true;
		bool matches_forward = true;
		bool matches_backward = true; // btree always supports backward scans
		for (duckdb::idx_t i = 0; i < order_attrs.size(); i++) {
			AttrNumber index_attnum = index_rel->rd_index->indkey.values[i];
			if (index_attnum <= 0 || index_attnum != order_attrs[i]) {
				attr_match = false;
				break;
			}
			int16 option = index_rel->rd_indoption ? index_rel->rd_indoption[i] : 0;
			bool index_desc = (option & INDOPTION_DESC) != 0;
			bool index_nulls_first;
			if (option & INDOPTION_NULLS_FIRST) {
				index_nulls_first = true;
			} else {
				index_nulls_first = index_desc; // DESC defaults to NULLS FIRST, ASC defaults to NULLS LAST
			}
			const auto &spec = orders[i];
			auto order_type =
			    spec.order_type == duckdb::OrderType::ORDER_DEFAULT ? duckdb::OrderType::ASCENDING : spec.order_type;
			bool spec_desc = order_type == duckdb::OrderType::DESCENDING;
			auto null_order =
			    spec.null_order == duckdb::OrderByNullType::ORDER_DEFAULT
			        ? (spec_desc ? duckdb::OrderByNullType::NULLS_FIRST : duckdb::OrderByNullType::NULLS_LAST)
			        : spec.null_order;
			bool spec_nulls_first = null_order == duckdb::OrderByNullType::NULLS_FIRST;
			if (matches_forward && (spec_desc != index_desc || spec_nulls_first != index_nulls_first)) {
				matches_forward = false;
			}
			if (matches_backward) {
				bool backward_desc = !index_desc;
				bool backward_nulls_first = !index_nulls_first;
				if (spec_desc != backward_desc || spec_nulls_first != backward_nulls_first) {
					matches_backward = false;
				}
			}
			if (!matches_forward && !matches_backward) {
				attr_match = false;
				break;
			}
		}
		if (attr_match && (matches_forward || matches_backward)) {
			supported = true;
		}
		index_close(index_rel, AccessShareLock);
		if (supported) {
			break;
		}
	}
	list_free(index_list);
	return supported;
}

static bool
ExtractColumnIndex(Expression &expr, LogicalGet &get, duckdb::idx_t &column_index) {
	const auto &column_ids = get.GetColumnIds();
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF: {
		auto &ref = expr.Cast<BoundReferenceExpression>();
		auto ref_index = ref.index;
		duckdb::idx_t mapped_index;
		if (get.projection_ids.empty()) {
			mapped_index = ref_index;
		} else {
			if (ref_index >= get.projection_ids.size()) {
				return false;
			}
			mapped_index = get.projection_ids[ref_index];
		}
		if (mapped_index >= column_ids.size()) {
			return false;
		}
		auto &col_idx = column_ids[mapped_index];
		if (col_idx.HasChildren() || col_idx.IsVirtualColumn()) {
			return false;
		}
		column_index = mapped_index;
		return true;
	}
	default:
		break;
	}

	switch (expr.GetExpressionType()) {
	case ExpressionType::BOUND_COLUMN_REF: {
		auto &col = expr.Cast<BoundColumnRefExpression>();
		if (col.binding.table_index != get.table_index) {
			return false;
		}
		auto binding_column_index = col.binding.column_index;
		for (duckdb::idx_t i = 0; i < column_ids.size(); i++) {
			auto &col_idx = column_ids[i];
			if (col_idx.HasChildren() || col_idx.IsVirtualColumn()) {
				continue;
			}
			if (col_idx.GetPrimaryIndex() == binding_column_index) {
				column_index = i;
				return true;
			}
		}
		return false;
	}
	default:
		return false;
	}
}

static LogicalGet *
FindUnderlyingGet(LogicalOperator &input) {
	auto *current = &input;
	while (current->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = current->Cast<LogicalProjection>();
		if (projection.children.size() != 1) {
			return nullptr;
		}
		current = projection.children[0].get();
	}
	if (current->type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	return &current->Cast<LogicalGet>();
}

static bool
ResolveColumnIndex(Expression &expr, LogicalOperator &input, LogicalGet &get, duckdb::idx_t &column_index) {
	if (input.type == LogicalOperatorType::LOGICAL_GET) {
		return ExtractColumnIndex(expr, get, column_index);
	}
	if (input.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = input.Cast<LogicalProjection>();
		if (projection.children.size() != 1) {
			return false;
		}
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto &ref = expr.Cast<BoundReferenceExpression>();
			if (ref.index >= projection.expressions.size()) {
				return false;
			}
			return ResolveColumnIndex(*projection.expressions[ref.index], *projection.children[0], get, column_index);
		}
		if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			auto &col = expr.Cast<BoundColumnRefExpression>();
			if (col.binding.table_index == projection.table_index) {
				auto binding_column_index = col.binding.column_index;
				if (binding_column_index >= projection.expressions.size()) {
					return false;
				}
				return ResolveColumnIndex(*projection.expressions[binding_column_index], *projection.children[0], get,
				                          column_index);
			}
		}
		return ResolveColumnIndex(expr, *projection.children[0], get, column_index);
	}
	return ExtractColumnIndex(expr, get, column_index);
}

static bool
TryPushdownOrder(unique_ptr<LogicalOperator> &op) {
	// Pushdown is unconditional but narrow: it only fires when a btree index can
	// produce the requested ordering (IndexSupportsOrder below), for both the
	// ORDER BY and the Top-N (ORDER BY + LIMIT) cases.
	//
	// Both LOGICAL_ORDER_BY and LOGICAL_TOP_N (ORDER BY + LIMIT) carry an `orders`
	// list above a single child; Top-N additionally carries limit/offset constants.
	duckdb::vector<BoundOrderByNode> *orders;
	unique_ptr<LogicalOperator> *child_slot;
	optional_idx limit;
	duckdb::idx_t offset = 0;
	if (op->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
		auto &order = op->Cast<LogicalOrder>();
		if (order.children.size() != 1) {
			return false;
		}
		orders = &order.orders;
		child_slot = &order.children[0];
	} else if (op->type == LogicalOperatorType::LOGICAL_TOP_N) {
		auto &topn = op->Cast<LogicalTopN>();
		if (topn.children.size() != 1) {
			return false;
		}
		orders = &topn.orders;
		child_slot = &topn.children[0];
		limit = topn.limit;
		offset = topn.offset;
	} else {
		return false;
	}
	auto &child = *child_slot;
	auto *get_ptr = FindUnderlyingGet(*child);
	if (!get_ptr) {
		return false;
	}
	auto &get = *get_ptr;
	if (get.function.name != "pgduckdb_postgres_scan") {
		return false;
	}
	auto *bind_data = dynamic_cast<PostgresScanFunctionData *>(get.bind_data.get());
	if (!bind_data) {
		return false;
	}
	bind_data->order_bys.clear();
	bind_data->limit = optional_idx();
	bind_data->offset = 0;
	const auto &column_ids = get.GetColumnIds();
	duckdb::vector<PostgresOrderBySpec> new_orders;
	duckdb::vector<AttrNumber> order_attrs;
	new_orders.reserve(orders->size());
	order_attrs.reserve(orders->size());
	for (auto &order_node : *orders) {
		duckdb::idx_t column_index;
		if (!ResolveColumnIndex(*order_node.expression, *child, get, column_index)) {
			pd_log(DEBUG2, "(PGDuckDB/OrderPushdown) Unable to map ORDER BY expression to Postgres column");
			return false;
		}
		if (column_index >= column_ids.size()) {
			return false;
		}
		auto &column_id = column_ids[column_index];
		if (column_id.HasChildren() || column_id.IsVirtualColumn() || column_id.IsRowIdColumn()) {
			return false;
		}
		PostgresOrderBySpec spec;
		spec.column_index = column_index;
		spec.order_type = order_node.type;
		spec.null_order = order_node.null_order;
		spec.column_name = get.GetColumnName(column_id);
		new_orders.push_back(spec);
		order_attrs.push_back(static_cast<AttrNumber>(column_id.GetPrimaryIndex() + 1));
	}
	if (!IndexSupportsOrder(bind_data->rel, order_attrs, new_orders)) {
		pd_log(DEBUG2, "(PGDuckDB/OrderPushdown) Skipping pushdown: no matching index found for ORDER BY");
		return false;
	}
	pd_log(DEBUG2, "(PGDuckDB/OrderPushdown) Pushing %zu ORDER BY key(s)%s to Postgres scan", new_orders.size(),
	       limit.IsValid() ? " + LIMIT" : "");
	bind_data->order_bys = std::move(new_orders);
	bind_data->limit = limit;
	bind_data->offset = offset;
	op = std::move(*child_slot);
	return true;
}

static bool
PushdownRecursive(unique_ptr<LogicalOperator> &op) {
	bool changed = false;
	for (auto &child : op->children) {
		changed |= PushdownRecursive(child);
	}
	// Post-order: children are rewritten first, so a stacked ORDER BY directly over
	// a now-pushed inner order still sees the scan. One pass per node is enough --
	// pushdown only ever rewrites downward, so re-visiting after this cannot help.
	changed |= TryPushdownOrder(op);
	return changed;
}

static void
OptimizePlan(OptimizerExtensionInput &, unique_ptr<LogicalOperator> &plan) {
	PushdownRecursive(plan);
}

PostgresOrderPushdownOptimizer::PostgresOrderPushdownOptimizer() {
	optimize_function = OptimizePlan;
}

} // namespace pgddb
