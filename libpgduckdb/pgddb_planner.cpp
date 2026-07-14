#include "pgddb/pgddb_planner.hpp"

#include "duckdb.hpp"

#include "pgddb/catalog/pgddb_transaction.hpp"
#include "pgddb/scan/postgres_scan.hpp"
#include "pgddb/pgddb_types.hpp"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "optimizer/planmain.h"
#include "tcop/pquery.h"
#include "utils/syscache.h"
#include "utils/guc.h"
#include "parser/parse_relation.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pgddb/pgddb_ruleutils.h"

#if PG_VERSION_NUM >= 180000
#include "executor/executor.h"
#endif
}

#include "pgddb/pgddb_duckdb.hpp"
#include "pgddb/pgddb_node.hpp"
#include "pgddb/pgddb_table_am.hpp"
#include "pgddb/vendor/pg_list.hpp"
#include "pgddb/utility/cpp_wrapper.hpp"
#include "pgddb/pgddb_types.hpp"

namespace pgddb {

/*
 * True if any RangeTblEntry references a PG heap table (AM not registered with
 * libpgddb). The CustomScan uses this to materialize fully instead of streaming,
 * since concurrent PG access during stream-fetch could race. See
 * https://github.com/duckdb/pg_duckdb/discussions/866.
 */
bool
ContainsPostgresTable(Node *node, void *context) {
	if (node == NULL)
		return false;

	if (IsA(node, Query)) {
		Query *query = (Query *)node;
		foreach_node(RangeTblEntry, rte, query->rtable) {
			if (rte->relid == InvalidOid) {
				continue;
			}
			char relkind = get_rel_relkind(rte->relid);
			if (relkind == RELKIND_VIEW) {
				/* Tables referenced in the view are also in the rtable */
				continue;
			}
			if (pgddb::TableAmGetName(rte->relid) == nullptr) {
				return true;
			}
		}

#if PG_VERSION_NUM >= 160000
		return query_tree_walker(query, ContainsPostgresTable, context, 0);
#else
		return query_tree_walker(query, (bool (*)())((void *)ContainsPostgresTable), context, 0);
#endif
	}

#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, ContainsPostgresTable, context);
#else
	return expression_tree_walker(node, (bool (*)())((void *)ContainsPostgresTable), context);
#endif
}

duckdb::unique_ptr<duckdb::PreparedStatement>
Prepare(const Query *query, const char *explain_prefix) {
	Query *copied_query = (Query *)copyObjectImpl(query);
	const char *query_string = pgddb_get_querydef(copied_query);

	if (explain_prefix) {
		query_string = psprintf("%s %s", explain_prefix, query_string);
	}

	elog(DEBUG2, "(PGDuckDB/DuckdbPrepare) Preparing: %s", query_string);

	auto con = pgddb::GetConnection();
	return con->context->Prepare(query_string);
}

std::string
DeparseQuery(const Query *query) {
	Query *copied_query = (Query *)copyObjectImpl(query);
	return std::string(pgddb_get_querydef(copied_query));
}

static Plan *
CreatePlan(Query *query, bool throw_error) {
	int elevel = throw_error ? ERROR : WARNING;
	/* Prepare so we can get the returned types and column names. */
	duckdb::unique_ptr<duckdb::PreparedStatement> prepared_query = Prepare(query);

	if (prepared_query->HasError()) {
		elog(elevel, "(PGDuckDB/CreatePlan) Prepared query returned an error: %s", prepared_query->GetError().c_str());
		return nullptr;
	}

	CustomScan *duckdb_node = makeNode(CustomScan);

	auto &prepared_result_types = prepared_query->GetTypes();

	for (size_t i = 0; i < prepared_result_types.size(); i++) {
		Oid postgresColumnOid = pgddb::GetPostgresDuckDBType(prepared_result_types[i], throw_error);

		if (!OidIsValid(postgresColumnOid)) {
			return nullptr;
		}

		HeapTuple tp;
		Form_pg_type typtup;

		tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(postgresColumnOid));
		if (!HeapTupleIsValid(tp)) {
			elog(elevel, "(PGDuckDB/CreatePlan) Cache lookup failed for type %u", postgresColumnOid);
			return nullptr;
		}

		typtup = (Form_pg_type)GETSTRUCT(tp);
		typtup->typtypmod = pgddb::GetPostgresDuckDBTypemod(prepared_result_types[i]);

		/* varno 1: the final plan has a single RTE (this custom scan). */
		Var *var = makeVar(1, i + 1, postgresColumnOid, typtup->typtypmod, typtup->typcollation, 0);

		TargetEntry *target_entry =
		    makeTargetEntry((Expr *)var, i + 1, (char *)pstrdup(prepared_query->GetNames()[i].c_str()), false);

		duckdb_node->custom_scan_tlist = lappend(duckdb_node->custom_scan_tlist, copyObjectImpl(target_entry));

		/* INDEX_VAR varno makes the plan targetlist reference custom_scan_tlist. */
		var->varno = INDEX_VAR;

		/* Plan needs an actual targetlist too, e.g. for materialization. */
		duckdb_node->scan.plan.targetlist = lappend(duckdb_node->scan.plan.targetlist, target_entry);

		ReleaseSysCache(tp);
	}

	duckdb_node->custom_private = list_make1(query);
	duckdb_node->methods = &scan_methods;

	return (Plan *)duckdb_node;
}

static RangeTblEntry *
DuckdbRangeTableEntry(CustomScan *custom_scan) {
	List *column_names = NIL;
	foreach_node(TargetEntry, target_entry, custom_scan->scan.plan.targetlist) {
		column_names = lappend(column_names, makeString(target_entry->resname));
	}
	RangeTblEntry *rte = makeNode(RangeTblEntry);

	/* RTE_RELATION asserts on unset fields; RTE_NAMEDTUPLESTORE needs none. */
	rte->rtekind = RTE_NAMEDTUPLESTORE;
	rte->eref = makeAlias("duckdb_scan", column_names);
	rte->inFromCl = true;

	return rte;
}

static void
check_view_perms_recursive(Query *query) {
	ListCell *lc;

	if (query == NULL) {
		return;
	}

	foreach (lc, query->rtable) {
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

#if PG_VERSION_NUM < 160000
		if (rte->relkind == RELKIND_VIEW) {
			bool result = ExecCheckRTEPerms(rte);
			if (!result) {
				aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_VIEW, get_rel_name(rte->relid));
			}
		}
#else
		if (rte->perminfoindex != 0 && rte->relkind == RELKIND_VIEW) {
			RTEPermissionInfo *perminfo = getRTEPermissionInfo(query->rteperminfos, rte);
			bool result = ExecCheckOneRelPerms(perminfo);
			if (!result) {
				aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_VIEW, get_rel_name(perminfo->relid));
			}
		}
#endif

		if (rte->rtekind == RTE_SUBQUERY && rte->subquery) {
			check_view_perms_recursive(rte->subquery);
		}
	}

	if (query->cteList) {
		ListCell *lc_cte;
		foreach (lc_cte, query->cteList) {
			CommonTableExpr *cte = (CommonTableExpr *)lfirst(lc_cte);
			if (IsA(cte->ctequery, Query)) {
				check_view_perms_recursive((Query *)cte->ctequery);
			}
		}
	}
}

PlannedStmt *
PlanNode(Query *parse, int cursor_options, bool throw_error) {

	/* Check perms if there's a view or WITH statement */
	check_view_perms_recursive(parse);

	Plan *duckdb_plan = InvokeCPPFunc(CreatePlan, parse, throw_error);
	CustomScan *custom_scan = castNode(CustomScan, duckdb_plan);

	if (!duckdb_plan) {
		return nullptr;
	}

	/* Scrollable cursors need a top Material node; CustomScan can't scan backwards. */
	if (cursor_options & CURSOR_OPT_SCROLL) {
		duckdb_plan = materialize_finished_plan(duckdb_plan);
	}

	RangeTblEntry *rte = DuckdbRangeTableEntry(custom_scan);

	PlannedStmt *result = makeNode(PlannedStmt);
	result->commandType = parse->commandType;
	result->queryId = parse->queryId;
	result->hasReturning = (parse->returningList != NIL);
	result->hasModifyingCTE = parse->hasModifyingCTE;
	result->canSetTag = parse->canSetTag;
	result->transientPlan = false;
	result->dependsOnRole = false;
	result->parallelModeNeeded = false;
	result->planTree = duckdb_plan;
	result->rtable = list_make1(rte);
#if PG_VERSION_NUM >= 160000
	result->permInfos = NULL;
#endif
#if PG_VERSION_NUM >= 190000
	result->resultRelationRelids = NULL;
#else
	result->resultRelations = NULL;
#endif
	result->appendRelations = NULL;
	result->subplans = NIL;
	result->rewindPlanIDs = NULL;
	result->rowMarks = NIL;
	result->relationOids = NIL;
	result->invalItems = NIL;
	result->paramExecTypes = NIL;

	/* utilityStmt should be null, but copy it anyway */
	result->utilityStmt = parse->utilityStmt;
	result->stmt_location = parse->stmt_location;
	result->stmt_len = parse->stmt_len;

	return result;
}

} // namespace pgddb
