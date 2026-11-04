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
#include "parser/parse_coerce.h"
#include "rewrite/rewriteManip.h"
#include "tcop/pquery.h"
#include "utils/syscache.h"
#include "utils/guc.h"
#include "parser/parse_relation.h"
#include "utils/acl.h"
#include "utils/builtins.h"
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

static bool
ContainsValuesRTE(Query *query) {
	foreach_node(RangeTblEntry, rte, query->rtable) {
		if (rte->rtekind == RTE_VALUES) {
			return true;
		}
		if (rte->rtekind == RTE_SUBQUERY && ContainsValuesRTE(rte->subquery)) {
			return true;
		}
	}
	return false;
}

/* The parser wraps the source of INSERT ... SELECT in a single subquery RTE; returns its index or 0. */
static int
FindInsertSelectRTI(const Query *query) {
	int rti = 1;
	foreach_node(RangeTblEntry, rte, query->rtable) {
		if (rte->rtekind == RTE_SUBQUERY) {
			return rti;
		}
		rti++;
	}
	return 0;
}

/* A regular or partitioned table not managed by the consumer's table AM. */
bool
IsPostgresTable(Oid relid) {
	char relkind = get_rel_relkind(relid);
	return (relkind == RELKIND_RELATION || relkind == RELKIND_PARTITIONED_TABLE) &&
	       pgddb::TableAmGetName(relid) == nullptr;
}

/* An INSERT into a Postgres table: only its SELECT source runs in DuckDB. */
bool
IsPostgresInsert(const Query *query) {
	if (query->commandType != CMD_INSERT || query->resultRelation == 0) {
		return false;
	}
	return IsPostgresTable(list_nth_node(RangeTblEntry, query->rtable, query->resultRelation - 1)->relid);
}

bool
IsAllowedPostgresInsert(Query *query, bool throw_error) {
	int elevel = throw_error ? ERROR : DEBUG4;
	if (query->commandType != CMD_INSERT) {
		elog(elevel, "DuckDB only supports INSERT/SELECT on Postgres tables");
		return false;
	}

	if (!IsPostgresInsert(query)) {
		elog(elevel, "DuckDB does not support modifying this kind of Postgres relation");
		return false;
	}

	/*
	 * EXPLAIN ANALYZE would run the SELECT inside a DuckDB EXPLAIN ANALYZE and
	 * silently insert nothing. explain_analyze is only valid while planning an
	 * EXPLAIN, hence the ActivePortal check.
	 */
	if (explain_analyze && ActivePortal && ActivePortal->commandTag == CMDTAG_EXPLAIN) {
		elog(elevel, "DuckDB does not support EXPLAIN ANALYZE on INSERTs into Postgres tables");
		return false;
	}

	int select_rti = FindInsertSelectRTI(query);
	if (select_rti == 0) {
		elog(elevel, "DuckDB does not support INSERT without a subquery");
		return false;
	}

	/*
	 * Literal input may differ between Postgres and DuckDB (bytea, numeric
	 * rounding), so prefer Postgres for VALUES unless DuckDB is required.
	 */
	RangeTblEntry *select_rte = list_nth_node(RangeTblEntry, query->rtable, select_rti - 1);
	if (!throw_error && ContainsValuesRTE(select_rte->subquery)) {
		elog(elevel, "DuckDB does not support INSERTs with value subqueries");
		return false;
	}

	RangeTblEntry *target_rte = list_nth_node(RangeTblEntry, query->rtable, query->resultRelation - 1);
	Relation rel = RelationIdGetRelation(target_rte->relid);
	TupleDesc target_desc = RelationGetDescr(rel);
	for (int i = 0; i < target_desc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(target_desc, i);
		if (attr->attisdropped) {
			continue;
		}
		if (pgddb::ConvertPostgresToDuckColumnType(attr).id() == duckdb::LogicalTypeId::INVALID) {
			elog(elevel, "DuckDB does not support INSERTs into tables with column `%s` of unsupported type (OID %u)",
			     NameStr(attr->attname), attr->atttypid);
			RelationClose(rel);
			return false;
		}
	}
	RelationClose(rel);

	return true;
}

std::string
DeparseQuery(const Query *query) {
	Query *copied_query = (Query *)copyObjectImpl(query);
	if (IsPostgresInsert(copied_query)) {
		int select_rti = FindInsertSelectRTI(copied_query);
		Assert(select_rti != 0);
		Query *select_query = list_nth_node(RangeTblEntry, copied_query->rtable, select_rti - 1)->subquery;

		/*
		 * Move the INSERT's CTEs into the SELECT. They move down one query
		 * level, so fix up levelsup first (as pull_up_simple_subquery does).
		 * CTE references resolve by name; DuckDB rejects duplicate names.
		 */
		if (copied_query->cteList != NIL) {
			IncrementVarSublevelsUp((Node *)select_query, -1, 1);
			select_query->cteList = list_concat(copied_query->cteList, select_query->cteList);
			select_query->hasRecursive = select_query->hasRecursive || copied_query->hasRecursive;
		}
		copied_query = select_query;
	}
	return pgddb_get_querydef(copied_query);
}

duckdb::unique_ptr<duckdb::PreparedStatement>
Prepare(const Query *query, const char *explain_prefix) {
	std::string query_string = DeparseQuery(query);

	if (explain_prefix) {
		query_string = std::string(explain_prefix) + " " + query_string;
	}

	elog(DEBUG2, "(PGDuckDB/DuckdbPrepare) Preparing: %s", query_string.c_str());

	auto con = pgddb::GetConnection();
	return con->context->Prepare(query_string);
}

/*
 * Builds the ModifyTable subplan targetlist: one entry per table column in
 * table order, mapping each to the DuckDB output column its INSERT targetlist
 * Var references, or keeping the rewriter's default/constant expression.
 */
static List *
InsertTargetList(TupleDesc pg_tupdesc, Query *query, List *duckdb_targetlist) {
	List *target_list = NIL;
	int select_rti = FindInsertSelectRTI(query);

	for (int i = 0; i < pg_tupdesc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(pg_tupdesc, i);

		/* ExecCheckPlanOutput expects a NULL for dropped columns. */
		if (attr->attisdropped) {
			Expr *null_expr = (Expr *)makeNullConst(INT4OID, -1, InvalidOid);
			target_list = lappend(target_list, makeTargetEntry(null_expr, attr->attnum, NULL, false));
			continue;
		}

		TargetEntry *target_entry = NULL;
		foreach_node(TargetEntry, query_target_entry, query->targetList) {
			if (query_target_entry->resno != attr->attnum) {
				continue;
			}

			AttrNumber select_attno = 0;
			foreach_node(Var, var, pull_var_clause((Node *)query_target_entry->expr, 0)) {
				if (var->varno == select_rti) {
					select_attno = var->varattno;
					break;
				}
			}

			/* A default or inlined constant, already coerced to the column type. */
			if (select_attno == 0) {
				target_entry = query_target_entry;
				break;
			}

			if (select_attno > list_length(duckdb_targetlist)) {
				elog(ERROR, "SELECT column assigned to column \"%s\" is missing from the DuckDB result",
				     NameStr(attr->attname));
			}

			target_entry = list_nth_node(TargetEntry, duckdb_targetlist, select_attno - 1);
			target_entry->resno = attr->attnum;

			/* Assignment coercion, so e.g. too-long varchar values error like Postgres INSERT. */
			Oid source_type = exprType((Node *)target_entry->expr);
			int32 source_typmod = exprTypmod((Node *)target_entry->expr);
			if (source_type != attr->atttypid || source_typmod != attr->atttypmod) {
				Expr *coerced_expr =
				    (Expr *)coerce_to_target_type(NULL, (Node *)target_entry->expr, source_type, attr->atttypid,
				                                  attr->atttypmod, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
				if (coerced_expr == NULL) {
					elog(ERROR, "cannot coerce column \"%s\" from type %s to type %s", NameStr(attr->attname),
					     format_type_be(source_type), format_type_be(attr->atttypid));
				}
				target_entry->expr = coerced_expr;
			}
			break;
		}

		/* The rewriter already added defaults, so anything left is NULL. */
		if (!target_entry) {
			target_entry = makeTargetEntry((Expr *)makeNullConst(attr->atttypid, attr->atttypmod, attr->attcollation),
			                               attr->attnum, pstrdup(NameStr(attr->attname)), false);
		}
		target_list = lappend(target_list, target_entry);
	}

	return target_list;
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

		/* varno is filled in by SetCustomScanVarno once the final rtable is known. */
		Var *var = makeVar(0, i + 1, postgresColumnOid, typtup->typtypmod, typtup->typcollation, 0);

		TargetEntry *target_entry =
		    makeTargetEntry((Expr *)var, i + 1, (char *)pstrdup(prepared_query->GetNames()[i].c_str()), false);

		duckdb_node->custom_scan_tlist = lappend(duckdb_node->custom_scan_tlist, copyObjectImpl(target_entry));

		/* INDEX_VAR varno makes the plan targetlist reference custom_scan_tlist. */
		var->varno = INDEX_VAR;

		/* Plan needs an actual targetlist too, e.g. for materialization. */
		duckdb_node->scan.plan.targetlist = lappend(duckdb_node->scan.plan.targetlist, target_entry);

		ReleaseSysCache(tp);
	}

	if (IsPostgresInsert(query)) {
		RangeTblEntry *target_rte = list_nth_node(RangeTblEntry, query->rtable, query->resultRelation - 1);
		Relation rel = RelationIdGetRelation(target_rte->relid);
		duckdb_node->scan.plan.targetlist =
		    InsertTargetList(RelationGetDescr(rel), query, duckdb_node->scan.plan.targetlist);
		RelationClose(rel);
	}

	duckdb_node->custom_private = list_make1(query);
	duckdb_node->methods = &scan_methods;

	return (Plan *)duckdb_node;
}

static void
SetCustomScanVarno(CustomScan *custom_scan, int varno) {
	foreach_node(TargetEntry, target_entry, custom_scan->custom_scan_tlist) {
		castNode(Var, target_entry->expr)->varno = varno;
	}
}

static RangeTblEntry *
DuckdbRangeTableEntry(CustomScan *custom_scan) {
	List *column_names = NIL;
	foreach_node(TargetEntry, target_entry, custom_scan->custom_scan_tlist) {
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

	/* Let Postgres plan the INSERT, then swap in our scan as the ModifyTable source. */
	if (IsPostgresInsert(parse)) {
		Query *copied_query = (Query *)copyObjectImpl(parse);
#if PG_VERSION_NUM >= 190000
		PlannedStmt *postgres_plan = standard_planner(copied_query, NULL, cursor_options, NULL, NULL);
#else
		PlannedStmt *postgres_plan = standard_planner(copied_query, NULL, cursor_options, NULL);
#endif
		Assert(IsA(postgres_plan->planTree, ModifyTable));
		outerPlan(postgres_plan->planTree) = duckdb_plan;

		postgres_plan->rtable = lappend(postgres_plan->rtable, DuckdbRangeTableEntry(custom_scan));
		SetCustomScanVarno(custom_scan, list_length(postgres_plan->rtable));
		return postgres_plan;
	}

	RangeTblEntry *rte = DuckdbRangeTableEntry(custom_scan);
	SetCustomScanVarno(custom_scan, 1);

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
