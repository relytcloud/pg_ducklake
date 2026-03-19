/*
 * pgducklake_hooks.cpp
 *
 * Installs pg_ducklake planner and utility hooks.
 *
 * Planner hook:
 * - rewrites regclass overloads of ducklake functions into text-arg versions
 * - delegates INSERT UNNEST planning optimization to pgducklake_direct_insert
 *
 * Utility hook:
 * - catches explicit COMMIT utility statements and commits DuckDB early
 * - intercepts CREATE INDEX USING ducklake_sorted to set DuckLake sort order
 * - intercepts DROP INDEX on ducklake_sorted indexes to reset sort order
 * - detaches the DuckLake catalog on DROP EXTENSION pg_ducklake so that
 *   a subsequent CREATE EXTENSION can re-attach a fresh catalog
 */

#include "pgducklake/pgducklake_hooks.hpp"
#include "pgducklake/pgducklake_call_handler.hpp"
#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_direct_insert.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_fdw.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_sorted_index.hpp"
#include "pgduckdb/pgduckdb_contracts.hpp"

#include <string>

extern "C" {
#include "postgres.h"

#include "access/relation.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "catalog/pg_proc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
}

namespace {

planner_hook_type prev_planner_hook = NULL;
ProcessUtility_hook_type prev_process_utility_hook = NULL;

/*
 * Rewrite a single FuncExpr from regclass to text-arg form.
 *
 * ducklake.list_files('t'::regclass)
 *   -> ducklake.list_files('public', 't')
 */
void TryRewriteRegclassFunc(FuncExpr *func) {
  if (list_length(func->args) < 1)
    return;

  Node *first_arg = (Node *)linitial(func->args);
  if (!IsA(first_arg, Const))
    return;
  Const *regclass_const = (Const *)first_arg;
  if (regclass_const->consttype != REGCLASSOID)
    return;

  /* Confirm function belongs to the ducklake schema */
  Oid ducklake_nsp = get_namespace_oid(PGDUCKLAKE_PG_SCHEMA, true);
  if (!OidIsValid(ducklake_nsp))
    return;
  if (get_func_namespace(func->funcid) != ducklake_nsp)
    return;

  /* Look up the text-arg version; skip if none exists (e.g. get_partition) */
  char *func_name = get_func_name(func->funcid);
  List *func_name_list = list_make2(makeString(pstrdup(PGDUCKLAKE_PG_SCHEMA)),
                                    makeString(func_name));

  int old_nargs = list_length(func->args);
  int new_nargs = old_nargs + 1; /* regclass -> (text, text) */
  Oid *new_argtypes = (Oid *)palloc(sizeof(Oid) * new_nargs);
  new_argtypes[0] = TEXTOID;
  new_argtypes[1] = TEXTOID;

  int i = 2;
  ListCell *lc;
  for_each_from(lc, func->args, 1) {
    new_argtypes[i++] = exprType((Node *)lfirst(lc));
  }

  Oid text_funcid = LookupFuncName(func_name_list, new_nargs,
                                   new_argtypes, true);
  if (!OidIsValid(text_funcid))
    return;

  if (regclass_const->constisnull)
    ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("table argument cannot be NULL")));

  Oid relid = DatumGetObjectId(regclass_const->constvalue);

  /* Validate that the target is a ducklake table */
  Oid ducklake_am_oid = get_am_oid("ducklake", false);
  Relation rel = relation_open(relid, AccessShareLock);
  Oid rel_am = rel->rd_rel->relam;
  relation_close(rel, AccessShareLock);

  if (rel_am != ducklake_am_oid)
    ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
             errmsg("table \"%s\" is not a DuckLake table",
                    get_rel_name(relid))));

  /* Resolve OID to schema + table names */
  char *schema_name = get_namespace_name(get_rel_namespace(relid));
  char *table_name = get_rel_name(relid);

  /* Build new args: (schema_text, table_text, remaining...) */
  Const *schema_const =
      makeConst(TEXTOID, -1, InvalidOid, -1,
                CStringGetTextDatum(schema_name), false, false);
  Const *table_const =
      makeConst(TEXTOID, -1, InvalidOid, -1,
                CStringGetTextDatum(table_name), false, false);

  List *new_args = list_make2(schema_const, table_const);
  for_each_from(lc, func->args, 1) {
    new_args = lappend(new_args, lfirst(lc));
  }

  func->funcid = text_funcid;
  func->args = new_args;
}

bool RewriteRegclassWalker(Node *node, void *context) {
  if (node == NULL)
    return false;

  if (IsA(node, FuncExpr))
    TryRewriteRegclassFunc((FuncExpr *)node);

  if (IsA(node, Query)) {
#if PG_VERSION_NUM >= 160000
    return query_tree_walker((Query *)node, RewriteRegclassWalker,
                             context, 0);
#else
    return query_tree_walker((Query *)node,
                             (bool (*)())((void *)RewriteRegclassWalker),
                             context, 0);
#endif
  }

#if PG_VERSION_NUM >= 160000
  return expression_tree_walker(node, RewriteRegclassWalker, context);
#else
  return expression_tree_walker(
      node, (bool (*)())((void *)RewriteRegclassWalker), context);
#endif
}

void RewriteRegclassFunctions(Query *parse) {
#if PG_VERSION_NUM >= 160000
  query_tree_walker(parse, RewriteRegclassWalker, NULL, 0);
#else
  query_tree_walker(parse, (bool (*)())((void *)RewriteRegclassWalker),
                    NULL, 0);
#endif
}

PlannedStmt *DucklakePlannerHook(Query *parse, const char *query_string,
                                 int cursor_options,
                                 ParamListInfo bound_params) {
  if (pgducklake::enable_direct_insert) {
    PlannedStmt *direct_insert_plan =
        pgducklake::TryCreateDirectInsertPlan(parse, bound_params);
    if (direct_insert_plan)
      return direct_insert_plan;
  }

  if (pgduckdb::DuckdbIsInitialized()) {
    /* Rewrite regclass overloads before pg_duckdb sees the query */
    RewriteRegclassFunctions(parse);
    /* ATTACH databases for any ducklake FDW tables before pg_duckdb plans */
    pgducklake::RegisterForeignTablesInQuery(parse);
  }

  return prev_planner_hook(parse, query_string, cursor_options, bound_params);
}

bool IsCommitUtilityStmt(PlannedStmt *pstmt) {
  if (!pstmt || !pstmt->utilityStmt ||
      !IsA(pstmt->utilityStmt, TransactionStmt))
    return false;

  auto *stmt = castNode(TransactionStmt, pstmt->utilityStmt);
  return stmt->kind == TRANS_STMT_COMMIT;
}

void ForceDuckDBCommitOnExplicitCommit() {
  const char *duckdb_errmsg = nullptr;
  int rc = pgducklake::ExecuteDuckDBQuery("COMMIT", &duckdb_errmsg);
  if (rc == 0)
    return;

  // Explicit PG COMMIT should always be mirrored to DuckDB COMMIT here.
  ereport(ERROR, (errmsg("pg_ducklake commit hook failed to commit DuckDB: %s",
                         duckdb_errmsg ? duckdb_errmsg : "unknown error")));
}

/*
 * Check whether the statement is DROP EXTENSION pg_ducklake.
 */
bool IsDropDucklakeExtensionStmt(PlannedStmt *pstmt) {
  if (!pstmt || !pstmt->utilityStmt || !IsA(pstmt->utilityStmt, DropStmt))
    return false;

  auto *drop = castNode(DropStmt, pstmt->utilityStmt);
  if (drop->removeType != OBJECT_EXTENSION)
    return false;

  ListCell *lc;
  foreach (lc, drop->objects) {
    char *extname = strVal(lfirst(lc));
    if (strcmp(extname, PGDUCKLAKE_PG_EXTENSION) == 0)
      return true;
  }
  return false;
}

/*
 * Check whether a procedure OID belongs to the ducklake schema and was
 * registered via the ducklake_only_procedure C stub.
 */
bool IsDucklakeOnlyProcedure(Oid funcid) {
  Oid ducklake_nsp = get_namespace_oid(PGDUCKLAKE_PG_SCHEMA, true);
  if (!OidIsValid(ducklake_nsp))
    return false;
  if (get_func_namespace(funcid) != ducklake_nsp)
    return false;
  HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
  if (!HeapTupleIsValid(tp))
    return false;
  bool isnull;
  Datum prosrc_datum = SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_prosrc, &isnull);
  if (isnull) {
    ReleaseSysCache(tp);
    return false;
  }
  char *prosrc_str = TextDatumGetCString(prosrc_datum);
  ReleaseSysCache(tp);
  return strcmp(prosrc_str, "ducklake_only_procedure") == 0;
}

void DucklakeUtilityHook(PlannedStmt *pstmt, const char *query_string,
                         bool read_only_tree, ProcessUtilityContext context,
                         ParamListInfo params,
                         struct QueryEnvironment *query_env, DestReceiver *dest,
                         QueryCompletion *qc) {
  if (IsCommitUtilityStmt(pstmt) && pgduckdb::DuckdbIsInitialized()) {
    elog(DEBUG1, "pg_ducklake utility hook caught COMMIT");
    ForceDuckDBCommitOnExplicitCommit();
  }

  Node *parsetree = pstmt->utilityStmt;

  if (IsA(parsetree, CallStmt)) {
    CallStmt *call = castNode(CallStmt, parsetree);
    if (call->funcexpr && IsDucklakeOnlyProcedure(call->funcexpr->funcid)) {
      pgducklake::HandleDuckdbCall(call, query_string);
      if (qc)
        SetQueryCompletion(qc, CMDTAG_CALL, 0);
      return;
    }
  }

  /* CREATE INDEX ... USING ducklake_sorted */
  if (IsA(parsetree, IndexStmt)) {
    IndexStmt *idx = castNode(IndexStmt, parsetree);
    if (idx->accessMethod &&
        strcmp(idx->accessMethod, PGDUCKLAKE_SORTED_AM) == 0) {
      pgducklake::HandleCreateSortedIndex(pstmt, query_string, read_only_tree,
                                          context, params, query_env, dest, qc,
                                          prev_process_utility_hook);
      return;
    }
  }

  /* DROP INDEX on ducklake_sorted indexes -- collect before drop */
  std::vector<pgducklake::SortedIndexDrop> sorted_drops;
  if (IsA(parsetree, DropStmt))
    sorted_drops = pgducklake::FindSortedIndexDrops(castNode(DropStmt, parsetree));

  bool dropping_extension = IsDropDucklakeExtensionStmt(pstmt);

  prev_process_utility_hook(pstmt, query_string, read_only_tree, context,
                            params, query_env, dest, qc);

  /* After DROP INDEX completes, reset sort order in DuckDB.
   * Skip when syncing from metadata (snapshot trigger already reset the sort). */
  if (!sorted_drops.empty() && !pgducklake::syncing_from_metadata) {
    PushActiveSnapshot(GetTransactionSnapshot());
    if (!pgduckdb::DuckdbEnsureCacheValid()) {
      PopActiveSnapshot();
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("pg_duckdb is not available")));
    }

    for (auto &drop : sorted_drops) {
      std::string query = std::string("ALTER TABLE ") +
                           pgduckdb_relation_name(drop.table_oid) +
                           " RESET SORTED BY";
      elog(DEBUG1, "ducklake_sorted drop: %s", query.c_str());

      const char *error_msg = nullptr;
      int result = pgducklake::ExecuteDuckDBQuery(query.c_str(), &error_msg);
      if (result != 0)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("failed to reset sort order: %s",
                               error_msg ? error_msg : "unknown error")));
    }
    PopActiveSnapshot();
  }

  // After DROP EXTENSION completes, detach the DuckLake catalog from DuckDB
  // so that a subsequent CREATE EXTENSION can attach a fresh one.
  if (dropping_extension)
    ducklake_detach_catalog();
}

} // namespace

namespace pgducklake {
void InitHooks() {
  // Install planner hook after pg_duckdb (LIFO: our hook runs first).
  prev_planner_hook = planner_hook ? planner_hook : standard_planner;
  planner_hook = DucklakePlannerHook;

  // Chain ProcessUtility so we can observe COMMIT utility statements.
  prev_process_utility_hook =
      ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
  ProcessUtility_hook = DucklakeUtilityHook;
}

} // namespace pgducklake
