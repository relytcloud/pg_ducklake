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
#include "pgduckdb/pgduckdb_contracts.hpp"

#include <string>
#include <vector>

extern "C" {
#include "postgres.h"

#include "access/relation.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
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

namespace pgducklake {
extern bool syncing_from_metadata;
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

/* ---- Sorted index helpers ---- */

std::string EscapeSQLString(const char *str) {
  std::string result("'");
  for (const char *p = str; *p; p++) {
    if (*p == '\'')
      result += "''";
    else
      result += *p;
  }
  result += '\'';
  return result;
}

/*
 * Convert a raw parse-tree Node into SQL text.
 * Handles ColumnRef, FuncCall, A_Const, TypeCast.
 */
std::string NodeToSQL(Node *node) {
  if (node == NULL)
    return "";

  switch (nodeTag(node)) {
  case T_ColumnRef: {
    ColumnRef *cr = (ColumnRef *)node;
    std::string result;
    ListCell *lc;
    bool first = true;
    foreach (lc, cr->fields) {
      if (!first)
        result += ".";
      first = false;
      Node *field = (Node *)lfirst(lc);
      if (IsA(field, String))
        result += strVal(field);
    }
    return result;
  }
  case T_FuncCall: {
    FuncCall *fc = (FuncCall *)node;
    std::string result;
    ListCell *lc;
    bool first = true;
    foreach (lc, fc->funcname) {
      if (!first)
        result += ".";
      first = false;
      result += strVal(lfirst(lc));
    }
    result += "(";
    first = true;
    foreach (lc, fc->args) {
      if (!first)
        result += ", ";
      first = false;
      result += NodeToSQL((Node *)lfirst(lc));
    }
    result += ")";
    return result;
  }
  case T_A_Const: {
    A_Const *ac = (A_Const *)node;
    if (ac->isnull)
      return "NULL";
    if (IsA(&ac->val, Integer))
      return std::to_string(ac->val.ival.ival);
    if (IsA(&ac->val, Float))
      return ac->val.fval.fval;
    if (IsA(&ac->val, String))
      return EscapeSQLString(ac->val.sval.sval);
    return "NULL";
  }
  case T_TypeCast: {
    TypeCast *tc = (TypeCast *)node;
    std::string result = NodeToSQL(tc->arg);
    result += "::";
    /* Build type name from TypeName */
    ListCell *lc;
    bool first = true;
    foreach (lc, tc->typeName->names) {
      Node *n = (Node *)lfirst(lc);
      if (IsA(n, String)) {
        /* Skip pg_catalog schema qualifier */
        if (strcmp(strVal(n), "pg_catalog") == 0)
          continue;
        if (!first)
          result += ".";
        first = false;
        result += strVal(n);
      }
    }
    return result;
  }
  default:
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("unsupported expression type in index key")));
    return ""; /* unreachable */
  }
}

/*
 * Handle CREATE INDEX ... USING ducklake_sorted.
 * Validates the statement, lets PG create the catalog entry, then
 * executes ALTER TABLE ... SET SORTED BY in DuckDB.
 */
void HandleCreateSortedIndex(PlannedStmt *pstmt, const char *query_string,
                             bool read_only_tree, ProcessUtilityContext context,
                             ParamListInfo params,
                             struct QueryEnvironment *query_env,
                             DestReceiver *dest, QueryCompletion *qc) {
  IndexStmt *stmt = castNode(IndexStmt, pstmt->utilityStmt);

  /* Reject unsupported options */
  if (stmt->concurrent)
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("CONCURRENTLY is not supported for ducklake_sorted indexes")));
  if (stmt->unique)
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("UNIQUE is not supported for ducklake_sorted indexes")));
  if (stmt->whereClause)
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("WHERE clause is not supported for ducklake_sorted indexes")));
  if (list_length(stmt->indexIncludingParams) > 0)
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("INCLUDE is not supported for ducklake_sorted indexes")));
  if (stmt->tableSpace)
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("TABLESPACE is not supported for ducklake_sorted indexes")));

  /* Resolve table OID and validate it is a DuckLake table */
  Oid relid = RangeVarGetRelid(stmt->relation, AccessShareLock, false);
  Oid ducklake_am_oid = get_am_oid("ducklake", false);
  Relation rel = relation_open(relid, AccessShareLock);
  Oid rel_am = rel->rd_rel->relam;
  relation_close(rel, AccessShareLock);

  if (rel_am != ducklake_am_oid)
    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("table \"%s\" is not a DuckLake table",
                           get_rel_name(relid))));

  /* Validate index elements and build sort spec */
  std::string sort_spec;
  ListCell *lc;
  bool first = true;
  foreach (lc, stmt->indexParams) {
    IndexElem *elem = (IndexElem *)lfirst(lc);

    if (elem->collation != NIL)
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("COLLATE is not supported for ducklake_sorted indexes")));
    if (elem->opclass != NIL) {
      /* Allow if it is only the default opclass from our family */
      if (list_length(elem->opclass) != 2 ||
          strcmp(strVal(linitial(elem->opclass)), PGDUCKLAKE_PG_SCHEMA) != 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("custom opclass is not supported for ducklake_sorted indexes")));
    }

    if (!first)
      sort_spec += ", ";
    first = false;

    if (elem->name)
      sort_spec += elem->name;
    else if (elem->expr)
      sort_spec += NodeToSQL(elem->expr);
    else
      ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                      errmsg("index key must be a column or expression")));

    /* Direction */
    if (elem->ordering == SORTBY_DESC)
      sort_spec += " DESC";
    else
      sort_spec += " ASC";

    /* Null ordering */
    if (elem->nulls_ordering == SORTBY_NULLS_FIRST)
      sort_spec += " NULLS FIRST";
    else if (elem->nulls_ordering == SORTBY_NULLS_LAST)
      sort_spec += " NULLS LAST";
  }

  /* Let PostgreSQL create the index in pg_class */
  prev_process_utility_hook(pstmt, query_string, read_only_tree, context,
                            params, query_env, dest, qc);

  /* Skip the DuckDB call when syncing from metadata (snapshot trigger
   * already created the sort keys in DuckDB; we only need the pg_class entry). */
  if (pgducklake::syncing_from_metadata)
    return;

  /* Execute SET SORTED BY in DuckDB */
  std::string query = std::string("ALTER TABLE ") +
                       pgduckdb_relation_name(relid) +
                       " SET SORTED BY (" + sort_spec + ")";

  elog(DEBUG1, "ducklake_sorted: %s", query.c_str());

  PushActiveSnapshot(GetTransactionSnapshot());
  if (!pgduckdb::DuckdbEnsureCacheValid()) {
    PopActiveSnapshot();
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("pg_duckdb is not available")));
  }

  const char *error_msg = nullptr;
  int result = pgducklake::ExecuteDuckDBQuery(query.c_str(), &error_msg);
  PopActiveSnapshot();
  if (result != 0)
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to set sort order: %s",
                           error_msg ? error_msg : "unknown error")));
}

/*
 * Check whether a DropStmt targets ducklake_sorted indexes.
 * Returns a list of (index OID, parent table OID) pairs for affected indexes.
 */
struct SortedIndexDrop {
  Oid index_oid;
  Oid table_oid;
};

std::vector<SortedIndexDrop> FindSortedIndexDrops(DropStmt *drop) {
  std::vector<SortedIndexDrop> result;

  if (drop->removeType != OBJECT_INDEX)
    return result;

  Oid sorted_am_oid = get_am_oid("ducklake_sorted", true);
  if (!OidIsValid(sorted_am_oid))
    return result;

  ListCell *lc;
  foreach (lc, drop->objects) {
    List *name = (List *)lfirst(lc);
    RangeVar *rv = makeRangeVarFromNameList(name);
    Oid index_oid = RangeVarGetRelid(rv, AccessShareLock, drop->missing_ok);
    if (!OidIsValid(index_oid))
      continue;

    HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(index_oid));
    if (!HeapTupleIsValid(tp))
      continue;

    Form_pg_class classForm = (Form_pg_class)GETSTRUCT(tp);
    Oid relam = classForm->relam;
    ReleaseSysCache(tp);

    if (relam != sorted_am_oid)
      continue;

    Oid table_oid = IndexGetRelation(index_oid, false);
    result.push_back({index_oid, table_oid});
  }

  return result;
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
        strcmp(idx->accessMethod, "ducklake_sorted") == 0) {
      HandleCreateSortedIndex(pstmt, query_string, read_only_tree, context,
                              params, query_env, dest, qc);
      return;
    }
  }

  /* DROP INDEX on ducklake_sorted indexes -- collect before drop */
  std::vector<SortedIndexDrop> sorted_drops;
  if (IsA(parsetree, DropStmt)) {
    DropStmt *drop = castNode(DropStmt, parsetree);
    sorted_drops = FindSortedIndexDrops(drop);
  }

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
