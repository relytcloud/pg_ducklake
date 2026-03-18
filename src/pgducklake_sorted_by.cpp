/*
 * pgducklake_sorted_by.cpp -- Sorted index CREATE/DROP interception.
 *
 * HandleCreateSortedIndex translates CREATE INDEX ... USING ducklake_sorted
 * into ALTER TABLE ... SET SORTED BY in DuckDB.
 * FindSortedIndexDrops identifies ducklake_sorted indexes in a DROP statement
 * so the utility hook can issue RESET SORTED BY after the drop.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_index_am.hpp"
#include "pgduckdb/pgduckdb_contracts.hpp"

#include <string>

extern "C" {
#include "postgres.h"

#include "access/relation.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "nodes/value.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
}

namespace pgducklake {

namespace {

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
#if PG_VERSION_NUM >= 150000
    if (ac->isnull)
      return "NULL";
    if (IsA(&ac->val, Integer))
      return std::to_string(ac->val.ival.ival);
    if (IsA(&ac->val, Float))
      return ac->val.fval.fval;
    if (IsA(&ac->val, String))
      return EscapeSQLString(ac->val.sval.sval);
#else
    switch (ac->val.type) {
    case T_Integer:
      return std::to_string(intVal(&ac->val));
    case T_Float:
      return strVal(&ac->val);
    case T_String:
      return EscapeSQLString(strVal(&ac->val));
    default:
      break;
    }
#endif
    return "NULL";
  }
  case T_TypeCast: {
    TypeCast *tc = (TypeCast *)node;
    std::string result = NodeToSQL(tc->arg);
    result += "::";
    ListCell *lc;
    bool first = true;
    foreach (lc, tc->typeName->names) {
      Node *n = (Node *)lfirst(lc);
      if (IsA(n, String)) {
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
    return "";
  }
}

} // anonymous namespace

void HandleCreateSortedIndex(PlannedStmt *pstmt, const char *query_string,
                             bool read_only_tree, ProcessUtilityContext context,
                             ParamListInfo params,
                             struct QueryEnvironment *query_env,
                             DestReceiver *dest, QueryCompletion *qc,
                             ProcessUtility_hook_type prev_hook) {
  IndexStmt *stmt = castNode(IndexStmt, pstmt->utilityStmt);

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

  Oid relid = RangeVarGetRelid(stmt->relation, AccessShareLock, false);
  Oid ducklake_am_oid = get_am_oid("ducklake", false);
  Relation rel = relation_open(relid, AccessShareLock);
  Oid rel_am = rel->rd_rel->relam;
  relation_close(rel, AccessShareLock);

  if (rel_am != ducklake_am_oid)
    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("table \"%s\" is not a DuckLake table",
                           get_rel_name(relid))));

  std::string sort_spec;
  ListCell *lc;
  bool first = true;
  foreach (lc, stmt->indexParams) {
    IndexElem *elem = (IndexElem *)lfirst(lc);

    if (elem->collation != NIL)
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("COLLATE is not supported for ducklake_sorted indexes")));
    if (elem->opclass != NIL) {
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

    if (elem->ordering == SORTBY_DESC)
      sort_spec += " DESC";
    else
      sort_spec += " ASC";

    if (elem->nulls_ordering == SORTBY_NULLS_FIRST)
      sort_spec += " NULLS FIRST";
    else if (elem->nulls_ordering == SORTBY_NULLS_LAST)
      sort_spec += " NULLS LAST";
  }

  prev_hook(pstmt, query_string, read_only_tree, context,
            params, query_env, dest, qc);

  if (syncing_from_metadata)
    return;

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
  int result = ExecuteDuckDBQuery(query.c_str(), &error_msg);
  PopActiveSnapshot();
  if (result != 0)
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to set sort order: %s",
                           error_msg ? error_msg : "unknown error")));
}

std::vector<SortedIndexDrop> FindSortedIndexDrops(DropStmt *drop) {
  std::vector<SortedIndexDrop> result;

  if (drop->removeType != OBJECT_INDEX)
    return result;

  Oid am_oid = get_am_oid(PGDUCKLAKE_SORTED_AM, true);
  if (!OidIsValid(am_oid))
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

    if (relam != am_oid)
      continue;

    Oid table_oid = IndexGetRelation(index_oid, false);
    result.push_back({index_oid, table_oid});
  }

  return result;
}

} // namespace pgducklake
