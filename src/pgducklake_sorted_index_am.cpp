/*
 * pgducklake_sorted_index_am.cpp -- Sorted index AM and interception helpers.
 *
 * Provides a minimal IndexAmRoutine so that CREATE INDEX ... USING
 * ducklake_sorted registers a real pg_class entry. The index stores no data
 * and is never used by the planner; it exists only as a catalog marker that
 * the utility hook translates into ALTER TABLE ... SET SORTED BY in DuckDB.
 *
 * Also contains HandleCreateSortedIndex / FindSortedIndexDrops which are
 * called from the utility hook in pgducklake_hooks.cpp.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_sorted_index.hpp"
#include "pgduckdb/pgduckdb_contracts.hpp"

#include <string>

extern "C" {
#include "postgres.h"

#include "access/amapi.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "nodes/pathnodes.h"
#include "nodes/value.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "pgduckdb/pgduckdb_ruleutils.h"

/* ================================================================
 * Index AM routines
 * ================================================================ */

PG_FUNCTION_INFO_V1(ducklake_sorted_am_handler);

static IndexBuildResult *sorted_ambuild(Relation heap, Relation index,
                                        IndexInfo *indexInfo) {
  IndexBuildResult *result = (IndexBuildResult *)palloc0(sizeof(IndexBuildResult));
  result->heap_tuples = 0;
  result->index_tuples = 0;
  return result;
}

static void sorted_ambuildempty(Relation index) {}

static bool sorted_aminsert(Relation rel, Datum *values, bool *isnull,
                            ItemPointer ht_ctid, Relation heapRel,
                            IndexUniqueCheck checkUnique,
                            bool indexUnchanged,
                            IndexInfo *indexInfo) {
  return false;
}

static IndexBulkDeleteResult *sorted_ambulkdelete(IndexVacuumInfo *info,
                                                  IndexBulkDeleteResult *stats,
                                                  IndexBulkDeleteCallback callback,
                                                  void *callback_state) {
  return stats;
}

static IndexBulkDeleteResult *sorted_amvacuumcleanup(IndexVacuumInfo *info,
                                                     IndexBulkDeleteResult *stats) {
  return stats;
}

static void sorted_amcostestimate(PlannerInfo *root, IndexPath *path,
                                  double loop_count,
                                  Cost *indexStartupCost,
                                  Cost *indexTotalCost,
                                  Selectivity *indexSelectivity,
                                  double *indexCorrelation,
                                  double *indexPages) {
  *indexStartupCost = 1.0e10;
  *indexTotalCost = 1.0e10;
  *indexSelectivity = 1.0;
  *indexCorrelation = 0.0;
  *indexPages = 0;
}

static bytea *sorted_amoptions(Datum reloptions, bool validate) {
  return NULL;
}

static bool sorted_amvalidate(Oid opclassoid) {
  return true;
}

static IndexScanDesc sorted_ambeginscan(Relation rel, int nkeys, int norderbys) {
  return RelationGetIndexScan(rel, nkeys, norderbys);
}

static void sorted_amrescan(IndexScanDesc scan, ScanKey keys, int nkeys,
                            ScanKey orderbys, int norderbys) {}

static void sorted_amendscan(IndexScanDesc scan) {}

Datum ducklake_sorted_am_handler(PG_FUNCTION_ARGS) {
  IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

  amroutine->amstrategies = 0;
  amroutine->amsupport = 0;
  amroutine->amoptsprocnum = 0;
  amroutine->amcanorder = true;
  amroutine->amcanorderbyop = false;
  amroutine->amcanbackward = false;
  amroutine->amcanunique = false;
  amroutine->amcanmulticol = true;
  amroutine->amoptionalkey = false;
  amroutine->amsearcharray = false;
  amroutine->amsearchnulls = false;
  amroutine->amstorage = false;
  amroutine->amclusterable = false;
  amroutine->ampredlocks = false;
  amroutine->amcanparallel = false;
#if PG_VERSION_NUM >= 170000
  amroutine->amcanbuildparallel = false;
#endif
  amroutine->amcaninclude = false;
  amroutine->amusemaintenanceworkmem = false;
#if PG_VERSION_NUM >= 180000
  amroutine->amsummarizing = false;
#else
  amroutine->amparallelvacuumoptions = 0;
#endif
  amroutine->amkeytype = InvalidOid;

  amroutine->ambuild = sorted_ambuild;
  amroutine->ambuildempty = sorted_ambuildempty;
  amroutine->aminsert = sorted_aminsert;
  amroutine->ambulkdelete = sorted_ambulkdelete;
  amroutine->amvacuumcleanup = sorted_amvacuumcleanup;
  amroutine->amcanreturn = NULL;
  amroutine->amcostestimate = sorted_amcostestimate;
  amroutine->amoptions = sorted_amoptions;
  amroutine->amproperty = NULL;
  amroutine->ambuildphasename = NULL;
  amroutine->amvalidate = sorted_amvalidate;
  amroutine->ambeginscan = sorted_ambeginscan;
  amroutine->amrescan = sorted_amrescan;
  amroutine->amgettuple = NULL;
  amroutine->amgetbitmap = NULL;
  amroutine->amendscan = sorted_amendscan;
  amroutine->ammarkpos = NULL;
  amroutine->amrestrpos = NULL;
  amroutine->amestimateparallelscan = NULL;
  amroutine->aminitparallelscan = NULL;
  amroutine->amparallelrescan = NULL;

  PG_RETURN_POINTER(amroutine);
}

} /* extern "C" */

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

/*
 * Drop all ducklake_sorted pg_class indexes on a table.
 * Caller must have an active SPI connection and syncing_from_metadata = true.
 */
void DropSortedIndexForTable(Oid relid) {
  Oid sorted_am_oid = get_am_oid(PGDUCKLAKE_SORTED_AM, true);
  if (!OidIsValid(sorted_am_oid))
    return;

  char *sql = psprintf(R"(
	SELECT c.oid FROM pg_catalog.pg_index i
	JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
	WHERE i.indrelid = %u
	  AND c.relam = %u
	)",
                       relid, sorted_am_oid);

  int ret = SPI_exec(sql, 0);
  std::vector<Oid> idx_oids;
  if (ret == SPI_OK_SELECT) {
    for (uint64_t j = 0; j < SPI_processed; ++j) {
      bool isnull;
      Oid idx_oid = DatumGetObjectId(SPI_getbinval(
          SPI_tuptable->vals[j], SPI_tuptable->tupdesc, 1, &isnull));
      if (!isnull)
        idx_oids.push_back(idx_oid);
    }
  }
  for (Oid idx_oid : idx_oids) {
    char *drop_sql = psprintf("DROP INDEX %s",
                              quote_identifier(get_rel_name(idx_oid)));
    SPI_exec(drop_sql, 0);
  }
}

/*
 * Create a ducklake_sorted pg_class index for a table, dropping any existing
 * sorted index first.
 * Caller must have an active SPI connection and syncing_from_metadata = true.
 */
void CreateSortedIndexForTable(Oid relid, const char *sort_spec) {
  DropSortedIndexForTable(relid);

  char *schema_name = get_namespace_name(get_rel_namespace(relid));
  char *table_name = get_rel_name(relid);
  char *sql = psprintf("CREATE INDEX ON %s.%s USING ducklake_sorted (%s)",
                       quote_identifier(schema_name),
                       quote_identifier(table_name),
                       sort_spec);

  elog(DEBUG1, "Sorted index sync: %s", sql);
  int ret = SPI_exec(sql, 0);
  if (ret != SPI_OK_UTILITY)
    elog(ERROR, "SPI_exec CREATE INDEX failed: %s",
         SPI_result_code_string(ret));
}

} // namespace pgducklake
