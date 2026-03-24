/*
 * pgducklake_sorted_by.cpp -- ducklake_sorted index AM, procedures, and sync.
 *
 * @scope extension: ducklake_sorted index AM, procs ducklake.set_sort
 *   and ducklake.reset_sort
 * @scope backend: sort_synced_from_pg guard bool
 * @scope duckdb-instance: sync sorted indexes between DuckDB and pg_class.
 *   SyncSortKeys is registered as a sync handler with pgducklake_sync.cpp.
 *
 * Provides a minimal IndexAmRoutine so that CREATE INDEX ... USING
 * ducklake_sorted registers a real pg_class entry. The index stores no data
 * and is never used by the planner; it exists only as a catalog marker that
 * the utility hook translates into ALTER TABLE ... SET SORTED BY in DuckDB.
 *
 * Also contains: ducklake.set_sort/reset_sort SQL procedures,
 * HandleCreateSortedIndex, HandleDropSortedIndex, FindSortedIndexDrops,
 * SyncSortKeys, and pg_class sync helpers called from pgducklake_hooks.cpp
 * and pgducklake_sync.cpp.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"

#include <duckdb/common/error_data.hpp> /* must precede postgres.h (FATAL macro) */

#include "pgducklake/pgducklake_table.hpp"

#include "pgducklake/pgducklake_sorted_by.hpp"
#include "pgducklake/utility/cpp_wrapper.hpp"
#include "pgduckdb/pgduckdb_contracts.hpp"

#include <string>

#include <duckdb/common/string_util.hpp>

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
#include "utils/array.h"
#include "utils/syscache.h"

#include "pgduckdb/pgduckdb_ruleutils.h"

/* ================================================================
 * Index AM routines
 * ================================================================ */

PG_FUNCTION_INFO_V1(ducklake_sorted_am_handler);

static IndexBuildResult *sorted_ambuild(Relation heap, Relation index, IndexInfo *indexInfo) {
  IndexBuildResult *result = (IndexBuildResult *)palloc0(sizeof(IndexBuildResult));
  result->heap_tuples = 0;
  result->index_tuples = 0;
  return result;
}

static void sorted_ambuildempty(Relation index) {
}

static bool sorted_aminsert(Relation rel, Datum *values, bool *isnull, ItemPointer ht_ctid, Relation heapRel,
                            IndexUniqueCheck checkUnique, bool indexUnchanged, IndexInfo *indexInfo) {
  return false;
}

static IndexBulkDeleteResult *sorted_ambulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                                  IndexBulkDeleteCallback callback, void *callback_state) {
  return stats;
}

static IndexBulkDeleteResult *sorted_amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats) {
  return stats;
}

static void sorted_amcostestimate(PlannerInfo *root, IndexPath *path, double loop_count, Cost *indexStartupCost,
                                  Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation,
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

static void sorted_amrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys) {
}

static void sorted_amendscan(IndexScanDesc scan) {
}

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

/* ================================================================
 * ducklake.set_sort / ducklake.reset_sort procedures
 * ================================================================ */

DECLARE_PG_FUNCTION(ducklake_set_sort) {
  if (PG_ARGISNULL(0))
    elog(ERROR, "table cannot be NULL");
  if (PG_ARGISNULL(1))
    elog(ERROR, "sorted_by cannot be NULL");

  Oid relid = PG_GETARG_OID(0);
  EnsureDuckLakeTable(relid);

  ArrayType *arr = PG_GETARG_ARRAYTYPE_P(1);
  if (ARR_NDIM(arr) == 0)
    elog(ERROR, "sorted_by cannot be empty");

  int nelems;
  Datum *elems;
  bool *nulls;
  deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT, &elems, &nulls, &nelems);

  if (nelems == 0)
    elog(ERROR, "sorted_by cannot be empty");

  std::string spec;
  for (int i = 0; i < nelems; i++) {
    if (nulls[i])
      elog(ERROR, "sort key cannot be NULL");
    if (i > 0)
      spec += ", ";
    spec += text_to_cstring(DatumGetTextPP(elems[i]));
  }

  std::string query = std::string("ALTER TABLE ") + pgduckdb_relation_name(relid) + " SET SORTED BY (" + spec + ")";

  pgducklake::sort_synced_from_pg = true;
  const char *error_msg = nullptr;
  int result = pgducklake::ExecuteDuckDBQuery(query.c_str(), &error_msg);
  pgducklake::sort_synced_from_pg = false;
  if (result != 0)
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to set sort order: %s", error_msg ? error_msg : "unknown error")));

  /* Sync pg_class: drop old ducklake_sorted index, create new one. */
  SPI_connect();
  pgducklake::syncing_from_metadata = true;
  pgducklake::CreateSortedIndexForTable(relid, spec.c_str());
  pgducklake::syncing_from_metadata = false;
  SPI_finish();

  PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_reset_sort) {
  if (PG_ARGISNULL(0))
    elog(ERROR, "table cannot be NULL");

  Oid relid = PG_GETARG_OID(0);
  EnsureDuckLakeTable(relid);

  std::string query = std::string("ALTER TABLE ") + pgduckdb_relation_name(relid) + " RESET SORTED BY";

  pgducklake::sort_synced_from_pg = true;
  const char *error_msg = nullptr;
  int result = pgducklake::ExecuteDuckDBQuery(query.c_str(), &error_msg);
  pgducklake::sort_synced_from_pg = false;
  if (result != 0)
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to reset sort order: %s", error_msg ? error_msg : "unknown error")));

  /* Drop any ducklake_sorted index on this table */
  SPI_connect();
  pgducklake::syncing_from_metadata = true;
  pgducklake::DropSortedIndexForTable(relid);
  pgducklake::syncing_from_metadata = false;
  SPI_finish();

  PG_RETURN_VOID();
}

} /* extern "C" */

namespace pgducklake {

bool sort_synced_from_pg = false;

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
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported expression type in index key")));
    return "";
  }
}

} // anonymous namespace

void HandleCreateSortedIndex(PlannedStmt *pstmt, const char *query_string, bool read_only_tree,
                             ProcessUtilityContext context, ParamListInfo params, struct QueryEnvironment *query_env,
                             DestReceiver *dest, QueryCompletion *qc, ProcessUtility_hook_type prev_hook) {
  IndexStmt *stmt = castNode(IndexStmt, pstmt->utilityStmt);

  if (stmt->concurrent)
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("CONCURRENTLY is not supported for ducklake_sorted indexes")));
  if (stmt->unique)
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("UNIQUE is not supported for ducklake_sorted indexes")));
  if (stmt->whereClause)
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("WHERE clause is not supported for ducklake_sorted indexes")));
  if (list_length(stmt->indexIncludingParams) > 0)
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("INCLUDE is not supported for ducklake_sorted indexes")));
  if (stmt->tableSpace)
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("TABLESPACE is not supported for ducklake_sorted indexes")));

  Oid relid = RangeVarGetRelid(stmt->relation, AccessShareLock, false);
  Oid ducklake_am_oid = get_am_oid("ducklake", false);
  Relation rel = relation_open(relid, AccessShareLock);
  Oid rel_am = rel->rd_rel->relam;
  relation_close(rel, AccessShareLock);

  if (rel_am != ducklake_am_oid)
    ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("table \"%s\" is not a DuckLake table", get_rel_name(relid))));

  std::string sort_spec;
  ListCell *lc;
  bool first = true;
  foreach (lc, stmt->indexParams) {
    IndexElem *elem = (IndexElem *)lfirst(lc);

    if (elem->collation != NIL)
      ereport(ERROR,
              (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("COLLATE is not supported for ducklake_sorted indexes")));
    if (elem->opclass != NIL) {
      if (list_length(elem->opclass) != 2 || strcmp(strVal(linitial(elem->opclass)), PGDUCKLAKE_PG_SCHEMA) != 0)
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
      ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("index key must be a column or expression")));

    if (elem->ordering == SORTBY_DESC)
      sort_spec += " DESC";
    else
      sort_spec += " ASC";

    if (elem->nulls_ordering == SORTBY_NULLS_FIRST)
      sort_spec += " NULLS FIRST";
    else if (elem->nulls_ordering == SORTBY_NULLS_LAST)
      sort_spec += " NULLS LAST";
  }

  prev_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);

  if (syncing_from_metadata)
    return;

  std::string query =
      std::string("ALTER TABLE ") + pgduckdb_relation_name(relid) + " SET SORTED BY (" + sort_spec + ")";

  elog(DEBUG1, "ducklake_sorted: %s", query.c_str());

  PushActiveSnapshot(GetTransactionSnapshot());
  if (!pgduckdb::DuckdbEnsureCacheValid()) {
    PopActiveSnapshot();
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("pg_duckdb is not available")));
  }

  const char *error_msg = nullptr;
  int result = ExecuteDuckDBQuery(query.c_str(), &error_msg);
  PopActiveSnapshot();
  if (result != 0)
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to set sort order: %s", error_msg ? error_msg : "unknown error")));
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
 * Batch-sync ducklake_sorted pg_class indexes.
 * Drops old sorted indexes on all affected tables, then creates new ones.
 * Caller must have an active SPI connection and syncing_from_metadata = true.
 */
void SyncSortedIndexes(const std::vector<SortedIndexCreate> &creates, const std::vector<Oid> &resets) {
  Oid sorted_am_oid = get_am_oid(PGDUCKLAKE_SORTED_AM, true);
  if (!OidIsValid(sorted_am_oid))
    return;

  /* Helper: drop all ducklake_sorted indexes on a table */
  auto drop_indexes = [sorted_am_oid](Oid relid) {
    char *sql = psprintf(R"(
		SELECT c.oid FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
		WHERE i.indrelid = %u AND c.relam = %u
		)",
                         relid, sorted_am_oid);
    int ret = SPI_exec(sql, 0);
    std::vector<Oid> idx_oids;
    if (ret == SPI_OK_SELECT) {
      for (uint64_t j = 0; j < SPI_processed; ++j) {
        bool isnull;
        Oid idx_oid = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[j], SPI_tuptable->tupdesc, 1, &isnull));
        if (!isnull)
          idx_oids.push_back(idx_oid);
      }
    }
    for (Oid idx_oid : idx_oids) {
      char *drop_sql = psprintf("DROP INDEX %s", quote_identifier(get_rel_name(idx_oid)));
      SPI_exec(drop_sql, 0);
    }
  };

  for (auto &action : creates) {
    drop_indexes(action.relid);

    char *schema_name = get_namespace_name(get_rel_namespace(action.relid));
    char *table_name = get_rel_name(action.relid);
    char *sql = psprintf("CREATE INDEX ON %s.%s USING ducklake_sorted (%s)", quote_identifier(schema_name),
                         quote_identifier(table_name), action.sort_spec.c_str());
    elog(DEBUG1, "Sorted index sync: %s", sql);
    int ret = SPI_exec(sql, 0);
    if (ret != SPI_OK_UTILITY)
      elog(ERROR, "SPI_exec CREATE INDEX failed: %s", SPI_result_code_string(ret));
  }

  for (Oid relid : resets)
    drop_indexes(relid);
}

void CreateSortedIndexForTable(Oid relid, const char *sort_spec) {
  SyncSortedIndexes({{relid, sort_spec}}, {});
}

void DropSortedIndexForTable(Oid relid) {
  SyncSortedIndexes({}, {relid});
}

/*
 * Post-DROP INDEX handler: reset sort order in DuckDB for each dropped
 * ducklake_sorted index.  Called from the utility hook after the DROP
 * has been executed by PostgreSQL.
 */
void HandleDropSortedIndex(const std::vector<SortedIndexDrop> &drops) {
  if (drops.empty() || syncing_from_metadata)
    return;

  PushActiveSnapshot(GetTransactionSnapshot());
  if (!pgduckdb::DuckdbEnsureCacheValid()) {
    PopActiveSnapshot();
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("pg_duckdb is not available")));
  }

  for (auto &drop : drops) {
    std::string query = std::string("ALTER TABLE ") + pgduckdb_relation_name(drop.table_oid) + " RESET SORTED BY";
    elog(DEBUG1, "ducklake_sorted drop: %s", query.c_str());

    const char *error_msg = nullptr;
    int result = ExecuteDuckDBQuery(query.c_str(), &error_msg);
    if (result != 0)
      ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                      errmsg("failed to reset sort order: %s", error_msg ? error_msg : "unknown error")));
  }
  PopActiveSnapshot();
}

/*
 * Sync sort keys from DuckLake metadata: create/drop ducklake_sorted
 * pg_class indexes to match sort_info changes in this snapshot.
 * Caller must have an active SPI connection with syncing_from_metadata = true.
 */
void SyncSortKeys(const char *sid) {
  /* Skip sort-key sync when sort was set from PostgreSQL (set_sort/
   * CREATE INDEX already handled the pg_class index; re-running here
   * would deadlock). */
  if (sort_synced_from_pg)
    return;

  /* New sort keys set */
  std::string query = duckdb::StringUtil::Format(R"(
		SELECT s.schema_name, t.table_name,
		       se.expression, se.sort_direction, se.null_order
		FROM ducklake.ducklake_sort_info si
		JOIN ducklake.ducklake_sort_expression se USING (sort_id)
		JOIN ducklake.ducklake_table t ON si.table_id = t.table_id
		JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
		WHERE si.begin_snapshot = %s
		  AND t.end_snapshot IS NULL
		  AND s.end_snapshot IS NULL
		ORDER BY t.table_id, se.sort_key_index
		)",
                                                 sid);

  int ret = SPI_exec(query.c_str(), 0);
  if (ret != SPI_OK_SELECT)
    elog(ERROR, "SPI_exec failed: %s", SPI_result_code_string(ret));

  /* Collect (relid, sort_spec) pairs from SPI results, then batch-execute. */
  std::vector<SortedIndexCreate> sort_creates;

  if (SPI_processed > 0) {
    struct SortKeyInfo {
      std::string schema_name, table_name, expression, direction, null_order;
    };
    std::vector<SortKeyInfo> sort_keys;
    for (uint64_t i = 0; i < SPI_processed; ++i) {
      HeapTuple tup = SPI_tuptable->vals[i];
      TupleDesc td = SPI_tuptable->tupdesc;
      SortKeyInfo sk;
      char *v;
      v = SPI_getvalue(tup, td, 1);
      sk.schema_name = v ? v : "";
      v = SPI_getvalue(tup, td, 2);
      sk.table_name = v ? v : "";
      v = SPI_getvalue(tup, td, 3);
      sk.expression = v ? v : "";
      v = SPI_getvalue(tup, td, 4);
      sk.direction = v ? v : "ASC";
      v = SPI_getvalue(tup, td, 5);
      sk.null_order = v ? v : "";
      sort_keys.push_back(std::move(sk));
    }

    /* Group by table and build sort spec */
    std::string prev_schema, prev_table, idx_cols;
    auto flush = [&]() {
      if (idx_cols.empty())
        return;
      Oid nsp_oid = get_namespace_oid(prev_schema.c_str(), true);
      if (!OidIsValid(nsp_oid))
        return;
      Oid relid = get_relname_relid(prev_table.c_str(), nsp_oid);
      if (!OidIsValid(relid))
        return;
      sort_creates.push_back({relid, std::move(idx_cols)});
      idx_cols.clear();
    };

    for (auto &sk : sort_keys) {
      if (sk.schema_name != prev_schema || sk.table_name != prev_table) {
        flush();
        prev_schema = sk.schema_name;
        prev_table = sk.table_name;
      }

      if (!idx_cols.empty())
        idx_cols += ", ";
      idx_cols += sk.expression;
      idx_cols += " ";
      idx_cols += sk.direction;
      if (!sk.null_order.empty()) {
        if (sk.null_order == "NULLS_FIRST")
          idx_cols += " NULLS FIRST";
        else if (sk.null_order == "NULLS_LAST")
          idx_cols += " NULLS LAST";
      }
    }
    flush();
  }

  /* Sort keys reset */
  query = duckdb::StringUtil::Format(R"(
		SELECT DISTINCT s.schema_name, t.table_name
		FROM ducklake.ducklake_sort_info si
		JOIN ducklake.ducklake_table t ON si.table_id = t.table_id
		JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
		WHERE si.end_snapshot = %s
		  AND t.end_snapshot IS NULL
		  AND s.end_snapshot IS NULL
		  AND NOT EXISTS (
		    SELECT 1 FROM ducklake.ducklake_sort_info si2
		    WHERE si2.table_id = si.table_id
		      AND si2.begin_snapshot = %s
		  )
		)",
                                     sid, sid);

  ret = SPI_exec(query.c_str(), 0);
  if (ret != SPI_OK_SELECT)
    elog(ERROR, "SPI_exec failed: %s", SPI_result_code_string(ret));

  std::vector<Oid> sort_resets;
  if (SPI_processed > 0) {
    for (uint64_t i = 0; i < SPI_processed; ++i) {
      HeapTuple tup = SPI_tuptable->vals[i];
      TupleDesc td = SPI_tuptable->tupdesc;
      char *schema = SPI_getvalue(tup, td, 1);
      char *table = SPI_getvalue(tup, td, 2);
      if (!schema || !table)
        continue;
      Oid nsp_oid = get_namespace_oid(schema, true);
      if (!OidIsValid(nsp_oid))
        continue;
      Oid relid = get_relname_relid(table, nsp_oid);
      if (OidIsValid(relid))
        sort_resets.push_back(relid);
    }
  }

  SyncSortedIndexes(sort_creates, sort_resets);
}

} // namespace pgducklake
