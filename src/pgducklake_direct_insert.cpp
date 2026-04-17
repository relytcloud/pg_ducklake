/*
 * pgducklake_direct_insert.cpp
 *
 * @scope backend: register custom scan node methods; cached ducklake AM OID
 * @scope duckdb-instance: per-statement scan state (snapshot, row IDs)
 *
 * Optimization for INSERT patterns that bypass DuckDB execution:
 *   1. INSERT ... SELECT UNNEST($1), UNNEST($2), ... (parameterized arrays)
 *   2. INSERT ... VALUES (const, ...), ... (constant-value rows)
 *
 * Both paths detect the pattern at planner time, create a CustomScan plan,
 * and insert directly into the inlined data table.  UNNEST uses SPI;
 * VALUES uses table_multi_insert for native heap performance.
 *
 * Lifecycle:
 * 1. Planner hook detects pattern and creates custom scan plan
 * 2. Executor initializes state and allocates snapshot/row IDs
 * 3. Executor inserts rows (SPI for UNNEST, heap AM for VALUES)
 * 4. Executor returns completion (no tuples to output)
 */

#include "duckdb.hpp"

#include "pgducklake/pgducklake_direct_insert.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"
#include "pgducklake/pgducklake_sync.hpp"

#include <unordered_map>

extern "C" {
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#endif
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/value.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
}

namespace pgducklake {

/*
 * Session-level caches for metadata that is stable across EXECUTE calls.
 * Cleared on DuckDB instance recycle (recycle_ddb) via ResetDirectInsertCaches().
 */
struct InliningInfoCache {
  uint64_t table_id;
  uint64_t schema_version;
};

struct InlinedColumnTypesCache {
  List *col_types; // List of Oid, palloc'd in TopMemoryContext
};

static std::unordered_map<Oid, InliningInfoCache> inlining_info_cache;
/* Keyed by (table_id, schema_version) so a DDL that changes column
 * types (and bumps schema_version) invalidates the cache entry. */
struct TableSchemaKey {
  uint64_t table_id;
  uint64_t schema_version;
  bool operator==(const TableSchemaKey &o) const {
    return table_id == o.table_id && schema_version == o.schema_version;
  }
};
struct TableSchemaKeyHash {
  size_t operator()(const TableSchemaKey &k) const {
    return std::hash<uint64_t>()(k.table_id) ^ (std::hash<uint64_t>()(k.schema_version) << 1);
  }
};
static std::unordered_map<TableSchemaKey, InlinedColumnTypesCache, TableSchemaKeyHash> inlined_col_types_cache;

void ResetDirectInsertCaches() {
  inlining_info_cache.clear();
  for (auto &entry : inlined_col_types_cache) {
    list_free(entry.second.col_types);
  }
  inlined_col_types_cache.clear();
}

/* ----------------------------------------------------------------
 * Mode discriminant and scan state
 * ---------------------------------------------------------------- */

enum DirectInsertMode {
  DIRECT_INSERT_UNNEST = 0,
  DIRECT_INSERT_VALUES = 1,
};

// Define the scan state structure here (needs full CustomScanState definition)
struct DirectInsertScanState {
  CustomScanState css; // Must be first

  DirectInsertMode mode;
  Oid target_table_oid;
  uint64_t table_id;
  uint64_t schema_version;
  char *inlined_table_name;
  List *column_names; // List of String nodes
  List *column_types; // List of Oid (inlined table types)

  /* UNNEST-specific */
  List *param_ids; // List of int
  int expected_row_count;
  ParamListInfo bound_params;

  /* VALUES-specific */
  int values_num_rows;
  int values_num_cols;
  Datum *values_data;    // flat [row * num_cols + col]
  bool *values_nulls;    // flat [row * num_cols + col]
  Oid *values_src_types; // per-column source OID

  bool finished;
  int64_t rows_inserted;

  uint64_t begin_snapshot;
  uint64_t next_row_id;
};

// Custom scan methods
static Node *DirectInsert_CreateCustomScanState(CustomScan *cscan);
static void DirectInsert_BeginCustomScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *DirectInsert_ExecCustomScan(CustomScanState *node);
static void DirectInsert_EndCustomScan(CustomScanState *node);
static void DirectInsert_ReScanCustomScan(CustomScanState *node);
static void DirectInsert_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es);

static CustomExecMethods direct_insert_exec_methods = {
    .CustomName = "DuckLakeDirectInsert",
    .BeginCustomScan = DirectInsert_BeginCustomScan,
    .ExecCustomScan = DirectInsert_ExecCustomScan,
    .EndCustomScan = DirectInsert_EndCustomScan,
    .ReScanCustomScan = DirectInsert_ReScanCustomScan,
    .MarkPosCustomScan = NULL,
    .RestrPosCustomScan = NULL,
    .EstimateDSMCustomScan = NULL,
    .InitializeDSMCustomScan = NULL,
    .ReInitializeDSMCustomScan = NULL,
    .InitializeWorkerCustomScan = NULL,
    .ShutdownCustomScan = NULL,
    .ExplainCustomScan = DirectInsert_ExplainCustomScan,
};

static CustomScanMethods direct_insert_scan_methods = {
    .CustomName = "DuckLakeDirectInsert",
    .CreateCustomScanState = DirectInsert_CreateCustomScanState,
};

/* Context for VALUES detection (file-local) */
struct ValuesInsertContext {
  Oid target_table_oid;
  uint64_t table_id;
  uint64_t schema_version;
  int num_rows;
  int num_cols;
  List *target_col_names;  // List of char*
  List *inlined_col_types; // List of Oid
  List *src_col_types;     // List of Oid (user-facing PG types)
  Const **consts;          // flat [num_rows * num_cols] Const nodes
};

/* Shared precondition result for INSERT pattern detection */
struct InsertPreconditionResult {
  Oid target_oid;
  uint64_t table_id;
  uint64_t schema_version;
  Relation target_rel; // caller must close
};

// Helper functions
static bool CheckInsertPreconditions(Query *parse, InsertPreconditionResult *result_out);
static bool TryDetectDirectInsertPattern(Query *parse, ParamListInfo bound_params, DirectInsertContext *context_out);
static bool TryDetectValuesInsertPattern(Query *parse, ValuesInsertContext *context_out);
static bool IsUnnestOfParam(Node *node, int *param_id_out, Oid *param_type_out);
static bool ValidateArrayLengths(ParamListInfo bound_params, List *param_ids, int *expected_row_count_out);
static PlannedStmt *CreateDirectInsertPlan(Query *parse, DirectInsertContext *context);
static PlannedStmt *CreateValuesInsertPlan(Query *parse, ValuesInsertContext *context);
static void DirectInsertIntoInlinedTable(DirectInsertScanState *state);
static void DirectInsertValuesIntoInlinedTable(DirectInsertScanState *state);

/*
 * Map a DuckDB type string (from ducklake_column.column_type) to the PG OID
 * used in the inlined data table.  Mirrors PostgresMetadataManager::
 * GetColumnTypeInternal / TypeIsNativelySupported.
 *
 * Returns the PG OID for the inlined column, or InvalidOid when the type
 * cannot be handled by the direct insert path (nested types, VARIANT, etc.).
 */
static Oid DuckDBTypeToInlinedOid(const char *duckdb_type, Oid element_type) {
  // Nested types stored as VARCHAR in the inlined table -- bail out,
  // these cannot appear in the UNNEST($param) pattern.
  // DuckDB ToString() uses mixed case: "STRUCT(...)", "MAP(...)", "INTEGER[]".
  if (pg_strncasecmp(duckdb_type, "STRUCT", 6) == 0 || pg_strncasecmp(duckdb_type, "MAP", 3) == 0 ||
      strchr(duckdb_type, '[') != NULL) {
    return InvalidOid;
  }

  // VARIANT and GEOMETRY do not support inlining at all
  if (pg_strcasecmp(duckdb_type, "VARIANT") == 0 || pg_strcasecmp(duckdb_type, "GEOMETRY") == 0) {
    return InvalidOid;
  }

  // VARCHAR and BLOB are stored as BYTEA
  if (pg_strcasecmp(duckdb_type, "VARCHAR") == 0 || pg_strcasecmp(duckdb_type, "BLOB") == 0) {
    return BYTEAOID;
  }

  // Scalar types with wider DuckDB range are stored as VARCHAR.
  // TIMESTAMPTZ is excluded: timestamptz_out crashes when called from
  // the VALUES direct insert path after a prior direct insert modified
  // snapshot state in the same session.
  if (pg_strcasecmp(duckdb_type, "TIMESTAMP WITH TIME ZONE") == 0 || pg_strcasecmp(duckdb_type, "TIMESTAMPTZ") == 0) {
    return InvalidOid;
  }
  if (pg_strcasecmp(duckdb_type, "UBIGINT") == 0 || pg_strcasecmp(duckdb_type, "HUGEINT") == 0 ||
      pg_strcasecmp(duckdb_type, "UHUGEINT") == 0 || pg_strcasecmp(duckdb_type, "DATE") == 0 ||
      pg_strcasecmp(duckdb_type, "TIMESTAMP") == 0 || pg_strcasecmp(duckdb_type, "TIMESTAMP_S") == 0 ||
      pg_strcasecmp(duckdb_type, "TIMESTAMP_MS") == 0 || pg_strcasecmp(duckdb_type, "TIMESTAMP_NS") == 0) {
    return VARCHAROID;
  }

  // Natively supported -- PG element type matches the inlined column type
  return element_type;
}

/*
 * Query ducklake_column metadata to determine the PG types used in the
 * inlined data table for each user column.  Returns false on bail-out
 * (nested types, missing metadata, column count mismatch).
 *
 * element_types: List of Oid -- the user-facing PG type for each column
 * (array element type for UNNEST, column type for VALUES).
 */
static bool GetInlinedColumnTypes(uint64_t table_id, List *element_types, List **inlined_col_types_out) {
  int ret;
  int num_cols = list_length(element_types);

  // Allocate in the caller's memory context -- SPI_connect switches to a
  // private context that is freed by SPI_finish, so List nodes built inside
  // SPI would be freed too.
  Oid *oids = (Oid *)palloc(sizeof(Oid) * num_cols);

  if ((ret = SPI_connect()) < 0) {
    return false;
  }

  StringInfoData query;
  initStringInfo(&query);
  appendStringInfo(&query, R"(
SELECT column_type
FROM ducklake.ducklake_column
WHERE table_id = %llu
AND end_snapshot IS NULL
AND parent_column IS NULL
ORDER BY column_order)",
                   (unsigned long long)table_id);

  ret = SPI_execute(query.data, true, 0);
  if (ret != SPI_OK_SELECT) {
    SPI_finish();
    return false;
  }

  if ((int)SPI_processed != num_cols) {
    SPI_finish();
    return false;
  }

  ListCell *lc = list_head(element_types);
  for (int i = 0; i < num_cols; i++) {
    bool isnull;
    Datum type_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
    if (isnull) {
      SPI_finish();
      return false;
    }
    char *duckdb_type = TextDatumGetCString(type_datum);

    Oid element_type = lfirst_oid(lc);
    Oid inlined_oid = DuckDBTypeToInlinedOid(duckdb_type, element_type);
    pfree(duckdb_type);

    if (!OidIsValid(inlined_oid)) {
      SPI_finish();
      return false;
    }
    oids[i] = inlined_oid;
    lc = lnext(element_types, lc);
  }

  SPI_finish();

  List *result = NIL;
  for (int i = 0; i < num_cols; i++) {
    result = lappend_oid(result, oids[i]);
  }
  pfree(oids);

  *inlined_col_types_out = result;
  return true;
}

void RegisterDirectInsertNode() {
  RegisterCustomScanMethods(&direct_insert_scan_methods);
}

/* ----------------------------------------------------------------
 * Shared precondition checks for INSERT optimization
 * ---------------------------------------------------------------- */

/*
 * Check preconditions shared by UNNEST and VALUES patterns:
 *   1. CMD_INSERT
 *   2. Not in explicit transaction block
 *   3. Target RTE is RTE_RELATION
 *   4. Target table uses ducklake AM
 *   5. Table has inlined data (GetTableInliningInfo)
 *
 * On success, result_out->target_rel is open with AccessShareLock --
 * the caller MUST close it.
 */
static bool CheckInsertPreconditions(Query *parse, InsertPreconditionResult *result_out) {
  if (parse->commandType != CMD_INSERT) {
    return false;
  }

  if (IsTransactionBlock()) {
    return false;
  }

  if (parse->resultRelation == 0 || list_length(parse->rtable) < parse->resultRelation) {
    return false;
  }

  RangeTblEntry *target_rte = (RangeTblEntry *)list_nth(parse->rtable, parse->resultRelation - 1);
  if (target_rte->rtekind != RTE_RELATION) {
    return false;
  }

  Oid target_oid = target_rte->relid;

  static Oid ducklake_am_oid = InvalidOid;
  if (!OidIsValid(ducklake_am_oid))
    ducklake_am_oid = get_am_oid("ducklake", true);
  if (!OidIsValid(ducklake_am_oid))
    return false;

  Relation target_rel = relation_open(target_oid, AccessShareLock);
  Oid am_oid = target_rel->rd_rel->relam;

  if (am_oid != ducklake_am_oid) {
    relation_close(target_rel, AccessShareLock);
    return false;
  }

  uint64_t table_id, schema_version;
  auto cache_it = inlining_info_cache.find(target_oid);
  if (cache_it != inlining_info_cache.end()) {
    table_id = cache_it->second.table_id;
    schema_version = cache_it->second.schema_version;
  } else {
    if (!pgducklake::GetTableInliningInfo(target_oid, &table_id, &schema_version)) {
      relation_close(target_rel, AccessShareLock);
      return false;
    }
    inlining_info_cache[target_oid] = {table_id, schema_version};
  }

  result_out->target_oid = target_oid;
  result_out->table_id = table_id;
  result_out->schema_version = schema_version;
  result_out->target_rel = target_rel;
  return true;
}

/*
 * Retrieve inlined column types, using the session-level cache.
 * On cache miss, queries ducklake_column metadata via SPI and caches
 * the result in TopMemoryContext.  Caller must not free the returned list.
 *
 * element_types: List of Oid -- user-facing PG type per column.
 */
static bool GetCachedInlinedColumnTypes(uint64_t table_id, uint64_t schema_version, List *element_types,
                                        List **inlined_col_types_out) {
  TableSchemaKey col_key = {table_id, schema_version};
  auto col_cache_it = inlined_col_types_cache.find(col_key);
  if (col_cache_it != inlined_col_types_cache.end()) {
    *inlined_col_types_out = col_cache_it->second.col_types;
    return true;
  }

  List *inlined_col_types = NIL;
  if (!GetInlinedColumnTypes(table_id, element_types, &inlined_col_types)) {
    return false;
  }

  MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);
  List *persistent = NIL;
  ListCell *lc_oid;
  foreach (lc_oid, inlined_col_types) {
    persistent = lappend_oid(persistent, lfirst_oid(lc_oid));
  }
  MemoryContextSwitchTo(old_ctx);
  inlined_col_types_cache[col_key] = {persistent};
  *inlined_col_types_out = persistent;
  return true;
}

/* ----------------------------------------------------------------
 * Entry point: try both UNNEST and VALUES patterns
 * ---------------------------------------------------------------- */

PlannedStmt *TryCreateDirectInsertPlan(Query *parse, ParamListInfo bound_params) {
  /* Try UNNEST pattern first (requires bound parameters) */
  DirectInsertContext context = {};
  if (TryDetectDirectInsertPattern(parse, bound_params, &context)) {
    ereport(DEBUG1, (errmsg("DuckLake direct insert: optimization enabled for "
                            "INSERT UNNEST pattern, "
                            "table_id=%lu, expected_rows=%d",
                            (unsigned long)context.table_id, context.expected_row_count)));
    return CreateDirectInsertPlan(parse, &context);
  }

  /* Try VALUES pattern */
  ValuesInsertContext values_ctx = {};
  if (TryDetectValuesInsertPattern(parse, &values_ctx)) {
    ereport(DEBUG1, (errmsg("DuckLake direct insert: optimization enabled for "
                            "INSERT VALUES pattern, "
                            "table_id=%lu, rows=%d",
                            (unsigned long)values_ctx.table_id, values_ctx.num_rows)));
    return CreateValuesInsertPlan(parse, &values_ctx);
  }

  return nullptr;
}

/* ----------------------------------------------------------------
 * UNNEST pattern detection (existing, refactored to use shared preconditions)
 * ---------------------------------------------------------------- */

static bool TryDetectDirectInsertPattern(Query *parse, ParamListInfo bound_params, DirectInsertContext *context_out) {
  InsertPreconditionResult precond = {};
  if (!CheckInsertPreconditions(parse, &precond)) {
    return false;
  }

  /* From here, precond.target_rel is open -- must close on all paths. */
  Relation target_rel = precond.target_rel;

  // Check 6: Must have SELECT query as source
  if (!parse->jointree || !parse->jointree->fromlist || list_length(parse->jointree->fromlist) != 1) {
    relation_close(target_rel, AccessShareLock);
    return false;
  }

  // Extract the SELECT subquery
  Node *from_node = (Node *)linitial(parse->jointree->fromlist);
  if (!IsA(from_node, RangeTblRef)) {
    relation_close(target_rel, AccessShareLock);
    return false;
  }

  int from_rtindex = ((RangeTblRef *)from_node)->rtindex;
  RangeTblEntry *from_rte = (RangeTblEntry *)list_nth(parse->rtable, from_rtindex - 1);

  // Check if it's a subquery RTE
  Query *subquery = NULL;
  if (from_rte->rtekind == RTE_SUBQUERY) {
    subquery = from_rte->subquery;
  } else if (from_rte->rtekind == RTE_RELATION) {
    subquery = parse;
  } else {
    relation_close(target_rel, AccessShareLock);
    return false;
  }

  // Check 7: Target list must contain only UNNEST(Param) expressions
  if (!subquery->targetList) {
    relation_close(target_rel, AccessShareLock);
    return false;
  }

  // Get target table column names from tuple descriptor (relation is already
  // open)
  TupleDesc tupdesc = RelationGetDescr(target_rel);

  List *param_infos = NIL;
  List *target_col_names = NIL;
  List *target_col_types = NIL;

  int attno = 0;
  ListCell *lc;
  foreach (lc, subquery->targetList) {
    TargetEntry *tle = (TargetEntry *)lfirst(lc);

    int param_id;
    Oid param_type;
    if (!IsUnnestOfParam((Node *)tle->expr, &param_id, &param_type)) {
      relation_close(target_rel, AccessShareLock);
      return false;
    }

    // Store parameter info
    ParamInfo *pinfo = (ParamInfo *)palloc(sizeof(ParamInfo));
    pinfo->param_id = param_id;
    pinfo->param_type = param_type;

    // Get element type
    Oid element_type = get_element_type(param_type);
    if (!OidIsValid(element_type)) {
      relation_close(target_rel, AccessShareLock);
      return false;
    }
    pinfo->element_type = element_type;

    param_infos = lappend(param_infos, pinfo);

    // Get actual column name from target relation
    if (attno >= tupdesc->natts) {
      relation_close(target_rel, AccessShareLock);
      return false;
    }
    Form_pg_attribute attr = TupleDescAttr(tupdesc, attno);
    target_col_names = lappend(target_col_names, pstrdup(NameStr(attr->attname)));
    target_col_types = lappend_oid(target_col_types, element_type);
    attno++;
  }

  relation_close(target_rel, AccessShareLock);

  // Check 8: All parameters must be present and have matching array lengths
  int expected_row_count = 0;
  List *param_ids = NIL;
  foreach (lc, param_infos) {
    ParamInfo *pinfo = (ParamInfo *)lfirst(lc);
    param_ids = lappend_int(param_ids, pinfo->param_id);
  }

  if (!ValidateArrayLengths(bound_params, param_ids, &expected_row_count)) {
    return false;
  }

  // Check 9: Inlined column types
  List *element_types = NIL;
  foreach (lc, param_infos) {
    ParamInfo *pinfo = (ParamInfo *)lfirst(lc);
    element_types = lappend_oid(element_types, pinfo->element_type);
  }

  List *inlined_col_types = NIL;
  if (!GetCachedInlinedColumnTypes(precond.table_id, precond.schema_version, element_types, &inlined_col_types)) {
    return false;
  }

  // All checks passed, fill context
  context_out->target_table_oid = precond.target_oid;
  context_out->table_id = precond.table_id;
  context_out->schema_version = precond.schema_version;
  context_out->param_infos = param_infos;
  context_out->expected_row_count = expected_row_count;
  context_out->target_col_names = target_col_names;
  context_out->target_col_types = inlined_col_types;

  return true;
}

static bool IsUnnestOfParam(Node *node, int *param_id_out, Oid *param_type_out) {
  if (!node) {
    return false;
  }

  // Handle FuncExpr
  if (IsA(node, FuncExpr)) {
    FuncExpr *funcexpr = (FuncExpr *)node;

    // Check if it's UNNEST function
    char *funcname = get_func_name(funcexpr->funcid);
    if (!funcname || strcmp(funcname, "unnest") != 0) {
      return false;
    }

    // Check if argument is a Param node
    if (list_length(funcexpr->args) != 1) {
      return false;
    }

    Node *arg = (Node *)linitial(funcexpr->args);
    if (!IsA(arg, Param)) {
      return false;
    }

    Param *param = (Param *)arg;
    if (param->paramkind != PARAM_EXTERN) {
      return false;
    }

    *param_id_out = param->paramid;
    *param_type_out = param->paramtype;

    return true;
  }

  return false;
}

static bool ValidateArrayLengths(ParamListInfo bound_params, List *param_ids, int *expected_row_count_out) {
  if (!bound_params) {
    return false;
  }

  int expected_length = -1;
  ListCell *lc;

  foreach (lc, param_ids) {
    int param_id = lfirst_int(lc);

    // Param IDs are 1-indexed
    if (param_id < 1 || param_id > bound_params->numParams) {
      return false;
    }

    ParamExternData *pdata = &bound_params->params[param_id - 1];
    if (pdata->isnull) {
      return false;
    }

    // Must be an array
    Oid param_type = pdata->ptype;
    if (!type_is_array(param_type)) {
      return false;
    }

    // Get array length
    ArrayType *arr = DatumGetArrayTypeP(pdata->value);
    int ndims = ARR_NDIM(arr);
    if (ndims != 1) {
      return false;
    }

    int arr_length = ArrayGetNItems(ndims, ARR_DIMS(arr));

    if (expected_length == -1) {
      expected_length = arr_length;
    } else if (arr_length != expected_length) {
      // Array length mismatch
      return false;
    }
  }

  if (expected_length <= 0) {
    return false;
  }

  *expected_row_count_out = expected_length;
  return true;
}

/* ----------------------------------------------------------------
 * VALUES pattern detection
 * ---------------------------------------------------------------- */

/*
 * Try to fold an expression to a constant.  Uses PG's eval_const_expressions
 * to handle all immutable coercions (RelabelType, CoerceViaIO, FuncExpr with
 * Const args, etc.).  Returns false if the expression contains anything
 * non-constant (Param, Var, volatile function, SetToDefault, subquery).
 */
static bool TryEvalConstExpr(Node *expr, Const **const_out) {
  Node *folded = eval_const_expressions(NULL, expr);
  if (!IsA(folded, Const)) {
    return false;
  }
  *const_out = (Const *)folded;
  return true;
}

static bool TryDetectValuesInsertPattern(Query *parse, ValuesInsertContext *context_out) {
  InsertPreconditionResult precond = {};
  if (!CheckInsertPreconditions(parse, &precond)) {
    return false;
  }

  Relation target_rel = precond.target_rel;

  /* VALUES-specific bail-outs */
  if (parse->returningList != NIL || parse->onConflict != NULL || parse->cteList != NIL) {
    relation_close(target_rel, AccessShareLock);
    return false;
  }

  /* Find the VALUES source.  PG 18 creates an RTE_VALUES entry for
   * multi-row VALUES but inlines single-row VALUES directly into the
   * targetList.  Handle both cases. */
  RangeTblEntry *values_rte = NULL;
  ListCell *rtlc;
  foreach (rtlc, parse->rtable) {
    RangeTblEntry *rte = (RangeTblEntry *)lfirst(rtlc);
    if (rte->rtekind == RTE_VALUES) {
      values_rte = rte;
      break;
    }
  }

  List *values_lists = NIL;

  if (values_rte) {
    /* Multi-row VALUES: expressions in values_rte->values_lists */
    values_lists = values_rte->values_lists;
  } else {
    /* Single-row VALUES: expressions are in parse->targetList directly.
     * Build a synthetic single-row values_lists from targetList exprs. */
    if (!parse->targetList) {
      relation_close(target_rel, AccessShareLock);
      return false;
    }

    List *row = NIL;
    ListCell *tlc;
    foreach (tlc, parse->targetList) {
      TargetEntry *tle = (TargetEntry *)lfirst(tlc);
      if (tle->resjunk) {
        continue;
      }
      row = lappend(row, tle->expr);
    }
    values_lists = list_make1(row);
  }
  int num_rows = list_length(values_lists);
  if (num_rows == 0) {
    relation_close(target_rel, AccessShareLock);
    return false;
  }

  TupleDesc tupdesc = RelationGetDescr(target_rel);
  int num_table_cols = tupdesc->natts;

  /* Build column map from targetList: col_map[table_attr] = values_list index, or -1 */
  int *col_map = (int *)palloc(sizeof(int) * num_table_cols);
  memset(col_map, -1, sizeof(int) * num_table_cols);

  int val_col = 0;
  ListCell *lc;
  foreach (lc, parse->targetList) {
    TargetEntry *tle = (TargetEntry *)lfirst(lc);
    if (tle->resjunk) {
      continue;
    }
    if (tle->resno < 1 || tle->resno > num_table_cols) {
      relation_close(target_rel, AccessShareLock);
      pfree(col_map);
      return false;
    }
    col_map[tle->resno - 1] = val_col++;
  }

  /* Evaluate all expressions to Const nodes */
  Const **consts = (Const **)palloc(sizeof(Const *) * num_rows * num_table_cols);

  int row_idx = 0;
  foreach (lc, values_lists) {
    List *row_exprs = (List *)lfirst(lc);

    for (int col = 0; col < num_table_cols; col++) {
      int flat = row_idx * num_table_cols + col;
      int mapped = col_map[col];
      if (mapped < 0) {
        /* Unspecified column: create a typed NULL Const */
        Form_pg_attribute attr = TupleDescAttr(tupdesc, col);
        consts[flat] = makeConst(attr->atttypid, attr->atttypmod, attr->attcollation, attr->attlen, (Datum)0,
                                 true /* isnull */, attr->attbyval);
      } else {
        Node *expr = (Node *)list_nth(row_exprs, mapped);
        Const *c;
        if (!TryEvalConstExpr(expr, &c)) {
          relation_close(target_rel, AccessShareLock);
          pfree(col_map);
          pfree(consts);
          return false;
        }
        consts[flat] = c;
      }
    }
    row_idx++;
  }

  pfree(col_map);

  /* Collect user-facing column types + names from TupleDesc */
  List *src_col_types = NIL;
  List *target_col_names = NIL;
  for (int i = 0; i < num_table_cols; i++) {
    Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
    src_col_types = lappend_oid(src_col_types, attr->atttypid);
    target_col_names = lappend(target_col_names, pstrdup(NameStr(attr->attname)));
  }

  relation_close(target_rel, AccessShareLock);

  List *inlined_col_types = NIL;
  if (!GetCachedInlinedColumnTypes(precond.table_id, precond.schema_version, src_col_types, &inlined_col_types)) {
    pfree(consts);
    return false;
  }

  context_out->target_table_oid = precond.target_oid;
  context_out->table_id = precond.table_id;
  context_out->schema_version = precond.schema_version;
  context_out->num_rows = num_rows;
  context_out->num_cols = num_table_cols;
  context_out->target_col_names = target_col_names;
  context_out->inlined_col_types = inlined_col_types;
  context_out->src_col_types = src_col_types;
  context_out->consts = consts;

  return true;
}

/* ----------------------------------------------------------------
 * Plan creation
 * ---------------------------------------------------------------- */

/*
 * Build a PlannedStmt shell with a CustomScan node.  Shared by both
 * UNNEST and VALUES paths.
 */
static PlannedStmt *MakeDirectInsertPlannedStmt(Query *parse, List *custom_private) {
  PlannedStmt *pstmt = makeNode(PlannedStmt);
  pstmt->commandType = CMD_INSERT;
  pstmt->hasReturning = false;
  pstmt->hasModifyingCTE = false;
  pstmt->canSetTag = true;
  pstmt->transientPlan = false;
  pstmt->dependsOnRole = false;
  pstmt->parallelModeNeeded = false;
  pstmt->resultRelations = list_make1_int(parse->resultRelation);
  pstmt->rtable = parse->rtable;
#if PG_VERSION_NUM >= 160000
  pstmt->permInfos = parse->rteperminfos;
#endif

  CustomScan *cscan = makeNode(CustomScan);
  cscan->scan.plan.targetlist = NIL;
  cscan->scan.plan.qual = NIL;
  cscan->scan.plan.lefttree = NULL;
  cscan->scan.plan.righttree = NULL;
  cscan->flags = 0;
  cscan->methods = &direct_insert_scan_methods;
  cscan->custom_private = custom_private;

  pstmt->planTree = (Plan *)cscan;
  return pstmt;
}

static PlannedStmt *CreateDirectInsertPlan(Query *parse, DirectInsertContext *context) {
  List *custom_private = NIL;
  /* Mode flag */
  custom_private = lappend(custom_private, makeInteger(DIRECT_INSERT_UNNEST));
  custom_private = lappend(custom_private, makeInteger((int)context->target_table_oid));
  custom_private = lappend(custom_private, makeInteger((int)(context->table_id & 0xFFFFFFFF)));
  custom_private = lappend(custom_private, makeInteger((int)((context->table_id >> 32) & 0xFFFFFFFF)));
  custom_private = lappend(custom_private, makeInteger((int)(context->schema_version & 0xFFFFFFFF)));
  custom_private = lappend(custom_private, makeInteger((int)((context->schema_version >> 32) & 0xFFFFFFFF)));
  custom_private = lappend(custom_private, makeInteger(context->expected_row_count));

  // Encode param IDs
  custom_private = lappend(custom_private, makeInteger(list_length(context->param_infos)));
  ListCell *lc;
  foreach (lc, context->param_infos) {
    ParamInfo *pinfo = (ParamInfo *)lfirst(lc);
    custom_private = lappend(custom_private, makeInteger(pinfo->param_id));
  }

  // Encode column names (as makeString nodes)
  custom_private = lappend(custom_private, makeInteger(list_length(context->target_col_names)));
  foreach (lc, context->target_col_names) {
    char *colname = (char *)lfirst(lc);
    custom_private = lappend(custom_private, makeString(pstrdup(colname)));
  }

  // Encode column types (as integers)
  custom_private = lappend(custom_private, makeInteger(list_length(context->target_col_types)));
  foreach (lc, context->target_col_types) {
    Oid coltype = lfirst_oid(lc);
    custom_private = lappend(custom_private, makeInteger((int)coltype));
  }

  return MakeDirectInsertPlannedStmt(parse, custom_private);
}

static PlannedStmt *CreateValuesInsertPlan(Query *parse, ValuesInsertContext *context) {
  List *custom_private = NIL;
  /* Mode flag */
  custom_private = lappend(custom_private, makeInteger(DIRECT_INSERT_VALUES));
  custom_private = lappend(custom_private, makeInteger((int)context->target_table_oid));
  custom_private = lappend(custom_private, makeInteger((int)(context->table_id & 0xFFFFFFFF)));
  custom_private = lappend(custom_private, makeInteger((int)((context->table_id >> 32) & 0xFFFFFFFF)));
  custom_private = lappend(custom_private, makeInteger((int)(context->schema_version & 0xFFFFFFFF)));
  custom_private = lappend(custom_private, makeInteger((int)((context->schema_version >> 32) & 0xFFFFFFFF)));
  custom_private = lappend(custom_private, makeInteger(context->num_rows));
  custom_private = lappend(custom_private, makeInteger(context->num_cols));

  /* Column names */
  custom_private = lappend(custom_private, makeInteger(list_length(context->target_col_names)));
  ListCell *lc;
  foreach (lc, context->target_col_names) {
    custom_private = lappend(custom_private, makeString(pstrdup((char *)lfirst(lc))));
  }

  /* Inlined column types */
  custom_private = lappend(custom_private, makeInteger(list_length(context->inlined_col_types)));
  foreach (lc, context->inlined_col_types) {
    custom_private = lappend(custom_private, makeInteger((int)lfirst_oid(lc)));
  }

  /* Source column types */
  custom_private = lappend(custom_private, makeInteger(list_length(context->src_col_types)));
  foreach (lc, context->src_col_types) {
    custom_private = lappend(custom_private, makeInteger((int)lfirst_oid(lc)));
  }

  /* Const nodes: num_rows * num_cols (serializable Node*) */
  int total = context->num_rows * context->num_cols;
  for (int i = 0; i < total; i++) {
    custom_private = lappend(custom_private, context->consts[i]);
  }

  return MakeDirectInsertPlannedStmt(parse, custom_private);
}

/* ----------------------------------------------------------------
 * CustomScan state creation / decode
 * ---------------------------------------------------------------- */

/* Helper: advance ListCell and return current node. */
static inline Node *NextPrivate(List *priv, ListCell **lc) {
  Node *n = (Node *)lfirst(*lc);
  *lc = lnext(priv, *lc);
  return n;
}

static Node *DirectInsert_CreateCustomScanState(CustomScan *cscan) {
  DirectInsertScanState *state = (DirectInsertScanState *)palloc0(sizeof(DirectInsertScanState));
  NodeSetTag(state, T_CustomScanState);
  state->css.methods = &direct_insert_exec_methods;

  List *priv = cscan->custom_private;
  ListCell *lc = list_head(priv);

  /* Mode flag */
  state->mode = (DirectInsertMode)intVal(NextPrivate(priv, &lc));

  /* Common fields */
  state->target_table_oid = (Oid)intVal(NextPrivate(priv, &lc));

  uint32_t tlo = (uint32_t)intVal(NextPrivate(priv, &lc));
  uint32_t thi = (uint32_t)intVal(NextPrivate(priv, &lc));
  state->table_id = ((uint64_t)thi << 32) | tlo;

  uint32_t slo = (uint32_t)intVal(NextPrivate(priv, &lc));
  uint32_t shi = (uint32_t)intVal(NextPrivate(priv, &lc));
  state->schema_version = ((uint64_t)shi << 32) | slo;

  if (state->mode == DIRECT_INSERT_UNNEST) {
    /* UNNEST decode */
    state->expected_row_count = intVal(NextPrivate(priv, &lc));

    int num_params = intVal(NextPrivate(priv, &lc));
    state->param_ids = NIL;
    for (int i = 0; i < num_params; i++) {
      state->param_ids = lappend_int(state->param_ids, intVal(NextPrivate(priv, &lc)));
    }

    int num_cols = intVal(NextPrivate(priv, &lc));
    state->column_names = NIL;
    for (int i = 0; i < num_cols; i++) {
      state->column_names = lappend(state->column_names, makeString(pstrdup(strVal(NextPrivate(priv, &lc)))));
    }

    int num_types = intVal(NextPrivate(priv, &lc));
    state->column_types = NIL;
    for (int i = 0; i < num_types; i++) {
      state->column_types = lappend_oid(state->column_types, (Oid)intVal(NextPrivate(priv, &lc)));
    }
  } else {
    /* VALUES decode */
    state->values_num_rows = intVal(NextPrivate(priv, &lc));
    state->values_num_cols = intVal(NextPrivate(priv, &lc));

    int num_names = intVal(NextPrivate(priv, &lc));
    state->column_names = NIL;
    for (int i = 0; i < num_names; i++) {
      state->column_names = lappend(state->column_names, makeString(pstrdup(strVal(NextPrivate(priv, &lc)))));
    }

    int num_inl_types = intVal(NextPrivate(priv, &lc));
    state->column_types = NIL;
    for (int i = 0; i < num_inl_types; i++) {
      state->column_types = lappend_oid(state->column_types, (Oid)intVal(NextPrivate(priv, &lc)));
    }

    int num_src_types = intVal(NextPrivate(priv, &lc));
    state->values_src_types = (Oid *)palloc(sizeof(Oid) * num_src_types);
    for (int i = 0; i < num_src_types; i++) {
      state->values_src_types[i] = (Oid)intVal(NextPrivate(priv, &lc));
    }

    /* Decode Const nodes -> flat Datum/null arrays */
    int total = state->values_num_rows * state->values_num_cols;
    state->values_data = (Datum *)palloc(sizeof(Datum) * total);
    state->values_nulls = (bool *)palloc(sizeof(bool) * total);
    for (int i = 0; i < total; i++) {
      Const *c = (Const *)NextPrivate(priv, &lc);
      state->values_data[i] = c->constvalue;
      state->values_nulls[i] = c->constisnull;
    }
  }

  state->finished = false;
  state->rows_inserted = 0;

  return (Node *)state;
}

/* ----------------------------------------------------------------
 * Executor callbacks
 * ---------------------------------------------------------------- */

static void DirectInsert_BeginCustomScan(CustomScanState *node, EState *estate, int eflags) {
  DirectInsertScanState *state = (DirectInsertScanState *)node;

  if (state->mode == DIRECT_INSERT_UNNEST) {
    state->bound_params = estate->es_param_list_info;
    if (!state->bound_params) {
      ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("direct insert: no bound parameters found")));
    }
  }

  StringInfoData buf;
  initStringInfo(&buf);
  appendStringInfo(&buf, "ducklake.ducklake_inlined_data_%llu_%llu", (unsigned long long)state->table_id,
                   (unsigned long long)state->schema_version);
  state->inlined_table_name = buf.data;

  state->begin_snapshot = pgducklake::GetNextSnapshotId();
  state->next_row_id = pgducklake::GetNextRowIdForTable(state->table_id, state->schema_version);

  ereport(DEBUG1, (errmsg("DuckLake direct insert: initialized scan state, table=%s, "
                          "predicted_snapshot=%llu, next_row_id=%lu",
                          state->inlined_table_name, (unsigned long long)state->begin_snapshot,
                          (unsigned long)state->next_row_id)));
}

static TupleTableSlot *DirectInsert_ExecCustomScan(CustomScanState *node) {
  DirectInsertScanState *state = (DirectInsertScanState *)node;

  if (state->finished) {
    return NULL;
  }

  if (state->mode == DIRECT_INSERT_UNNEST) {
    DirectInsertIntoInlinedTable(state);
  } else {
    DirectInsertValuesIntoInlinedTable(state);
  }

  state->finished = true;

  node->ss.ps.state->es_processed = state->rows_inserted;

  pgducklake::SkipSnapshotSyncGuard sync_guard;
  pgducklake::CreateSnapshotForDirectInsert(state->begin_snapshot, state->table_id, state->rows_inserted);

  CommandCounterIncrement();

  pgducklake::ResetDirectInsertCaches();

  return NULL;
}

static void DirectInsert_EndCustomScan(CustomScanState *node) {
  // Cleanup (if needed)
}

static void DirectInsert_ReScanCustomScan(CustomScanState *node) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("direct insert does not support rescan")));
}

static void DirectInsert_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es) {
  DirectInsertScanState *state = (DirectInsertScanState *)node;

  ExplainPropertyText("Custom Scan", "DuckLakeDirectInsert", es);
  const char *pattern = (state->mode == DIRECT_INSERT_UNNEST) ? "UNNEST" : "VALUES";
  ExplainPropertyText("Pattern", pattern, es);
  int nrows = (state->mode == DIRECT_INSERT_UNNEST) ? state->expected_row_count : state->values_num_rows;
  ExplainPropertyInteger("Expected Rows", NULL, nrows, es);

  if (es->verbose) {
    ExplainPropertyText("Inlined Table", state->inlined_table_name, es);
  }
}

/* ----------------------------------------------------------------
 * UNNEST execution (existing, unchanged)
 * ---------------------------------------------------------------- */

static void DirectInsertIntoInlinedTable(DirectInsertScanState *state) {
  int ret;

  // Connect to SPI
  if ((ret = SPI_connect()) < 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_connect failed: %d", ret)));
  }

  // Extract arrays from parameters
  int num_params = list_length(state->param_ids);
  ArrayType **arrays = (ArrayType **)palloc(sizeof(ArrayType *) * num_params);
  Oid *element_types = (Oid *)palloc(sizeof(Oid) * num_params);

  ListCell *lc;
  int param_idx = 0;
  foreach (lc, state->param_ids) {
    int param_id = lfirst_int(lc);
    ParamExternData *pdata = &state->bound_params->params[param_id - 1];

    if (pdata->isnull) {
      ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("parameter $%d is null", param_id)));
    }

    arrays[param_idx] = DatumGetArrayTypeP(pdata->value);
    element_types[param_idx] = ARR_ELEMTYPE(arrays[param_idx]);
    param_idx++;
  }

  // Validate array lengths (should already be validated, but double-check)
  int arr_length = ArrayGetNItems(ARR_NDIM(arrays[0]), ARR_DIMS(arrays[0]));
  for (int i = 1; i < num_params; i++) {
    int len = ArrayGetNItems(ARR_NDIM(arrays[i]), ARR_DIMS(arrays[i]));
    if (len != arr_length) {
      ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("array length mismatch")));
    }
  }

  // Build INSERT statement
  StringInfoData query;
  initStringInfo(&query);
  appendStringInfo(&query, "INSERT INTO %s (row_id, begin_snapshot, end_snapshot", state->inlined_table_name);

  param_idx = 0;
  foreach (lc, state->column_names) {
    Node *node = (Node *)lfirst(lc);
    char *colname = strVal(node);
    appendStringInfo(&query, ", %s", colname);
    param_idx++;
  }

  appendStringInfo(&query, ") VALUES ($1, $2, NULL");
  for (int i = 0; i < num_params; i++) {
    appendStringInfo(&query, ", $%d", i + 3);
  }
  appendStringInfo(&query, ")");

  // Collect the inlined table column types from the plan.  These may differ
  // from element_types when a DuckDB type is not natively supported in PG
  // (e.g. VARCHAR stored as BYTEA, DATE stored as VARCHAR).
  Oid *inlined_types = (Oid *)palloc(sizeof(Oid) * num_params);
  int idx = 0;
  foreach (lc, state->column_types) {
    inlined_types[idx++] = lfirst_oid(lc);
  }

  // Prepare parameter types -- use inlined table column types so SPI_prepare
  // matches the actual inlined table schema.
  Oid *param_types = (Oid *)palloc(sizeof(Oid) * (num_params + 2));
  param_types[0] = INT8OID; // row_id
  param_types[1] = INT8OID; // begin_snapshot
  for (int i = 0; i < num_params; i++) {
    param_types[i + 2] = inlined_types[i];
  }

  // Prepare statement
  SPIPlanPtr plan = SPI_prepare(query.data, num_params + 2, param_types);
  if (!plan) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_prepare failed")));
  }

  // Insert rows
  Datum *values = (Datum *)palloc(sizeof(Datum) * (num_params + 2));
  char *nulls = (char *)palloc(sizeof(char) * (num_params + 2));
  memset(nulls, ' ', num_params + 2); // ' ' means not null

  int16 *typlen = (int16 *)palloc(sizeof(int16) * num_params);
  bool *typbyval = (bool *)palloc(sizeof(bool) * num_params);
  char *typalign = (char *)palloc(sizeof(char) * num_params);
  Datum **elem_values = (Datum **)palloc(sizeof(Datum *) * num_params);
  bool **elem_nulls = (bool **)palloc(sizeof(bool *) * num_params);

  // Pre-compute per-column output function OIDs for non-native scalar types
  // (DATE, TIMESTAMP, UBIGINT, etc. stored as VARCHAR).  Avoids a syscache
  // lookup per row inside the hot loop.
  Oid *typoutput = (Oid *)palloc0(sizeof(Oid) * num_params);
  bool *needs_text_conv = (bool *)palloc0(sizeof(bool) * num_params);

  for (int i = 0; i < num_params; i++) {
    int nelems;
    get_typlenbyvalalign(element_types[i], &typlen[i], &typbyval[i], &typalign[i]);
    deconstruct_array(arrays[i], element_types[i], typlen[i], typbyval[i], typalign[i], &elem_values[i], &elem_nulls[i],
                      &nelems);

    if ((inlined_types[i] == TEXTOID || inlined_types[i] == VARCHAROID) && inlined_types[i] != element_types[i]) {
      bool typisvarlena;
      getTypeOutputInfo(element_types[i], &typoutput[i], &typisvarlena);
      needs_text_conv[i] = true;
    }
  }

  uint64_t current_row_id = state->next_row_id;

  for (int row = 0; row < arr_length; row++) {
    values[0] = Int64GetDatum(current_row_id++);
    values[1] = Int64GetDatum(state->begin_snapshot);

    for (int i = 0; i < num_params; i++) {
      if (elem_nulls[i][row]) {
        values[i + 2] = (Datum)0;
        nulls[i + 2] = 'n';
      } else if (needs_text_conv[i]) {
        // Scalar type (DATE, TIMESTAMP, UBIGINT, etc.) -> VARCHAR:
        // use PG output function to produce a DuckDB-parseable text string.
        char *str = OidOutputFunctionCall(typoutput[i], elem_values[i][row]);
        values[i + 2] = CStringGetTextDatum(str);
        nulls[i + 2] = ' ';
        pfree(str);
      } else {
        // Types match (native), BYTEA zero-copy, or unexpected -- pass as-is.
        values[i + 2] = elem_values[i][row];
        nulls[i + 2] = ' ';
      }
    }

    // Execute INSERT
    ret = SPI_execute_plan(plan, values, nulls, false, 0);
    if (ret != SPI_OK_INSERT) {
      ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_execute_plan failed: %d", ret)));
    }

    state->rows_inserted++;
  }

  SPI_finish();

  ereport(DEBUG1, (errmsg("DuckLake direct insert: successfully inserted %lld rows into %s",
                          (long long)state->rows_inserted, state->inlined_table_name)));
}

/* ----------------------------------------------------------------
 * VALUES execution via table_multi_insert
 * ---------------------------------------------------------------- */

/* Number of system columns prepended to the inlined data table:
 * row_id, begin_snapshot, end_snapshot. */
#define INLINED_SYSTEM_COLS 3

/* Max tuples to batch before flushing. */
#define MAX_BUFFERED_TUPLES 1000

/*
 * Per-column conversion info for VALUES -> inlined table type mapping.
 */
struct ValuesColumnConvInfo {
  bool needs_text_conv;
  FmgrInfo typoutput_finfo; /* cached output function */
};

static void DirectInsertValuesIntoInlinedTable(DirectInsertScanState *state) {
  int num_rows = state->values_num_rows;
  int num_cols = state->values_num_cols;

  /* Open inlined data table by name */
  char relname[NAMEDATALEN];
  snprintf(relname, sizeof(relname), "ducklake_inlined_data_%llu_%llu", (unsigned long long)state->table_id,
           (unsigned long long)state->schema_version);

  Oid ducklake_nsp = get_namespace_oid("ducklake", false);
  Oid relid = get_relname_relid(relname, ducklake_nsp);
  if (!OidIsValid(relid)) {
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("inlined data table \"%s\" does not exist", relname)));
  }

  Relation inlined_rel = table_open(relid, RowExclusiveLock);
  TupleDesc inlined_tupdesc = RelationGetDescr(inlined_rel);

  /* Build per-column conversion info */
  ValuesColumnConvInfo *conv = (ValuesColumnConvInfo *)palloc0(sizeof(ValuesColumnConvInfo) * num_cols);
  ListCell *inl_lc = list_head(state->column_types);

  for (int i = 0; i < num_cols; i++) {
    Oid src_type = state->values_src_types[i];
    Oid inl_type = lfirst_oid(inl_lc);
    inl_lc = lnext(state->column_types, inl_lc);

    if (src_type == inl_type) {
      conv[i].needs_text_conv = false;
    } else if (inl_type == VARCHAROID || inl_type == TEXTOID) {
      /* Scalar types (DATE, TIMESTAMP, etc.) stored as VARCHAR in
       * the inlined table: convert via PG output function. */
      Oid typoutput;
      bool typisvarlena;
      getTypeOutputInfo(src_type, &typoutput, &typisvarlena);
      fmgr_info(typoutput, &conv[i].typoutput_finfo);
      conv[i].needs_text_conv = true;
    } else if (inl_type == BYTEAOID) {
      /* DuckDB VARCHAR/BLOB columns use BYTEA in the inlined table.
       * PG text/varchar and bytea share the same varlena binary
       * layout (length header + payload bytes), so the Datum can
       * be stored as-is without conversion. */
      conv[i].needs_text_conv = false;
    } else {
      conv[i].needs_text_conv = false;
    }
  }

  /* Ensure DateStyle is ISO for temporal -> VARCHAR text conversion.
   * PG output functions for DATE, TIMESTAMP, etc. are DateStyle-dependent;
   * DuckDB always expects ISO format (YYYY-MM-DD, YYYY-MM-DD HH:MM:SS). */
  bool any_text_conv = false;
  for (int i = 0; i < num_cols; i++) {
    if (conv[i].needs_text_conv) {
      any_text_conv = true;
      break;
    }
  }
  int saved_date_style = DateStyle;
  int saved_date_order = DateOrder;
  if (any_text_conv) {
    DateStyle = USE_ISO_DATES;
    DateOrder = DATEORDER_YMD;
  }

  /* Allocate slots for batched insert */
  int batch_size = (num_rows < MAX_BUFFERED_TUPLES) ? num_rows : MAX_BUFFERED_TUPLES;
  TupleTableSlot **slots = (TupleTableSlot **)palloc(sizeof(TupleTableSlot *) * batch_size);
  for (int i = 0; i < batch_size; i++) {
    slots[i] = MakeSingleTupleTableSlot(inlined_tupdesc, &TTSOpsVirtual);
  }

  BulkInsertState bistate = GetBulkInsertState();
  CommandId cid = GetCurrentCommandId(true);

  int nslots = 0;
  uint64_t current_row_id = state->next_row_id;

  for (int row = 0; row < num_rows; row++) {
    TupleTableSlot *slot = slots[nslots];
    ExecClearTuple(slot);

    Datum *sv = slot->tts_values;
    bool *sn = slot->tts_isnull;

    /* System columns */
    sv[0] = Int64GetDatum((int64)current_row_id++);
    sn[0] = false;
    sv[1] = Int64GetDatum((int64)state->begin_snapshot);
    sn[1] = false;
    sv[2] = (Datum)0; /* end_snapshot = NULL */
    sn[2] = true;

    /* User columns */
    for (int col = 0; col < num_cols; col++) {
      int flat = row * num_cols + col;
      int dst = col + INLINED_SYSTEM_COLS;

      if (state->values_nulls[flat]) {
        sv[dst] = (Datum)0;
        sn[dst] = true;
      } else if (conv[col].needs_text_conv) {
        char *str = OutputFunctionCall(&conv[col].typoutput_finfo, state->values_data[flat]);
        sv[dst] = CStringGetTextDatum(str);
        sn[dst] = false;
        pfree(str);
      } else {
        sv[dst] = state->values_data[flat];
        sn[dst] = false;
      }
    }

    ExecStoreVirtualTuple(slot);
    nslots++;

    if (nslots >= batch_size) {
      table_multi_insert(inlined_rel, slots, nslots, cid, 0, bistate);
      for (int i = 0; i < nslots; i++) {
        ExecClearTuple(slots[i]);
      }
      nslots = 0;
    }
  }

  /* Flush remaining */
  if (nslots > 0) {
    table_multi_insert(inlined_rel, slots, nslots, cid, 0, bistate);
  }

  table_finish_bulk_insert(inlined_rel, 0);
  FreeBulkInsertState(bistate);

  if (any_text_conv) {
    DateStyle = saved_date_style;
    DateOrder = saved_date_order;
  }

  for (int i = 0; i < batch_size; i++) {
    ExecDropSingleTupleTableSlot(slots[i]);
  }
  pfree(slots);
  pfree(conv);

  table_close(inlined_rel, RowExclusiveLock);

  state->rows_inserted = num_rows;

  ereport(DEBUG1, (errmsg("DuckLake direct insert (VALUES): inserted %d rows into %s", num_rows, relname)));
}

} // namespace pgducklake
