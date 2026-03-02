/*
 * pgducklake_direct_insert.cpp
 *
 * Optimization for INSERT ... SELECT UNNEST($1), UNNEST($2), ... pattern.
 *
 * Bypasses DuckDB execution by detecting the pattern at planner time and
 * directly inserting array data into inlined data tables via SPI (zero-copy
 * with array Datums).
 *
 * Lifecycle:
 * 1. Planner hook detects pattern and creates custom scan plan
 * 2. Executor initializes state and allocates snapshot/row IDs
 * 3. Executor extracts array elements and inserts via SPI
 * 4. Executor returns completion (no tuples to output)
 */

#include "duckdb.hpp"

#include "pgducklake/pgducklake_direct_insert.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/tableam.h"
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

// Define the scan state structure here (needs full CustomScanState definition)
struct DirectInsertScanState {
  CustomScanState css; // Must be first

  Oid target_table_oid;
  uint64_t table_id;
  uint64_t schema_version;
  char *inlined_table_name;
  List *param_ids;    // List of int
  List *column_names; // List of char*
  List *column_types; // List of Oid
  int expected_row_count;

  ParamListInfo bound_params;
  bool finished;
  int64_t rows_inserted;

  uint64_t begin_snapshot;
  uint64_t next_row_id;
};

// Custom scan methods
static Node *DirectInsert_CreateCustomScanState(CustomScan *cscan);
static void DirectInsert_BeginCustomScan(CustomScanState *node, EState *estate,
                                         int eflags);
static TupleTableSlot *DirectInsert_ExecCustomScan(CustomScanState *node);
static void DirectInsert_EndCustomScan(CustomScanState *node);
static void DirectInsert_ReScanCustomScan(CustomScanState *node);
static void DirectInsert_ExplainCustomScan(CustomScanState *node,
                                           List *ancestors, ExplainState *es);

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

// Helper functions
static bool TryDetectDirectInsertPattern(Query *parse,
                                         ParamListInfo bound_params,
                                         DirectInsertContext *context_out);
static bool IsUnnestOfParam(Node *node, int *param_id_out, Oid *param_type_out);
static bool ValidateArrayLengths(ParamListInfo bound_params, List *param_ids,
                                 int *expected_row_count_out);
static PlannedStmt *CreateDirectInsertPlan(Query *parse,
                                           DirectInsertContext *context);
static void DirectInsertIntoInlinedTable(DirectInsertScanState *state);

void RegisterDirectInsertNode() {
  RegisterCustomScanMethods(&direct_insert_scan_methods);
}

PlannedStmt *TryCreateDirectInsertPlan(Query *parse,
                                       ParamListInfo bound_params) {
  DirectInsertContext context = {};
  if (TryDetectDirectInsertPattern(parse, bound_params, &context)) {
    ereport(DEBUG1, (errmsg("DuckLake direct insert: optimization enabled for "
                            "INSERT UNNEST pattern, "
                            "table_id=%lu, expected_rows=%d",
                            (unsigned long)context.table_id,
                            context.expected_row_count)));
    return CreateDirectInsertPlan(parse, &context);
  }
  return nullptr;
}

static bool TryDetectDirectInsertPattern(Query *parse,
                                         ParamListInfo bound_params,
                                         DirectInsertContext *context_out) {
  // Check 1: Must be INSERT command
  if (parse->commandType != CMD_INSERT) {
    return false;
  }

  // Check 2: Must be in autocommit mode (not inside an explicit transaction
  // block) This ensures we can safely flush metadata after direct insert
  if (IsTransactionBlock()) {
    return false;
  }

  // Check 3: Must have a single result relation
  if (parse->resultRelation == 0 ||
      list_length(parse->rtable) < parse->resultRelation) {
    return false;
  }

  RangeTblEntry *target_rte =
      (RangeTblEntry *)list_nth(parse->rtable, parse->resultRelation - 1);
  if (target_rte->rtekind != RTE_RELATION) {
    return false;
  }

  Oid target_oid = target_rte->relid;

  // Check 4: Target table must use ducklake access method
  static Oid ducklake_am_oid = InvalidOid;
  if (!OidIsValid(ducklake_am_oid))
    ducklake_am_oid = get_am_oid("ducklake", false);
  Relation target_rel = relation_open(target_oid, AccessShareLock);
  Oid am_oid = target_rel->rd_rel->relam;

  if (am_oid != ducklake_am_oid) {
    relation_close(target_rel, AccessShareLock);
    return false;
  }

  // Check 5: Must have data inlining enabled
  uint64_t table_id, schema_version;
  if (!pgducklake::GetTableInliningInfo(target_oid, &table_id,
                                        &schema_version)) {
    relation_close(target_rel, AccessShareLock);
    return false;
  }

  // Check 6: Must have SELECT query as source
  if (!parse->jointree || !parse->jointree->fromlist ||
      list_length(parse->jointree->fromlist) != 1) {
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
  RangeTblEntry *from_rte =
      (RangeTblEntry *)list_nth(parse->rtable, from_rtindex - 1);

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
    target_col_names =
        lappend(target_col_names, pstrdup(NameStr(attr->attname)));
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

  // All checks passed, fill context
  context_out->target_table_oid = target_oid;
  context_out->table_id = table_id;
  context_out->schema_version = schema_version;
  context_out->param_infos = param_infos;
  context_out->expected_row_count = expected_row_count;
  context_out->target_col_names = target_col_names;
  context_out->target_col_types = target_col_types;

  return true;
}

static bool IsUnnestOfParam(Node *node, int *param_id_out,
                            Oid *param_type_out) {
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

static bool ValidateArrayLengths(ParamListInfo bound_params, List *param_ids,
                                 int *expected_row_count_out) {
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

static PlannedStmt *CreateDirectInsertPlan(Query *parse,
                                           DirectInsertContext *context) {
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

  // Create custom scan node
  CustomScan *cscan = makeNode(CustomScan);
  cscan->scan.plan.targetlist = NIL;
  cscan->scan.plan.qual = NIL;
  cscan->scan.plan.lefttree = NULL;
  cscan->scan.plan.righttree = NULL;
  cscan->flags = 0;
  cscan->methods = &direct_insert_scan_methods;

  // Encode context into custom_private
  // Use makeInteger/makeString for all values to create a homogeneous pointer
  // list
  List *custom_private = NIL;
  custom_private =
      lappend(custom_private, makeInteger((int)context->target_table_oid));
  custom_private = lappend(custom_private,
                           makeInteger((int)(context->table_id & 0xFFFFFFFF)));
  custom_private =
      lappend(custom_private,
              makeInteger((int)((context->table_id >> 32) & 0xFFFFFFFF)));
  custom_private = lappend(
      custom_private, makeInteger((int)(context->schema_version & 0xFFFFFFFF)));
  custom_private =
      lappend(custom_private,
              makeInteger((int)((context->schema_version >> 32) & 0xFFFFFFFF)));
  custom_private =
      lappend(custom_private, makeInteger(context->expected_row_count));

  // Encode param IDs
  custom_private =
      lappend(custom_private, makeInteger(list_length(context->param_infos)));
  ListCell *lc;
  foreach (lc, context->param_infos) {
    ParamInfo *pinfo = (ParamInfo *)lfirst(lc);
    custom_private = lappend(custom_private, makeInteger(pinfo->param_id));
  }

  // Encode column names (as makeString nodes)
  custom_private = lappend(custom_private,
                           makeInteger(list_length(context->target_col_names)));
  foreach (lc, context->target_col_names) {
    char *colname = (char *)lfirst(lc);
    custom_private = lappend(custom_private, makeString(pstrdup(colname)));
  }

  // Encode column types (as integers)
  custom_private = lappend(custom_private,
                           makeInteger(list_length(context->target_col_types)));
  foreach (lc, context->target_col_types) {
    Oid coltype = lfirst_oid(lc);
    custom_private = lappend(custom_private, makeInteger((int)coltype));
  }

  cscan->custom_private = custom_private;

  pstmt->planTree = (Plan *)cscan;

  return pstmt;
}

static Node *DirectInsert_CreateCustomScanState(CustomScan *cscan) {
  DirectInsertScanState *state =
      (DirectInsertScanState *)palloc0(sizeof(DirectInsertScanState));
  NodeSetTag(state, T_CustomScanState);
  state->css.methods = &direct_insert_exec_methods;

  // Decode custom_private (all encoded as Integer/String nodes)
  List *custom_private = cscan->custom_private;
  ListCell *lc = list_head(custom_private);

  state->target_table_oid = (Oid)intVal(lfirst(lc));
  lc = lnext(custom_private, lc);

  uint32_t table_id_low = (uint32_t)intVal(lfirst(lc));
  lc = lnext(custom_private, lc);
  uint32_t table_id_high = (uint32_t)intVal(lfirst(lc));
  lc = lnext(custom_private, lc);
  state->table_id = ((uint64_t)table_id_high << 32) | table_id_low;

  uint32_t schema_version_low = (uint32_t)intVal(lfirst(lc));
  lc = lnext(custom_private, lc);
  uint32_t schema_version_high = (uint32_t)intVal(lfirst(lc));
  lc = lnext(custom_private, lc);
  state->schema_version =
      ((uint64_t)schema_version_high << 32) | schema_version_low;

  state->expected_row_count = intVal(lfirst(lc));
  lc = lnext(custom_private, lc);

  // Decode param IDs
  int num_params = intVal(lfirst(lc));
  lc = lnext(custom_private, lc);
  state->param_ids = NIL;
  for (int i = 0; i < num_params; i++) {
    state->param_ids = lappend_int(state->param_ids, intVal(lfirst(lc)));
    lc = lnext(custom_private, lc);
  }

  // Decode column names
  int num_cols = intVal(lfirst(lc));
  lc = lnext(custom_private, lc);
  state->column_names = NIL;
  for (int i = 0; i < num_cols; i++) {
    Node *node = (Node *)lfirst(lc);
    char *colname = strVal(node);
    state->column_names =
        lappend(state->column_names, makeString(pstrdup(colname)));
    lc = lnext(custom_private, lc);
  }

  // Decode column types
  int num_types = intVal(lfirst(lc));
  lc = lnext(custom_private, lc);
  state->column_types = NIL;
  for (int i = 0; i < num_types; i++) {
    state->column_types =
        lappend_oid(state->column_types, (Oid)intVal(lfirst(lc)));
    lc = lnext(custom_private, lc);
  }

  state->finished = false;
  state->rows_inserted = 0;

  return (Node *)state;
}

static void DirectInsert_BeginCustomScan(CustomScanState *node, EState *estate,
                                         int eflags) {
  DirectInsertScanState *state = (DirectInsertScanState *)node;

  state->bound_params = estate->es_param_list_info;
  if (!state->bound_params) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("direct insert: no bound parameters found")));
  }

  StringInfoData buf;
  initStringInfo(&buf);
  appendStringInfo(&buf, "ducklake.ducklake_inlined_data_%llu_%llu",
                   (unsigned long long)state->table_id,
                   (unsigned long long)state->schema_version);
  state->inlined_table_name = buf.data;

  // Query next snapshot ID from DuckLake metadata tables
  // This will be used as the begin_snapshot for the inserted rows
  state->begin_snapshot = pgducklake::GetNextSnapshotId();
  state->next_row_id =
      pgducklake::GetNextRowIdForTable(state->table_id, state->schema_version);

  ereport(DEBUG1,
          (errmsg("DuckLake direct insert: initialized scan state, table=%s, "
                  "predicted_snapshot=%llu, next_row_id=%lu",
                  state->inlined_table_name,
                  (unsigned long long)state->begin_snapshot,
                  (unsigned long)state->next_row_id)));
}

static TupleTableSlot *DirectInsert_ExecCustomScan(CustomScanState *node) {
  DirectInsertScanState *state = (DirectInsertScanState *)node;

  if (state->finished) {
    return NULL;
  }

  // Perform direct SPI insertion
  DirectInsertIntoInlinedTable(state);

  state->finished = true;

  // Create the snapshot record immediately while we still have an active
  // PostgreSQL snapshot. This makes the inserted rows visible to subsequent
  // DuckLake queries.
  pgducklake::CreateSnapshotForDirectInsert(state->begin_snapshot,
                                            state->schema_version);

  // Update command counter for RETURNING clause (if any)
  CommandCounterIncrement();

  return NULL;
}

static void DirectInsert_EndCustomScan(CustomScanState *node) {
  // Cleanup (if needed)
}

static void DirectInsert_ReScanCustomScan(CustomScanState *node) {
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("direct insert does not support rescan")));
}

static void DirectInsert_ExplainCustomScan(CustomScanState *node,
                                           List *ancestors, ExplainState *es) {
  DirectInsertScanState *state = (DirectInsertScanState *)node;

  ExplainPropertyText("Custom Scan", "DuckLakeDirectInsert", es);
  ExplainPropertyInteger("Expected Rows", NULL, state->expected_row_count, es);

  if (es->verbose) {
    ExplainPropertyText("Inlined Table", state->inlined_table_name, es);
  }
}

static void DirectInsertIntoInlinedTable(DirectInsertScanState *state) {
  int ret;

  // Connect to SPI
  if ((ret = SPI_connect()) < 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("SPI_connect failed: %d", ret)));
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
      ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                      errmsg("parameter $%d is null", param_id)));
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
      ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                      errmsg("array length mismatch")));
    }
  }

  // Build INSERT statement
  StringInfoData query;
  initStringInfo(&query);
  appendStringInfo(&query,
                   "INSERT INTO %s (row_id, begin_snapshot, end_snapshot",
                   state->inlined_table_name);

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

  // Prepare parameter types
  Oid *param_types = (Oid *)palloc(sizeof(Oid) * (num_params + 2));
  param_types[0] = INT8OID; // row_id
  param_types[1] = INT8OID; // begin_snapshot
  for (int i = 0; i < num_params; i++) {
    param_types[i + 2] = element_types[i];
  }

  // Prepare statement
  SPIPlanPtr plan = SPI_prepare(query.data, num_params + 2, param_types);
  if (!plan) {
    ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_prepare failed")));
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

  for (int i = 0; i < num_params; i++) {
    int nelems;
    get_typlenbyvalalign(element_types[i], &typlen[i], &typbyval[i],
                         &typalign[i]);
    deconstruct_array(arrays[i], element_types[i], typlen[i], typbyval[i],
                      typalign[i], &elem_values[i], &elem_nulls[i], &nelems);
  }

  uint64_t current_row_id = state->next_row_id;

  for (int row = 0; row < arr_length; row++) {
    values[0] = Int64GetDatum(current_row_id++);
    values[1] = Int64GetDatum(state->begin_snapshot);

    for (int i = 0; i < num_params; i++) {
      if (elem_nulls[i][row]) {
        values[i + 2] = (Datum)0;
        nulls[i + 2] = 'n';
      } else {
        values[i + 2] = elem_values[i][row];
        nulls[i + 2] = ' ';
      }
    }

    // Execute INSERT
    ret = SPI_execute_plan(plan, values, nulls, false, 0);
    if (ret != SPI_OK_INSERT) {
      ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                      errmsg("SPI_execute_plan failed: %d", ret)));
    }

    state->rows_inserted++;
  }

  SPI_finish();

  ereport(
      DEBUG1,
      (errmsg("DuckLake direct insert: successfully inserted %lld rows into %s",
              (long long)state->rows_inserted, state->inlined_table_name)));
}

} // namespace pgducklake
