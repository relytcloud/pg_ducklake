/*
 * pgducklake_table.cpp -- Table lifecycle: AM handler + DDL event triggers.
 *
 * @scope extension: ducklake table AM handler, DDL event triggers
 *   (ducklake_create_table_trigger, ducklake_drop_table_trigger,
 *   ducklake_alter_table_trigger), EnsureDuckLakeTable
 * @scope duckdb-instance: SyncNewTables, SyncDroppedTables (snapshot sync)
 *
 * Provides the PostgreSQL TableAM implementation for DuckLake tables and
 * the DDL event triggers that synchronize table CREATE/DROP/ALTER from
 * PostgreSQL to DuckDB.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_sync.hpp"

#include <duckdb/common/error_data.hpp> /* must precede postgres.h (FATAL macro) */

#include "pgducklake/utility/cpp_wrapper.hpp"
#include "pgduckdb/pgduckdb_contracts.hpp"

#include <string>
#include <vector>

#include <duckdb/common/string_util.hpp>
#include <duckdb/parser/keyword_helper.hpp>

#include "common/ducklake_types.hpp"

extern "C" {
#include "postgres.h"

#include "access/heapam.h"
#include "access/relation.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "nodes/value.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "pgduckdb/pgduckdb_ruleutils.h"

// Defined in pgducklake_vacuum.cpp
extern void ducklake_do_vacuum(Relation onerel, VacuumParams *params,
                               BufferAccessStrategy bstrategy);
}

/* ================================================================
 * Variant type OID cache (used by create/alter table triggers)
 * ================================================================ */

namespace pgducklake {

static Oid variant_type_oid = InvalidOid;

static Oid GetVariantTypeOid() {
  if (!OidIsValid(variant_type_oid)) {
    Oid nsp_oid = get_namespace_oid(PGDUCKLAKE_PG_SCHEMA, true);
    if (OidIsValid(nsp_oid)) {
      variant_type_oid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
                                         CStringGetDatum("variant"),
                                         ObjectIdGetDatum(nsp_oid));
    }
  }
  return variant_type_oid;
}

} // namespace pgducklake

/* ================================================================
 * Table Access Method callbacks
 * ================================================================ */

extern "C" {

#define NOT_IMPLEMENTED()                                                      \
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),                      \
                  errmsg("ducklake does not implement %s", __func__)))

PG_FUNCTION_INFO_V1(ducklake_am_handler);

/* ------------------------------------------------------------------------
 * Slot related callbacks for duckdb AM
 * ------------------------------------------------------------------------
 */

static const TupleTableSlotOps *ducklake_slot_callbacks(Relation /*relation*/) {
  /*
   * Here we would most likely want to invent your own set of slot
   * callbacks for our AM. For now we just use the minimal tuple slot, we
   * only implement this function to make sure ANALYZE does not fail.
   */
  return &TTSOpsMinimalTuple;
}

/* ------------------------------------------------------------------------
 * Table Scan Callbacks for duckdb AM
 * ------------------------------------------------------------------------
 */

typedef struct DuckdbScanDescData {
  TableScanDescData rs_base; /* AM independent part of the descriptor */

  /* Add more fields here as needed by the AM. */
} DuckdbScanDescData;
typedef struct DuckdbScanDescData *DuckdbScanDesc;

static TableScanDesc duckdb_scan_begin(Relation relation, Snapshot snapshot,
                                       int nkeys, ScanKey /*key*/,
                                       ParallelTableScanDesc parallel_scan,
                                       uint32 flags) {
  DuckdbScanDesc scan = (DuckdbScanDesc)palloc(sizeof(DuckdbScanDescData));

  scan->rs_base.rs_rd = relation;
  scan->rs_base.rs_snapshot = snapshot;
  scan->rs_base.rs_nkeys = nkeys;
  scan->rs_base.rs_flags = flags;
  scan->rs_base.rs_parallel = parallel_scan;

  return (TableScanDesc)scan;
}

static void duckdb_scan_end(TableScanDesc sscan) {
  DuckdbScanDesc scan = (DuckdbScanDesc)sscan;

  pfree(scan);
}

static void duckdb_scan_rescan(TableScanDesc /*sscan*/, ScanKey /*key*/,
                               bool /*set_params*/, bool /*allow_strat*/,
                               bool /*allow_sync*/, bool /*allow_pagemode*/) {
  NOT_IMPLEMENTED();
}

static bool duckdb_scan_getnextslot(TableScanDesc /*sscan*/,
                                    ScanDirection /*direction*/,
                                    TupleTableSlot *slot) {
  /* If we are executing ALTER TABLE we return empty tuple */
  if (pgduckdb::DuckdbIsAlterTableInProgress()) {
    ExecClearTuple(slot);
    return false;
  }
  NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for duckdb AM
 * ------------------------------------------------------------------------
 */

static IndexFetchTableData *duckdb_index_fetch_begin(Relation /*rel*/) {
  NOT_IMPLEMENTED();
}

static void duckdb_index_fetch_reset(IndexFetchTableData * /*scan*/) {
  NOT_IMPLEMENTED();
}

static void duckdb_index_fetch_end(IndexFetchTableData * /*scan*/) {
  NOT_IMPLEMENTED();
}

static bool duckdb_index_fetch_tuple(struct IndexFetchTableData * /*scan*/,
                                     ItemPointer /*tid*/, Snapshot /*snapshot*/,
                                     TupleTableSlot * /*slot*/,
                                     bool * /*call_again*/,
                                     bool * /*all_dead*/) {
  NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for
 * duckdb AM.
 * ------------------------------------------------------------------------
 */

static bool duckdb_fetch_row_version(Relation /*relation*/, ItemPointer /*tid*/,
                                     Snapshot /*snapshot*/,
                                     TupleTableSlot * /*slot*/) {
  NOT_IMPLEMENTED();
}

static void duckdb_get_latest_tid(TableScanDesc /*sscan*/,
                                  ItemPointer /*tid*/) {
  NOT_IMPLEMENTED();
}

static bool duckdb_tuple_tid_valid(TableScanDesc /*scan*/,
                                   ItemPointer /*tid*/) {
  NOT_IMPLEMENTED();
}

static bool duckdb_tuple_satisfies_snapshot(Relation /*rel*/,
                                            TupleTableSlot * /*slot*/,
                                            Snapshot /*snapshot*/) {
  NOT_IMPLEMENTED();
}

static TransactionId
duckdb_index_delete_tuples(Relation /*rel*/, TM_IndexDeleteOp * /*delstate*/) {
  NOT_IMPLEMENTED();
}

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for duckdb AM.
 * ----------------------------------------------------------------------------
 */

static void duckdb_tuple_insert(Relation /*relation*/,
                                TupleTableSlot * /*slot*/, CommandId /*cid*/,
                                int /*options*/, BulkInsertState /*bistate*/) {
  /* No-op: CTAS data population is handled by the DDL event trigger via DuckDB.
   * Normal INSERTs are routed through pg_duckdb's execution path, not the table
   * AM. */
}

static void duckdb_tuple_insert_speculative(Relation /*relation*/,
                                            TupleTableSlot * /*slot*/,
                                            CommandId /*cid*/, int /*options*/,
                                            BulkInsertState /*bistate*/,
                                            uint32 /*specToken*/) {
  NOT_IMPLEMENTED();
}

static void duckdb_tuple_complete_speculative(Relation /*relation*/,
                                              TupleTableSlot * /*slot*/,
                                              uint32 /*spekToken*/,
                                              bool /*succeeded*/) {
  NOT_IMPLEMENTED();
}

static void duckdb_multi_insert(Relation /*relation*/,
                                TupleTableSlot ** /*slots*/, int /*ntuples*/,
                                CommandId /*cid*/, int /*options*/,
                                BulkInsertState /*bistate*/) {
  /* No-op: see duckdb_tuple_insert */
}

static TM_Result duckdb_tuple_delete(Relation /*relation*/, ItemPointer /*tid*/,
                                     CommandId /*cid*/, Snapshot /*snapshot*/,
                                     Snapshot /*crosscheck*/, bool /*wait*/,
                                     TM_FailureData * /*tmfd*/,
                                     bool /*changingPart*/) {
  NOT_IMPLEMENTED();
}

#if PG_VERSION_NUM >= 160000

static TM_Result duckdb_tuple_update(
    Relation /*relation*/, ItemPointer /*otid*/, TupleTableSlot * /*slot*/,
    CommandId /*cid*/, Snapshot /*snapshot*/, Snapshot /*crosscheck*/,
    bool /*wait*/, TM_FailureData * /*tmfd*/, LockTupleMode * /*lockmode*/,
    TU_UpdateIndexes * /*update_indexes*/) {
  NOT_IMPLEMENTED();
}

#else

static TM_Result duckdb_tuple_update(Relation /*rel*/, ItemPointer /*otid*/,
                                     TupleTableSlot * /*slot*/,
                                     CommandId /*cid*/, Snapshot /*snapshot*/,
                                     Snapshot /*crosscheck*/, bool /*wait*/,
                                     TM_FailureData * /*tmfd*/,
                                     LockTupleMode * /*lockmode*/,
                                     bool * /*update_indexes*/) {
  NOT_IMPLEMENTED();
}

#endif

static TM_Result duckdb_tuple_lock(Relation /*relation*/, ItemPointer /*tid*/,
                                   Snapshot /*snapshot*/,
                                   TupleTableSlot * /*slot*/, CommandId /*cid*/,
                                   LockTupleMode /*mode*/,
                                   LockWaitPolicy /*wait_policy*/,
                                   uint8 /*flags*/, TM_FailureData * /*tmfd*/) {
  NOT_IMPLEMENTED();
}

static void duckdb_finish_bulk_insert(Relation /*relation*/, int /*options*/) {
  /* No-op */
}

/* ------------------------------------------------------------------------
 * DDL related callbacks for duckdb AM.
 * ------------------------------------------------------------------------
 */

#if PG_VERSION_NUM >= 160000

static void duckdb_relation_set_new_filelocator(
    Relation /*rel*/, const RelFileLocator * /*newrnode*/, char /*persistence*/,
    TransactionId * /*freezeXid*/, MultiXactId * /*minmulti*/) {
  /* nothing to do, the table will be created in DuckDB later by the
   * duckdb_create_table_trigger event trigger */
}

#else

static void duckdb_relation_set_new_filenode(Relation /*rel*/,
                                             const RelFileNode * /*newrnode*/,
                                             char /*persistence*/,
                                             TransactionId * /*freezeXid*/,
                                             MultiXactId * /*minmulti*/) {
  /* nothing to do, the table will be created in DuckDB later by the
   * duckdb_create_table_trigger event trigger */
}

#endif

static void duckdb_relation_nontransactional_truncate(Relation rel) {}

#if PG_VERSION_NUM >= 160000

static void duckdb_copy_data(Relation /*rel*/,
                             const RelFileLocator * /*newrnode*/) {
  NOT_IMPLEMENTED();
}

#else

static void duckdb_copy_data(Relation /*rel*/,
                             const RelFileNode * /*newrnode*/) {
  NOT_IMPLEMENTED();
}

#endif

static void duckdb_copy_for_cluster(
    Relation /*OldTable*/, Relation /*NewTable*/, Relation /*OldIndex*/,
    bool /*use_sort*/, TransactionId /*OldestXmin*/,
    TransactionId * /*xid_cutoff*/, MultiXactId * /*multi_cutoff*/,
    double * /*num_tuples*/, double * /*tups_vacuumed*/,
    double * /*tups_recently_dead*/) {
  NOT_IMPLEMENTED();
}

static void duckdb_vacuum(Relation onerel, VacuumParams *params,
                          BufferAccessStrategy bstrategy) {
  ducklake_do_vacuum(onerel, params, bstrategy);
}

#if PG_VERSION_NUM >= 170000

static bool duckdb_scan_analyze_next_block(TableScanDesc /*scan*/,
                                           ReadStream * /*stream*/) {
  /* no data in postgres, so no point to analyze next block */
  return false;
}

#else

static bool duckdb_scan_analyze_next_block(TableScanDesc /*scan*/,
                                           BlockNumber /*blockno*/,
                                           BufferAccessStrategy /*bstrategy*/) {
  /* no data in postgres, so no point to analyze next block */
  return false;
}
#endif

static bool duckdb_scan_analyze_next_tuple(TableScanDesc /*scan*/,
                                           TransactionId /*OldestXmin*/,
                                           double * /*liverows*/,
                                           double * /*deadrows*/,
                                           TupleTableSlot * /*slot*/) {
  NOT_IMPLEMENTED();
}

static double duckdb_index_build_range_scan(
    Relation /*tableRelation*/, Relation /*indexRelation*/,
    IndexInfo * /*indexInfo*/, bool /*allow_sync*/, bool /*anyvisible*/,
    bool /*progress*/, BlockNumber /*start_blockno*/, BlockNumber /*numblocks*/,
    IndexBuildCallback /*callback*/, void * /*callback_state*/,
    TableScanDesc /*scan*/) {
  if (pgduckdb::DuckdbIsAlterTableInProgress()) {
    return 0;
  }
  NOT_IMPLEMENTED();
}

static void duckdb_index_validate_scan(Relation /*tableRelation*/,
                                       Relation /*indexRelation*/,
                                       IndexInfo * /*indexInfo*/,
                                       Snapshot /*snapshot*/,
                                       ValidateIndexState * /*state*/) {
  NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the duckdb AM
 * ------------------------------------------------------------------------
 */

static uint64 duckdb_relation_size(Relation /*rel*/,
                                   ForkNumber /*forkNumber*/) {
  /*
   * For now we just return 0. We should probably want return something more
   * useful in the future though.
   */
  return 0;
}

/*
 * Check to see whether the table needs a TOAST table.
 */
static bool duckdb_relation_needs_toast_table(Relation /*rel*/) {

  /* we don't need toast, because everything is stored in duckdb */
  return false;
}

/* ------------------------------------------------------------------------
 * Planner related callbacks for the duckdb AM
 * ------------------------------------------------------------------------
 */

static void duckdb_estimate_rel_size(Relation /*rel*/, int32 *attr_widths,
                                     BlockNumber *pages, double *tuples,
                                     double *allvisfrac) {
  /* no data available */
  if (attr_widths)
    *attr_widths = 0;
  if (pages)
    *pages = 0;
  if (tuples)
    *tuples = 0;
  if (allvisfrac)
    *allvisfrac = 0;
}

/* ------------------------------------------------------------------------
 * Executor related callbacks for the duckdb AM
 * ------------------------------------------------------------------------
 */

#if PG_VERSION_NUM >= 180000

static bool duckdb_scan_bitmap_next_tuple(TableScanDesc /*scan*/,
                                          TupleTableSlot * /*slot*/,
                                          bool * /*recheck*/,
                                          uint64 * /*lossy_pages*/,
                                          uint64 * /*exact_pages*/) {
  NOT_IMPLEMENTED();
}

#else

static bool duckdb_scan_bitmap_next_block(TableScanDesc /*scan*/,
                                          TBMIterateResult * /*tbmres*/) {
  NOT_IMPLEMENTED();
}

static bool duckdb_scan_bitmap_next_tuple(TableScanDesc /*scan*/,
                                          TBMIterateResult * /*tbmres*/,
                                          TupleTableSlot * /*slot*/) {
  NOT_IMPLEMENTED();
}

#endif

static bool duckdb_scan_sample_next_block(TableScanDesc /*scan*/,
                                          SampleScanState * /*scanstate*/) {
  NOT_IMPLEMENTED();
}

static bool duckdb_scan_sample_next_tuple(TableScanDesc /*scan*/,
                                          SampleScanState * /*scanstate*/,
                                          TupleTableSlot * /*slot*/) {
  NOT_IMPLEMENTED();
}

/* ------------------------------------------------------------------------
 * Definition of the duckdb table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine ducklake_methods = {
    .type = T_TableAmRoutine,

    .slot_callbacks = ducklake_slot_callbacks,

    .scan_begin = duckdb_scan_begin,
    .scan_end = duckdb_scan_end,
    .scan_rescan = duckdb_scan_rescan,
    .scan_getnextslot = duckdb_scan_getnextslot,

    /* optional callbacks */
    .scan_set_tidrange = NULL,
    .scan_getnextslot_tidrange = NULL,

    /* these are common helper functions */
    .parallelscan_estimate = table_block_parallelscan_estimate,
    .parallelscan_initialize = table_block_parallelscan_initialize,
    .parallelscan_reinitialize = table_block_parallelscan_reinitialize,

    .index_fetch_begin = duckdb_index_fetch_begin,
    .index_fetch_reset = duckdb_index_fetch_reset,
    .index_fetch_end = duckdb_index_fetch_end,
    .index_fetch_tuple = duckdb_index_fetch_tuple,

    .tuple_fetch_row_version = duckdb_fetch_row_version,
    .tuple_tid_valid = duckdb_tuple_tid_valid,
    .tuple_get_latest_tid = duckdb_get_latest_tid,
    .tuple_satisfies_snapshot = duckdb_tuple_satisfies_snapshot,
    .index_delete_tuples = duckdb_index_delete_tuples,

    .tuple_insert = duckdb_tuple_insert,
    .tuple_insert_speculative = duckdb_tuple_insert_speculative,
    .tuple_complete_speculative = duckdb_tuple_complete_speculative,
    .multi_insert = duckdb_multi_insert,
    .tuple_delete = duckdb_tuple_delete,
    .tuple_update = duckdb_tuple_update,
    .tuple_lock = duckdb_tuple_lock,
    .finish_bulk_insert = duckdb_finish_bulk_insert,

#if PG_VERSION_NUM >= 160000
    .relation_set_new_filelocator = duckdb_relation_set_new_filelocator,
#else
    .relation_set_new_filenode = duckdb_relation_set_new_filenode,
#endif
    .relation_nontransactional_truncate =
        duckdb_relation_nontransactional_truncate,
    .relation_copy_data = duckdb_copy_data,
    .relation_copy_for_cluster = duckdb_copy_for_cluster,
    .relation_vacuum = duckdb_vacuum,
    .scan_analyze_next_block = duckdb_scan_analyze_next_block,
    .scan_analyze_next_tuple = duckdb_scan_analyze_next_tuple,
    .index_build_range_scan = duckdb_index_build_range_scan,
    .index_validate_scan = duckdb_index_validate_scan,

    .relation_size = duckdb_relation_size,
    .relation_needs_toast_table = duckdb_relation_needs_toast_table,
    /* can be null because relation_needs_toast_table returns false */
    .relation_toast_am = NULL,
    .relation_fetch_toast_slice = NULL,

    .relation_estimate_size = duckdb_estimate_rel_size,
#if PG_VERSION_NUM < 180000
    .scan_bitmap_next_block = duckdb_scan_bitmap_next_block,
#endif
    .scan_bitmap_next_tuple = duckdb_scan_bitmap_next_tuple,
    .scan_sample_next_block = duckdb_scan_sample_next_block,
    .scan_sample_next_tuple = duckdb_scan_sample_next_tuple};

Datum ducklake_am_handler(FunctionCallInfo /*funcinfo*/) {
  RegisterDuckdbTableAm("pgducklake", &ducklake_methods);
  PG_RETURN_POINTER(&ducklake_methods);
}

/* ================================================================
 * DDL event triggers
 * ================================================================ */

DECLARE_PG_FUNCTION(ducklake_create_table_trigger) {
  if (pgducklake::syncing_from_metadata)
    PG_RETURN_NULL();

  if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
    elog(ERROR, "not fired by event trigger manager");

  EventTriggerData *trigger_data = (EventTriggerData *)fcinfo->context;
  Node *parsetree = trigger_data->parsetree;

  SPI_connect();

  auto save_nestlevel = NewGUCNestLevel();
  SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET,
                  PGC_S_SESSION);
  SetConfigOption("duckdb.force_execution", "false", PGC_USERSET,
                  PGC_S_SESSION);

  int ret = SPI_exec(R"(
		SELECT DISTINCT objid AS relid, pg_class.relpersistence = 't' AS is_temporary
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN pg_catalog.pg_class
		ON cmds.objid = pg_class.oid
		WHERE cmds.object_type = 'table'
		AND pg_class.relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake')
		)",
                     0);

  if (ret != SPI_OK_SELECT)
    elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

  /* if we selected a row it was a duckdb table */
  auto is_ducklake_table = SPI_processed > 0;
  if (!is_ducklake_table) {
    /* Reject variant columns on non-ducklake tables */
    Oid variant_oid = pgducklake::GetVariantTypeOid();
    if (OidIsValid(variant_oid)) {
      StringInfoData check_sql;
      initStringInfo(&check_sql);
      appendStringInfo(&check_sql,
          "SELECT 1 FROM pg_catalog.pg_event_trigger_ddl_commands() cmds "
          "JOIN pg_catalog.pg_attribute a ON cmds.objid = a.attrelid "
          "WHERE cmds.object_type = 'table' "
          "AND a.attnum > 0 AND NOT a.attisdropped "
          "AND a.atttypid = %u LIMIT 1",
          variant_oid);
      ret = SPI_exec(check_sql.data, 1);
      pfree(check_sql.data);
      if (ret == SPI_OK_SELECT && SPI_processed > 0)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("ducklake.variant type can only be used with "
                        "ducklake access method")));
    }
    AtEOXact_GUC(false, save_nestlevel);
    SPI_finish();
    PG_RETURN_NULL();
  }

  if (SPI_processed != 1) {
    elog(ERROR, "Expected single table to be created, but found %llu",
         static_cast<unsigned long long>(SPI_processed));
  }

  if (!IsA(parsetree, CreateStmt) && !IsA(parsetree, CreateTableAsStmt)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("Cannot create a DuckLake table this way, use "
                           "CREATE TABLE or CREATE TABLE AS")));
  }

  HeapTuple tuple = SPI_tuptable->vals[0];
  bool isnull;
  Datum relid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
  if (isnull) {
    elog(ERROR, "Expected relid to be returned, but found NULL");
  }

  Datum is_temporary_datum =
      SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &isnull);
  if (isnull) {
    elog(ERROR, "Expected temporary boolean to be returned, but found NULL");
  }

  Oid relid = DatumGetObjectId(relid_datum);
  bool is_temporary = DatumGetBool(is_temporary_datum);

  if (is_temporary) {
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("temporary tables are not supported with ducklake "
                           "access method")));
  }

  AtEOXact_GUC(false, save_nestlevel);
  SPI_finish();

  // Generate CREATE TABLE DDL for DuckDB
  std::string create_table_ddl(pgduckdb_get_tabledef(relid));
  elog(DEBUG1, "Creating DuckLake table: %s", create_table_ddl.c_str());

  // Execute CREATE TABLE in DuckDB via raw_query
  const char *error_msg = nullptr;
  int result =
      pgducklake::ExecuteDuckDBQuery(create_table_ddl.c_str(), &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to create DuckLake table: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  // Handle CREATE TABLE AS (CTAS) - populate data via DuckDB
  if (IsA(parsetree, CreateTableAsStmt) && !pgducklake::ctas_skip_data) {
    auto ctas_stmt = castNode(CreateTableAsStmt, parsetree);
    auto ctas_query = (Query *)ctas_stmt->query;
    const char *ctas_query_string = pgduckdb_get_querydef(ctas_query);
    std::string insert_string = std::string("INSERT INTO ") +
                                pgduckdb_relation_name(relid) + " " +
                                ctas_query_string;

    elog(DEBUG1, "CTAS data population: %s", insert_string.c_str());

    const char *insert_error_msg = nullptr;
    int insert_result = pgducklake::ExecuteDuckDBQuery(insert_string.c_str(),
                                                       &insert_error_msg);
    if (insert_result != 0) {
      ereport(ERROR,
              (errcode(ERRCODE_INTERNAL_ERROR),
               errmsg("failed to populate DuckLake table via CTAS: %s",
                      insert_error_msg ? insert_error_msg : "unknown error")));
    }
  }

  PG_RETURN_NULL();
}

DECLARE_PG_FUNCTION(ducklake_drop_table_trigger) {
  if (pgducklake::syncing_from_metadata)
    PG_RETURN_NULL();

  if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
    elog(ERROR, "not fired by event trigger manager");

  SPI_connect();

  auto save_nestlevel = NewGUCNestLevel();
  SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET,
                  PGC_S_SESSION);

  // Check if any tables were dropped
  int ret = SPI_exec(R"(
		SELECT 1
		FROM pg_catalog.pg_event_trigger_dropped_objects()
		WHERE object_type = 'table'
	)",
                     0);

  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));
  }

  if (SPI_processed == 0) {
    AtEOXact_GUC(false, save_nestlevel);
    SPI_finish();
    PG_RETURN_NULL();
  }

  /*
   * Drop inlined data/delete tables associated with the dropped DuckLake
   * tables.  These PostgreSQL tables live in the ducklake schema and block
   * DROP EXTENSION if left behind.
   */
  ret = SPI_exec(R"(
		SELECT idt.table_name, del.relname
		FROM pg_catalog.pg_event_trigger_dropped_objects() cmds
		JOIN ducklake.ducklake_table AS tbl
		  ON cmds.object_name = tbl.table_name
		JOIN ducklake.ducklake_schema AS schema
		  ON cmds.schema_name = schema.schema_name
		  AND tbl.schema_id = schema.schema_id
		LEFT JOIN ducklake.ducklake_inlined_data_tables idt
		  ON idt.table_id = tbl.table_id
		LEFT JOIN pg_catalog.pg_class del
		  ON del.relname = 'ducklake_inlined_delete_' || tbl.table_id
		  AND del.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace
		                          WHERE nspname = 'ducklake')
		WHERE cmds.object_type = 'table'
		  AND tbl.end_snapshot IS NULL
		  AND schema.end_snapshot IS NULL
		)",
                 0);

  if (ret == SPI_OK_SELECT && SPI_processed > 0) {
    std::string drop_sql;
    for (uint64_t proc = 0; proc < SPI_processed; ++proc) {
      HeapTuple tuple = SPI_tuptable->vals[proc];
      char *inlined_data_table_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
      char *inlined_delete_table_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2);
      if (inlined_data_table_name) {
        drop_sql += duckdb::StringUtil::Format(
            "DROP TABLE ducklake.%s;",
            duckdb::SQLIdentifier(inlined_data_table_name));
      }
      if (inlined_delete_table_name) {
        drop_sql += duckdb::StringUtil::Format(
            "DROP TABLE ducklake.%s;",
            duckdb::SQLIdentifier(inlined_delete_table_name));
      }
    }
    if (!drop_sql.empty()) {
      SPI_exec(drop_sql.c_str(), 0);
    }
  }

  /*
   * Query DuckLake metadata to find tables that need to be dropped.
   * We can't use pg_class here since the tables are already dropped.
   */
  ret = SPI_exec(R"(
		SELECT cmds.schema_name, cmds.object_name
		FROM pg_catalog.pg_event_trigger_dropped_objects() cmds
		JOIN ducklake.ducklake_table AS tbl
		  ON cmds.object_name = tbl.table_name
		JOIN ducklake.ducklake_schema AS schema
		  ON cmds.schema_name = schema.schema_name
		  AND tbl.schema_id = schema.schema_id
		WHERE cmds.object_type = 'table'
		  AND tbl.end_snapshot IS NULL
		  AND schema.end_snapshot IS NULL
		)",
                 0);

  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));
  }

  // Drop corresponding DuckDB tables
  for (uint64_t proc = 0; proc < SPI_processed; ++proc) {
    HeapTuple tuple = SPI_tuptable->vals[proc];

    char *schema_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
    char *table_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2);

    std::string drop_ddl = duckdb::StringUtil::Format(
        "DROP TABLE IF EXISTS " PGDUCKLAKE_DUCKDB_CATALOG ".%s.%s", schema_name,
        table_name);

    elog(DEBUG1, "Dropping DuckLake table: %s", drop_ddl.c_str());

    const char *error_msg = nullptr;
    int result = pgducklake::ExecuteDuckDBQuery(drop_ddl.c_str(), &error_msg);
    if (result != 0) {
      // Log warning but don't fail - table might already be gone
      elog(WARNING, "failed to drop DuckLake table %s.%s: %s", schema_name,
           table_name, error_msg ? error_msg : "unknown error");
    }
  }

  AtEOXact_GUC(false, save_nestlevel);
  SPI_finish();

  PG_RETURN_NULL();
}

void EnsureDuckLakeTable(Oid relid) {
  static Oid ducklake_am_oid = InvalidOid;
  if (!OidIsValid(ducklake_am_oid))
    ducklake_am_oid = get_am_oid("ducklake", false);

  Relation rel = relation_open(relid, AccessShareLock);
  Oid am_oid = rel->rd_rel->relam;
  relation_close(rel, AccessShareLock);

  if (am_oid != ducklake_am_oid)
    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("table \"%s\" is not a DuckLake table",
                           get_rel_name(relid))));
}

DECLARE_PG_FUNCTION(ducklake_alter_table_trigger) {
  if (pgducklake::syncing_from_metadata)
    PG_RETURN_NULL();

  if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
    elog(ERROR, "not fired by event trigger manager");

  EventTriggerData *trigger_data = (EventTriggerData *)fcinfo->context;
  Node *parsetree = trigger_data->parsetree;

  SPI_connect();

  auto save_nestlevel = NewGUCNestLevel();
  SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET,
                  PGC_S_SESSION);
  SetConfigOption("duckdb.force_execution", "false", PGC_USERSET,
                  PGC_S_SESSION);

  int ret = SPI_exec(R"(
		SELECT DISTINCT objid AS relid
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN pg_catalog.pg_class
		ON cmds.objid = pg_class.oid
		WHERE cmds.object_type IN ('table', 'table column')
		AND pg_class.relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake')
		)",
                     0);

  if (ret != SPI_OK_SELECT)
    elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

  if (SPI_processed == 0) {
    /* Reject variant columns added to non-ducklake tables */
    Oid variant_oid = pgducklake::GetVariantTypeOid();
    if (OidIsValid(variant_oid)) {
      StringInfoData check_sql;
      initStringInfo(&check_sql);
      appendStringInfo(&check_sql,
          "SELECT 1 FROM pg_catalog.pg_event_trigger_ddl_commands() cmds "
          "JOIN pg_catalog.pg_attribute a ON cmds.objid = a.attrelid "
          "WHERE cmds.object_type IN ('table', 'table column') "
          "AND a.attnum > 0 AND NOT a.attisdropped "
          "AND a.atttypid = %u LIMIT 1",
          variant_oid);
      ret = SPI_exec(check_sql.data, 1);
      pfree(check_sql.data);
      if (ret == SPI_OK_SELECT && SPI_processed > 0)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("ducklake.variant type can only be used with "
                        "ducklake access method")));
    }
    AtEOXact_GUC(false, save_nestlevel);
    SPI_finish();
    PG_RETURN_NULL();
  }

  HeapTuple tuple = SPI_tuptable->vals[0];
  bool isnull;
  Datum relid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
  if (isnull)
    elog(ERROR, "Expected relid to be returned, but found NULL");

  Oid relid = DatumGetObjectId(relid_datum);

  AtEOXact_GUC(false, save_nestlevel);
  SPI_finish();

  /* Generate DDL using pg_duckdb's ruleutils functions */
  char *ddl_str;
  if (IsA(parsetree, RenameStmt)) {
    ddl_str = pgduckdb_get_rename_relationdef(relid, (RenameStmt *)parsetree);
  } else if (IsA(parsetree, AlterTableStmt)) {
    ddl_str = pgduckdb_get_alter_tabledef(relid, (AlterTableStmt *)parsetree);
  } else {
    elog(ERROR, "Unexpected parsetree type in ALTER TABLE trigger: %d",
         nodeTag(parsetree));
  }

  elog(DEBUG1, "ALTER TABLE DDL for DuckLake: %s", ddl_str);

  const char *error_msg = nullptr;
  int result = pgducklake::ExecuteDuckDBQuery(ddl_str, &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to alter DuckLake table: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  PG_RETURN_NULL();
}

} // extern "C"

/* ================================================================
 * Snapshot sync: table create/drop
 * ================================================================ */

namespace pgducklake {

namespace {

/*
 * Map DuckLake type strings to PostgreSQL type strings.
 * Reuses DuckLakeTypes::FromString() to parse the type, then maps the
 * resulting LogicalTypeId to a PG type OID and formats it canonically.
 * Falls back to "text" for unrecognized or complex types.
 */
std::string DuckLakeTypeToPgType(const char *dl_type) {
  try {
    auto logical_type = duckdb::DuckLakeTypes::FromString(dl_type);
    Oid pg_type = InvalidOid;
    int32_t typemod = -1;

    switch (logical_type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      pg_type = BOOLOID;
      break;
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::UTINYINT:
      pg_type = INT2OID;
      break;
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::USMALLINT:
      pg_type = INT4OID;
      break;
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::UINTEGER:
      pg_type = INT8OID;
      break;
    case duckdb::LogicalTypeId::HUGEINT:
    case duckdb::LogicalTypeId::UBIGINT:
    case duckdb::LogicalTypeId::UHUGEINT:
      pg_type = NUMERICOID;
      break;
    case duckdb::LogicalTypeId::FLOAT:
      pg_type = FLOAT4OID;
      break;
    case duckdb::LogicalTypeId::DOUBLE:
      pg_type = FLOAT8OID;
      break;
    case duckdb::LogicalTypeId::DECIMAL: {
      pg_type = NUMERICOID;
      uint8_t width, scale;
      logical_type.GetDecimalProperties(width, scale);
      typemod = ((width << 16) | (scale & 0x7ff)) + VARHDRSZ;
      break;
    }
    case duckdb::LogicalTypeId::VARCHAR:
      pg_type = TEXTOID;
      break;
    case duckdb::LogicalTypeId::BLOB:
      pg_type = BYTEAOID;
      break;
    case duckdb::LogicalTypeId::UUID:
      pg_type = UUIDOID;
      break;
    case duckdb::LogicalTypeId::DATE:
      pg_type = DATEOID;
      break;
    case duckdb::LogicalTypeId::TIME:
      pg_type = TIMEOID;
      break;
    case duckdb::LogicalTypeId::TIME_TZ:
      pg_type = TIMETZOID;
      break;
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_MS:
    case duckdb::LogicalTypeId::TIMESTAMP_NS:
    case duckdb::LogicalTypeId::TIMESTAMP_SEC:
      pg_type = TIMESTAMPOID;
      break;
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      pg_type = TIMESTAMPTZOID;
      break;
    case duckdb::LogicalTypeId::INTERVAL:
      pg_type = INTERVALOID;
      break;
    case duckdb::LogicalTypeId::VARIANT:
      return "ducklake.variant";
    default:
      return "text";
    }

    return format_type_with_typemod(pg_type, typemod);
  } catch (...) {
    return "text";
  }
}

} // anonymous namespace

/*
 * Sync newly created tables from DuckLake metadata into pg_class.
 * Queries ducklake_table/ducklake_column for tables with begin_snapshot = sid
 * and emits CREATE TABLE ... USING ducklake for each one not already present.
 * Caller must have an active SPI connection.
 */
void SyncNewTables(const char *sid) {
  std::string query = duckdb::StringUtil::Format(R"(
		SELECT s.schema_name, t.table_name,
		       c.column_name, c.column_type, c.nulls_allowed
		FROM ducklake.ducklake_table t
		JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
		LEFT JOIN ducklake.ducklake_column c ON t.table_id = c.table_id
		  AND c.parent_column IS NULL
		  AND c.end_snapshot IS NULL
		WHERE t.begin_snapshot = %s
		  AND s.end_snapshot IS NULL
		ORDER BY t.table_id, c.column_order
		)",
                                                 sid);

  int ret = SPI_exec(query.c_str(), 0);
  if (ret != SPI_OK_SELECT)
    elog(ERROR, "SPI_exec failed: %s", SPI_result_code_string(ret));

  /* Save results -- SPI_exec invalidates previous tuptable */
  struct ColInfo {
    std::string schema_name, table_name, col_name, col_type;
    bool not_null, has_col;
  };
  std::vector<ColInfo> cols;
  for (uint64_t i = 0; i < SPI_processed; ++i) {
    HeapTuple tup = SPI_tuptable->vals[i];
    TupleDesc td = SPI_tuptable->tupdesc;
    ColInfo ci;
    char *v;
    v = SPI_getvalue(tup, td, 1);
    ci.schema_name = v ? v : "";
    v = SPI_getvalue(tup, td, 2);
    ci.table_name = v ? v : "";
    v = SPI_getvalue(tup, td, 3);
    ci.has_col = (v != nullptr);
    ci.col_name = v ? v : "";
    v = SPI_getvalue(tup, td, 4);
    ci.col_type = v ? v : "";
    v = SPI_getvalue(tup, td, 5);
    ci.not_null = (v && strcmp(v, "f") == 0);
    cols.push_back(std::move(ci));
  }

  /* Group by table and emit CREATE TABLE DDL */
  std::string prev_schema, prev_table, ddl;
  bool first_col = true;
  bool skip_table = false;

  auto emit_ddl = [&]() {
    if (!ddl.empty() && !skip_table) {
      ddl += ") USING ducklake";
      elog(DEBUG1, "Metadata sync: %s", ddl.c_str());
      ret = SPI_exec(ddl.c_str(), 0);
      if (ret != SPI_OK_UTILITY)
        elog(ERROR, "SPI_exec CREATE TABLE failed: %s",
             SPI_result_code_string(ret));
    }
    ddl.clear();
    skip_table = false;
  };

  for (auto &ci : cols) {
    if (ci.schema_name != prev_schema || ci.table_name != prev_table) {
      emit_ddl();

      /* Skip if table already exists in pg_class */
      Oid nsp_oid = get_namespace_oid(ci.schema_name.c_str(), true);
      if (OidIsValid(nsp_oid) &&
          OidIsValid(get_relname_relid(ci.table_name.c_str(), nsp_oid))) {
        skip_table = true;
        prev_schema = ci.schema_name;
        prev_table = ci.table_name;
        continue;
      }

      /* Create schema if it doesn't exist yet */
      if (ci.schema_name != "public") {
        std::string cs = "CREATE SCHEMA IF NOT EXISTS ";
        cs += quote_identifier(ci.schema_name.c_str());
        ret = SPI_exec(cs.c_str(), 0);
        if (ret != SPI_OK_UTILITY)
          elog(ERROR, "SPI_exec CREATE SCHEMA failed: %s",
               SPI_result_code_string(ret));
      }

      ddl = "CREATE TABLE ";
      ddl += quote_identifier(ci.schema_name.c_str());
      ddl += ".";
      ddl += quote_identifier(ci.table_name.c_str());
      ddl += " (";
      first_col = true;
      prev_schema = ci.schema_name;
      prev_table = ci.table_name;
    }

    if (skip_table || !ci.has_col)
      continue;

    if (!first_col)
      ddl += ", ";
    ddl += quote_identifier(ci.col_name.c_str());
    ddl += " ";
    ddl += DuckLakeTypeToPgType(ci.col_type.c_str());
    if (ci.not_null)
      ddl += " NOT NULL";
    first_col = false;
  }
  emit_ddl();
}

/*
 * Sync dropped tables from DuckLake metadata: drop pg_class entries for
 * tables whose end_snapshot = sid.
 * Caller must have an active SPI connection.
 */
void SyncDroppedTables(const char *sid) {
  std::string query = duckdb::StringUtil::Format(R"(
		SELECT s.schema_name, t.table_name
		FROM ducklake.ducklake_table t
		JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
		WHERE t.end_snapshot = %s
		)",
                                                 sid);

  int ret = SPI_exec(query.c_str(), 0);
  if (ret != SPI_OK_SELECT)
    elog(ERROR, "SPI_exec failed: %s", SPI_result_code_string(ret));

  struct DropInfo {
    std::string schema_name, table_name;
  };
  std::vector<DropInfo> drops;
  for (uint64_t i = 0; i < SPI_processed; ++i) {
    HeapTuple tup = SPI_tuptable->vals[i];
    TupleDesc td = SPI_tuptable->tupdesc;
    DropInfo di;
    char *v;
    v = SPI_getvalue(tup, td, 1);
    di.schema_name = v ? v : "";
    v = SPI_getvalue(tup, td, 2);
    di.table_name = v ? v : "";
    drops.push_back(std::move(di));
  }

  for (auto &di : drops) {
    Oid nsp_oid = get_namespace_oid(di.schema_name.c_str(), true);
    if (!OidIsValid(nsp_oid))
      continue;
    if (!OidIsValid(get_relname_relid(di.table_name.c_str(), nsp_oid)))
      continue;

    std::string drop_ddl = "DROP TABLE ";
    drop_ddl += quote_identifier(di.schema_name.c_str());
    drop_ddl += ".";
    drop_ddl += quote_identifier(di.table_name.c_str());
    elog(DEBUG1, "Metadata sync: %s", drop_ddl.c_str());
    ret = SPI_exec(drop_ddl.c_str(), 0);
    if (ret != SPI_OK_UTILITY)
      elog(ERROR, "SPI_exec DROP TABLE failed: %s",
           SPI_result_code_string(ret));
  }
}

} // namespace pgducklake
