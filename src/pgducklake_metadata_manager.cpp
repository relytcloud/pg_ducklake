/*
 * pgducklake_metadata_manager.cpp -- PostgreSQL-backed DuckLake metadata
 * manager.
 *
 * @scope duckdb-instance: per-transaction PgDuckLakeMetadataManager,
 *   SPI query execution, snapshot trigger setup
 *
 * Implements DuckLake metadata operations by translating DuckDB requests into
 * SQL against the ducklake_* metadata tables in PostgreSQL.
 */

#include "pgducklake/pgducklake_metadata_manager.hpp"
#include "pgducklake/pgducklake_sync.hpp"

// DuckDB headers first
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include <duckdb/common/string_util.hpp>

#include "common/ducklake_util.hpp"

// Our vendored type conversion utilities
#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_pg_types.hpp"

// PostgreSQL headers
extern "C" {
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "executor/spi.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
}

// Include after PostgreSQL headers (since these also include postgres.h)
#include "pgducklake/utility/cpp_wrapper.hpp"
#include "pgducklake/utility/unsafe_command_id_guard.hpp"
#include <cstring>

namespace pgducklake {
static duckdb::StatementType ConvertSPIResultToDuckStatementType(int result) {
  switch (result) {
  case SPI_OK_UTILITY:
    return duckdb::StatementType::EXECUTE_STATEMENT;
  case SPI_OK_SELECT:
  case SPI_OK_SELINTO:
    return duckdb::StatementType::SELECT_STATEMENT;
  case SPI_OK_INSERT:
  case SPI_OK_INSERT_RETURNING:
    return duckdb::StatementType::INSERT_STATEMENT;
  case SPI_OK_DELETE:
  case SPI_OK_DELETE_RETURNING:
    return duckdb::StatementType::DELETE_STATEMENT;
  case SPI_OK_UPDATE:
  case SPI_OK_UPDATE_RETURNING:
    return duckdb::StatementType::UPDATE_STATEMENT;
  default:
    // For now, we should not use other types query in SPI.
    return duckdb::StatementType::INVALID_STATEMENT;
  }
}

/* Deform SPI tuples into a DuckDB DataChunk using pre-allocated buffers.
 * Callers pass Datum/bool arrays sized to natts so we avoid per-chunk palloc. */
static void InsertSPITupleTableIntoChunk(duckdb::DataChunk &output, SPITupleTable *tuptable, idx_t start_idx,
                                         int num_tuples, Datum *values, bool *nulls) {
  D_ASSERT(tuptable);
  D_ASSERT(start_idx + num_tuples <= tuptable->numvals);

  if (num_tuples == 0) {
    return;
  }

  TupleDesc tupdesc = tuptable->tupdesc;
  int natts = tupdesc->natts;

  /* Cache per-column attribute metadata outside the row loop. */
  auto attlen = (int16 *)palloc(natts * sizeof(int16));
  auto atttypid = (Oid *)palloc(natts * sizeof(Oid));
  for (int col = 0; col < natts; col++) {
    auto attr = TupleDescAttr(tupdesc, col);
    attlen[col] = attr->attlen;
    atttypid[col] = attr->atttypid;
  }

  for (int row = 0; row < num_tuples; row++) {
    HeapTuple tuple = tuptable->vals[start_idx + row];
    heap_deform_tuple(tuple, tupdesc, values, nulls);

    for (int col = 0; col < natts; col++) {
      auto &result = output.data[col];

      if (nulls[col]) {
        auto &array_mask = duckdb::FlatVector::Validity(result);
        array_mask.SetInvalid(row);
      } else {
        Datum datum = values[col];

        if (attlen[col] == -1) {
          bool should_free = false;
          Datum detoasted_value = DetoastPostgresDatum(reinterpret_cast<varlena *>(datum), &should_free);
          ConvertPostgresToDuckValue(atttypid[col], detoasted_value, result, row);
          if (should_free) {
            pfree(DatumGetPointer(detoasted_value));
          }
        } else {
          ConvertPostgresToDuckValue(atttypid[col], datum, result, row);
        }
      }
    }
  }

  pfree(attlen);
  pfree(atttypid);
}

/*
 * RAII guard for pg_duckdb's GlobalProcessLock.  DuckLake metadata reads run
 * on a DuckDB worker thread that shares the PG backend with other DuckDB
 * threads.  We must hold this lock while calling any PG API (SPI, snapshots,
 * etc.) to prevent concurrent access from other DuckDB threads.
 */
class GlobalProcessLockGuard {
public:
  GlobalProcessLockGuard() {
    pgduckdb::DuckdbLockGlobalProcess();
  }
  ~GlobalProcessLockGuard() {
    pgduckdb::DuckdbUnlockGlobalProcess();
  }
  GlobalProcessLockGuard(const GlobalProcessLockGuard &) = delete;
  GlobalProcessLockGuard &operator=(const GlobalProcessLockGuard &) = delete;
};

/*
 * RAII guard that temporarily disables duckdb.force_execution.  SPI queries
 * from the metadata manager must be planned by PostgreSQL, not re-routed
 * through DuckDB's planner hook -- otherwise we deadlock on the ClientContext
 * mutex that the caller already holds.  We toggle the backing bool directly
 * to avoid SetConfigOption interactions with subtransaction GUC handling.
 */
class ForceExecutionGuard {
public:
  ForceExecutionGuard() : saved_(pgduckdb::DuckdbSetForceExecution(false)) {
  }
  ~ForceExecutionGuard() {
    pgduckdb::DuckdbSetForceExecution(saved_);
  }
  ForceExecutionGuard(const ForceExecutionGuard &) = delete;
  ForceExecutionGuard &operator=(const ForceExecutionGuard &) = delete;

private:
  bool saved_;
};

static duckdb::unique_ptr<duckdb::QueryResult> CreateSPIResult(const duckdb::string &query) {
  elog(DEBUG1, "Creating SPI result for query: %s", query.c_str());

  ForceExecutionGuard force_exec_guard;
  GlobalProcessLockGuard global_lock;
  PostgresScopedStackReset scoped_stack_reset;
  UnsafeCommandIdGuard command_id_guard;

  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());

  MemoryContext old_context = CurrentMemoryContext;
  duckdb::string error_message;
  bool had_error = false;
  int ret = -1;

  PG_TRY();
  {
    ret = SPI_execute(query.c_str(), false, 0);
  }
  PG_CATCH();
  {
    MemoryContextSwitchTo(old_context);
    ErrorData *edata = CopyErrorData();
    error_message = edata->message;
    FreeErrorData(edata);
    FlushErrorState();
    had_error = true;
  }
  PG_END_TRY();

  if (had_error) {
    PopActiveSnapshot();
    SPI_finish();
    duckdb::ErrorData error(duckdb::ExceptionType::IO, "SPI execution failed: " + error_message);
    return duckdb::make_uniq<duckdb::MaterializedQueryResult>(std::move(error));
  }

  if (ret < 0) {
    PopActiveSnapshot();
    SPI_finish();
    duckdb::ErrorData error(duckdb::ExceptionType::IO,
                            "SPI execution failed: " + duckdb::string(SPI_result_code_string(ret)));
    return duckdb::make_uniq<duckdb::MaterializedQueryResult>(std::move(error));
  }

  // Get the result table
  SPITupleTable *tuptable = SPI_tuptable;
  if (!tuptable) {
    PopActiveSnapshot();
    SPI_finish();

    // Return an empty result
    duckdb::vector<duckdb::string> names;
    duckdb::StatementProperties properties;
    duckdb::ClientProperties client_properties;

    // Create an empty ColumnDataCollection instead of passing nullptr
    auto &allocator = duckdb::Allocator::DefaultAllocator();
    auto empty_collection = duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator);

    return duckdb::make_uniq<duckdb::MaterializedQueryResult>(ConvertSPIResultToDuckStatementType(ret), properties,
                                                              names, std::move(empty_collection), client_properties);
  }

  TupleDesc tupdesc = tuptable->tupdesc;
  int num_columns = tupdesc->natts;
  uint64 num_rows = tuptable->numvals;

  // Convert column types and names
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::string> names;

  for (int i = 0; i < num_columns; i++) {
    Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

    D_ASSERT(!attr->attisdropped);

    // Get column name
    names.push_back(NameStr(attr->attname));

    // Convert Postgres type to DuckDB type
    types.push_back(ConvertPostgresToDuckColumnType(attr));
  }

  // Create a ColumnDataCollection to store the results
  duckdb::ClientProperties client_properties;
  auto &allocator = duckdb::Allocator::DefaultAllocator();
  auto collection_p = duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator, types);

  // Allocate deform buffers once for all chunks
  auto values = (Datum *)palloc(num_columns * sizeof(Datum));
  auto deform_nulls = (bool *)palloc(num_columns * sizeof(bool));

  // Convert SPI rows to DuckDB DataChunks and append them
  for (idx_t row_idx = 0; row_idx < num_rows; row_idx += STANDARD_VECTOR_SIZE) {
    idx_t chunk_size = duckdb::MinValue<int>(STANDARD_VECTOR_SIZE, num_rows - row_idx);
    auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
    chunk->Initialize(allocator, types, chunk_size);
    InsertSPITupleTableIntoChunk(*chunk, tuptable, row_idx, chunk_size, values, deform_nulls);

    chunk->SetCardinality(chunk_size);
    collection_p->Append(*chunk);
  }

  pfree(values);
  pfree(deform_nulls);

  PopActiveSnapshot();
  SPI_finish();

  // Create and return the MaterializedQueryResult
  duckdb::StatementProperties properties;
  return duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::StatementType::SELECT_STATEMENT, properties, names,
                                                            std::move(collection_p), client_properties);
}

/*
 * Substitute DuckLake catalog/schema placeholders with the PostgreSQL schema
 * constants. We avoid calling transaction.GetCatalog() here because during
 * DuckLake initialization (FinalizeLoad → InitializeDuckLake → Execute), the
 * AttachedDatabase is not yet reachable via the db_manager.
 */
static void SubstituteCatalogPlaceholders(duckdb::string &query) {
  query = duckdb::StringUtil::Replace(query, "{METADATA_CATALOG}", "\"" PGDUCKLAKE_PG_SCHEMA "\"");
  query = duckdb::StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}", "\"" PGDUCKLAKE_DUCKDB_CATALOG "\"");
  query = duckdb::StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}", "'" PGDUCKLAKE_DUCKDB_CATALOG "'");
  query = duckdb::StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}", "'" PGDUCKLAKE_PG_SCHEMA "'");
  query = duckdb::StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}", "\"" PGDUCKLAKE_PG_SCHEMA "\"");
}

/*
 * Execute a write query in a subtransaction and convert any PostgreSQL ERROR
 * into a duckdb::TransactionException. This allows DuckLake's FlushChanges()
 * retry loop to intercept duplicate-key / unique-constraint failures that
 * arise from concurrent commits, rather than having a PostgreSQL longjmp
 * bypass the C++ catch block and crash the backend.
 */
static duckdb::unique_ptr<duckdb::QueryResult> CreateSPIExecuteInSubtransaction(const duckdb::string &query) {
  elog(DEBUG1, "CreateSPIExecuteInSubtransaction: %s", query.c_str());

  ForceExecutionGuard force_exec_guard;
  GlobalProcessLockGuard global_lock;
  PostgresScopedStackReset scoped_stack_reset;
  UnsafeCommandIdGuard command_id_guard;

  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());

  MemoryContext old_context = CurrentMemoryContext;
  duckdb::string error_message;
  bool had_error = false;
  int ret = -1;

  pgduckdb::DuckdbAllowSubtransaction(true);
  BeginInternalSubTransaction(NULL);
  pgduckdb::DuckdbAllowSubtransaction(false);
  PG_TRY();
  {
    ret = SPI_execute(query.c_str(), false, 0);
  }
  PG_CATCH();
  {
    MemoryContextSwitchTo(old_context);
    ErrorData *edata = CopyErrorData();
    error_message = edata->message;
    FreeErrorData(edata);
    FlushErrorState();
    had_error = true;
    RollbackAndReleaseCurrentSubTransaction();
  }
  PG_END_TRY();

  if (!had_error) {
    if (ret < 0) {
      error_message = duckdb::string("SPI execute failed: ") + SPI_result_code_string(ret);
      had_error = true;
      RollbackAndReleaseCurrentSubTransaction();
    } else {
      ReleaseCurrentSubTransaction();
    }
  }

  PopActiveSnapshot();
  SPI_finish();

  if (had_error) {
    throw duckdb::TransactionException("%s", error_message.c_str());
  }

  duckdb::vector<duckdb::string> names;
  duckdb::StatementProperties properties;
  duckdb::ClientProperties client_properties;
  auto &allocator = duckdb::Allocator::DefaultAllocator();
  auto empty_collection = duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator);
  return duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::StatementType::EXECUTE_STATEMENT, properties, names,
                                                            std::move(empty_collection), client_properties);
}

PgDuckLakeMetadataManager::PgDuckLakeMetadataManager(duckdb::DuckLakeTransaction &transaction_)
    : duckdb::PostgresMetadataManager(transaction_) {
}

PgDuckLakeMetadataManager::~PgDuckLakeMetadataManager() {
}

/*
 * Replace {DATA_PATH} and {METADATA_PATH} from the DuckLakeCatalog when the
 * query actually contains them.  We guard with find() because GetCatalog()
 * is not safe during initialization (the AttachedDatabase is not yet
 * reachable), but these placeholders never appear in init queries.
 */
static void SubstitutePathPlaceholders(duckdb::string &query, duckdb::DuckLakeTransaction &transaction) {
  if (query.find("{DATA_PATH}") == duckdb::string::npos && query.find("{METADATA_PATH}") == duckdb::string::npos) {
    return;
  }
  auto &catalog = transaction.GetCatalog();
  query =
      duckdb::StringUtil::Replace(query, "{DATA_PATH}", duckdb::DuckLakeUtil::SQLLiteralToString(catalog.DataPath()));
  query = duckdb::StringUtil::Replace(query, "{METADATA_PATH}",
                                      duckdb::DuckLakeUtil::SQLLiteralToString(catalog.MetadataPath()));
}

duckdb::unique_ptr<duckdb::QueryResult> PgDuckLakeMetadataManager::Query(duckdb::string query) {
  SubstitutePathPlaceholders(query, transaction);
  SubstituteCatalogPlaceholders(query);
  return CreateSPIResult(query);
}

/*
 * Build a SELECT column list from the columns_to_read vector.
 * Mirrors the static GetProjection() in ducklake_metadata_manager.cpp.
 */
static duckdb::string BuildProjection(const duckdb::vector<duckdb::string> &columns_to_read) {
  duckdb::string result;
  duckdb::idx_t i = 1;
  for (auto &entry : columns_to_read) {
    if (!result.empty()) {
      result += ", ";
    }
    result += "inlined_data." + entry + " AS c" + std::to_string(i++);
  }
  return result;
}

/*
 * ReadInlinedData override: route through DuckDB's query engine instead of
 * SPI.  DuckDB resolves pgduckdb."ducklake".table through PostgresCatalog
 * -> PostgresTableReader, which acquires GlobalProcessLock in 32-tuple
 * batches instead of holding it for the entire SPI operation.
 */
duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::ReadInlinedData(duckdb::DuckLakeSnapshot snapshot, const duckdb::string &inlined_table_name,
                                           const duckdb::vector<duckdb::string> &columns_to_read) {
  auto projection = BuildProjection(columns_to_read);
  auto query =
      duckdb::StringUtil::Format(R"(
SELECT %s
FROM pgduckdb."%s".%s inlined_data
WHERE %llu >= begin_snapshot AND (%llu < end_snapshot OR end_snapshot IS NULL)
ORDER BY row_id;)",
                                 projection, PGDUCKLAKE_PG_SCHEMA, duckdb::SQLIdentifier(inlined_table_name),
                                 (unsigned long long)snapshot.snapshot_id, (unsigned long long)snapshot.snapshot_id);
  elog(DEBUG1, "ReadInlinedData via DuckDB: %s", query.c_str());
  return transaction.Query(query);
}

duckdb::unique_ptr<duckdb::QueryResult> PgDuckLakeMetadataManager::Query(duckdb::DuckLakeSnapshot snapshot,
                                                                         duckdb::string query) {
  DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
  return Query(query);
}

duckdb::unique_ptr<duckdb::QueryResult> PgDuckLakeMetadataManager::Execute(duckdb::string query) {
  SubstitutePathPlaceholders(query, transaction);
  SubstituteCatalogPlaceholders(query);
  return CreateSPIResult(query);
}

duckdb::unique_ptr<duckdb::QueryResult> PgDuckLakeMetadataManager::Execute(duckdb::DuckLakeSnapshot snapshot,
                                                                           duckdb::string query) {
  DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
  return Execute(query);
}

duckdb::unique_ptr<duckdb::QueryResult> PgDuckLakeMetadataManager::ExecuteCommit(duckdb::DuckLakeSnapshot snapshot,
                                                                                 duckdb::string query) {
  DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
  SubstituteCatalogPlaceholders(query);
  /* Skip the snapshot sync trigger during commit.  The trigger exists
   * for external DuckDB clients that write directly to the ducklake
   * metadata tables; pg_ducklake's own commits have nothing to
   * reverse-sync.  Running the trigger on a DuckDB worker thread
   * crashes because PG's InterruptHoldoffCount is not thread-safe. */
  SkipSnapshotSyncGuard sync_guard;
  return CreateSPIExecuteInSubtransaction(query);
}

bool PgDuckLakeMetadataManager::IsInitialized() {

  auto tup = SearchSysCache1(NAMESPACENAME, CStringGetDatum(PGDUCKLAKE_PG_SCHEMA));

  if (!HeapTupleIsValid(tup))
    return false;

  auto nspoid = ((Form_pg_namespace)GETSTRUCT(tup))->oid;
  ReleaseSysCache(tup);

  auto rel = table_open(RelationRelationId, AccessShareLock);

  ScanKeyData scankey;

  ScanKeyInit(&scankey, Anum_pg_class_relnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(nspoid));

  auto scan = systable_beginscan(rel, ClassNameNspIndexId, /* pg_class_relname_nsp_index */
                                 true, NULL, 1, &scankey);

  bool found = false;

  while (HeapTupleIsValid(tup = systable_getnext(scan))) {
    Form_pg_class classForm = (Form_pg_class)GETSTRUCT(tup);
    const char *relname = NameStr(classForm->relname);

    /* Match LIKE 'ducklake_%' */
    if (strncmp(relname, "ducklake_", 9) == 0 && classForm->relkind == RELKIND_RELATION) {
      found = true;
      break;
    }
  }

  systable_endscan(scan);
  table_close(rel, AccessShareLock);

  return found;
}

/*
 * Ensure the snapshot sync trigger exists on ducklake.ducklake_snapshot.
 * Called during metadata manager initialization (IsInitialized / InitializeDuckLake)
 * so the trigger is created exactly once per backend.
 *
 * Uses the same SPI pattern as CreateSPIResult (lock, snapshot, force_execution
 * GUC) since this runs inside DuckDB's ATTACH path where re-entering DuckDB
 * would cause infinite recursion.
 */
void PgDuckLakeMetadataManager::EnsureSnapshotTrigger() {
  GlobalProcessLockGuard global_lock;
  PostgresScopedStackReset scoped_stack_reset;

  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());

  auto save_nestlevel = NewGUCNestLevel();
  ::SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);

  int ret = SPI_exec(R"(
		SELECT 1 FROM pg_trigger t
		JOIN pg_class c ON t.tgrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = 'ducklake'
		  AND c.relname = 'ducklake_snapshot'
		  AND t.tgname = 'ducklake_snapshot_sync_trigger'
		)",
                     1);
  if (ret != SPI_OK_SELECT)
    elog(ERROR, "SPI_exec failed: %s", SPI_result_code_string(ret));

  if (SPI_processed == 0) {
    ret = SPI_exec(R"(
		CREATE TRIGGER ducklake_snapshot_sync_trigger
		AFTER INSERT ON ducklake.ducklake_snapshot
		FOR EACH ROW
		EXECUTE FUNCTION ducklake._snapshot_trigger()
		)",
                   0);
    if (ret != SPI_OK_UTILITY)
      elog(ERROR, "SPI_exec CREATE TRIGGER failed: %s", SPI_result_code_string(ret));
  }

  AtEOXact_GUC(false, save_nestlevel);
  PopActiveSnapshot();
  SPI_finish();
}

bool PgDuckLakeMetadataManager::IsInitialized(duckdb::DuckLakeOptions & /*options*/) {
  bool initialized = IsInitialized();
  if (initialized)
    EnsureSnapshotTrigger();
  return initialized;
}

void PgDuckLakeMetadataManager::InitializeDuckLake(bool has_explicit_schema, duckdb::DuckLakeEncryption encryption) {
  DuckLakeMetadataManager::InitializeDuckLake(has_explicit_schema, encryption);
  EnsureSnapshotTrigger();
}

duckdb::string PgDuckLakeMetadataManager::GetInlinedTableQueries(duckdb::DuckLakeSnapshot commit_snapshot,
                                                                 const duckdb::DuckLakeTableInfo &table,
                                                                 duckdb::string &inlined_tables,
                                                                 duckdb::string &inlined_table_queries) {
  auto table_name =
      DuckLakeMetadataManager::GetInlinedTableQueries(commit_snapshot, table, inlined_tables, inlined_table_queries);

  // Grant access to predefined roles so SPI metadata queries succeed
  // regardless of which user created the inlined data table.
  duckdb::string roles;
  for (const char *role : {superuser_role, writer_role, reader_role}) {
    if (role && role[0] != '\0') {
      if (!roles.empty())
        roles += ", ";
      roles += duckdb::StringUtil::Format("%s", duckdb::SQLIdentifier(role));
    }
  }
  if (!roles.empty()) {
    inlined_table_queries += duckdb::StringUtil::Format("\nGRANT ALL ON {METADATA_CATALOG}.%s TO %s;",
                                                        duckdb::SQLIdentifier(table_name), roles);
  }

  return table_name;
}

// Helper functions for direct insert optimization

TableInliningState GetTableInliningState(Oid table_oid, uint64_t *table_id_out, uint64_t *schema_version_out,
                                         int64_t *row_limit_out) {
  int ret;
  TableInliningState state = TI_NO_TABLE;

  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "SPI_connect failed: %d", ret);
    return TI_NO_TABLE;
  }

  HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(table_oid));
  if (!HeapTupleIsValid(tp)) {
    SPI_finish();
    return TI_NO_TABLE;
  }

  Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
  char *table_name = NameStr(reltup->relname);
  Oid schema_oid = reltup->relnamespace;
  ReleaseSysCache(tp);

  HeapTuple ntp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(schema_oid));
  if (!HeapTupleIsValid(ntp)) {
    SPI_finish();
    return TI_NO_TABLE;
  }

  Form_pg_namespace nstup = (Form_pg_namespace)GETSTRUCT(ntp);
  char *schema_name = NameStr(nstup->nspname);
  ReleaseSysCache(ntp);

  /* Single SPI query that returns all the information we need:
   *   col 0: table_id          -- from ducklake_table
   *   col 1: inlined schema_version -- from ducklake_inlined_data_tables
   *   col 2: data_inlining_row_limit -- from ducklake_metadata (NULL if unset)
   *   col 3: max per-table schema_version -- from ducklake_schema_versions
   *
   * Returns 0 rows when the table doesn't exist or has no inlined
   * data entry.  A NULL in col 2 means the limit was not explicitly
   * set; a NULL in col 3 means no ALTER has ever been performed on
   * this table (safe to proceed). */
  StringInfoData query;
  initStringInfo(&query);
  appendStringInfo(&query,
                   "SELECT dt.table_id, "
                   "       idt.schema_version, "
                   "       (SELECT m.value::bigint "
                   "        FROM ducklake.ducklake_metadata m "
                   "        WHERE m.key = 'data_inlining_row_limit' "
                   "        AND m.scope IS NULL), "
                   "       (SELECT MAX(sv.schema_version) "
                   "        FROM ducklake.ducklake_schema_versions sv "
                   "        WHERE sv.table_id = dt.table_id) "
                   "FROM ducklake.ducklake_table dt "
                   "JOIN ducklake.ducklake_schema ds ON dt.schema_id = ds.schema_id "
                   "LEFT JOIN ducklake.ducklake_inlined_data_tables idt "
                   "  ON idt.table_id = dt.table_id "
                   "WHERE dt.table_name = '%s' "
                   "AND ds.schema_name = '%s' "
                   "AND dt.end_snapshot IS NULL "
                   "AND ds.end_snapshot IS NULL "
                   "LIMIT 1",
                   table_name, schema_name);

  ret = SPI_execute(query.data, true, 1);
  if (ret == SPI_OK_SELECT && SPI_processed > 0) {
    HeapTuple tuple = SPI_tuptable->vals[0];
    bool isnull;

    /* col 0: table_id (must be present; NULL here means no ducklake row) */
    Datum table_id_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
    if (isnull) {
      state = TI_NO_TABLE;
      goto done;
    }
    uint64_t table_id = DatumGetInt64(table_id_datum);

    /* col 1: inlined schema_version (NULL if no inlined_data_tables row) */
    Datum sv_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &isnull);
    if (isnull) {
      state = TI_NO_INLINED_TABLE;
      goto done;
    }
    uint64_t schema_version = DatumGetInt64(sv_datum);

    /* col 2: data_inlining_row_limit must be explicitly set > 0 */
    Datum limit_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 3, &isnull);
    if (isnull || DatumGetInt64(limit_datum) <= 0) {
      state = TI_NO_INLINED_TABLE;
      goto done;
    }
    int64_t row_limit = DatumGetInt64(limit_datum);

    /* col 3: per-table schema version check.
     * NULL means no ALTER has been performed -- safe to proceed.
     * Non-NULL must match the inlined table's schema_version.
     *
     * Previously this compared against the global schema_version in
     * ducklake_snapshot, which caused false negatives: a DDL on any
     * other table would bump the global version and block direct
     * insert for all unrelated tables. */
    Datum max_sv_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 4, &isnull);
    if (!isnull && (uint64_t)DatumGetInt64(max_sv_datum) != schema_version) {
      state = TI_SCHEMA_VERSION_MISMATCH;
      goto done;
    }

    *table_id_out = table_id;
    *schema_version_out = schema_version;
    if (row_limit_out)
      *row_limit_out = row_limit;
    state = TI_OK;
  }

done:

  SPI_finish();
  return state;
}

bool GetTableInliningInfo(Oid table_oid, uint64_t *table_id_out, uint64_t *schema_version_out) {
  return GetTableInliningState(table_oid, table_id_out, schema_version_out, NULL) == TI_OK;
}

uint64_t GetNextRowIdForTable(uint64_t table_id, uint64_t schema_version) {
  int ret;
  uint64_t next_row_id = 0;

  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "SPI_connect failed: %d", ret);
    return 0;
  }

  /* Read next_row_id from ducklake_table_stats (O(1) index lookup).
   * The COPY FROM commit path keeps this row up to date after
   * each insert.  If no row exists (first insert into this
   * table), fall back to MAX(row_id) + 1 from the inlined data table. */
  StringInfoData query;
  initStringInfo(&query);
  appendStringInfo(&query,
                   "SELECT next_row_id "
                   "FROM ducklake.ducklake_table_stats "
                   "WHERE table_id = %llu",
                   (unsigned long long)table_id);

  ret = SPI_execute(query.data, true, 1);
  if (ret == SPI_OK_SELECT && SPI_processed > 0) {
    HeapTuple tuple = SPI_tuptable->vals[0];
    bool isnull;
    Datum row_id_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
    if (!isnull) {
      next_row_id = DatumGetInt64(row_id_datum);
    }
  } else if (ret == SPI_OK_SELECT) {
    /* No stats row -- fall back to scanning the inlined data table. */
    StringInfoData fallback;
    initStringInfo(&fallback);
    appendStringInfo(&fallback,
                     "SELECT COALESCE(MAX(row_id) + 1, 0) "
                     "FROM ducklake.ducklake_inlined_data_%llu_%llu",
                     (unsigned long long)table_id, (unsigned long long)schema_version);

    ret = SPI_execute(fallback.data, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0) {
      HeapTuple tuple = SPI_tuptable->vals[0];
      bool isnull;
      Datum row_id_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
      if (!isnull) {
        next_row_id = DatumGetInt64(row_id_datum);
      }
    }
  }

  SPI_finish();
  return next_row_id;
}

/* File-local helper for DirectInsertReservation: reserve the row_id range
 * under the per-table advisory lock.  Returns row_id_start. */
static uint64_t ReserveRowIdRangeImpl(uint64_t table_id, uint64_t nrows) {
  int ret;
  uint64_t row_id_start = 0;

  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "SPI_connect failed: %d", ret);
  }

  /* Per-table advisory lock; auto-released at xact end.  Serializes
   * concurrent direct inserts on the same table so the read+update of
   * ducklake_table_stats below is atomic without needing a UNIQUE on
   * table_id. */
  StringInfoData lock_query;
  initStringInfo(&lock_query);
  appendStringInfo(&lock_query, "SELECT pg_advisory_xact_lock(%d::int4, %lld::int4)",
                   (int)DUCKLAKE_DIRECT_INSERT_LOCK_NS, (long long)table_id);
  ret = SPI_execute(lock_query.data, false, 0);
  if (ret != SPI_OK_SELECT) {
    SPI_finish();
    elog(ERROR, "DirectInsertReservation: advisory lock acquire failed: %d", ret);
  }

  /* Read current next_row_id under the lock. */
  StringInfoData read_query;
  initStringInfo(&read_query);
  appendStringInfo(&read_query, "SELECT next_row_id FROM ducklake.ducklake_table_stats WHERE table_id = %llu",
                   (unsigned long long)table_id);
  ret = SPI_execute(read_query.data, true, 1);
  if (ret == SPI_OK_SELECT && SPI_processed > 0) {
    HeapTuple tuple = SPI_tuptable->vals[0];
    bool isnull;
    Datum d = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
    if (!isnull) {
      row_id_start = DatumGetInt64(d);
    }
  }

  /* Try UPDATE first; on 0 rows this is the first direct insert into
   * this table and we INSERT the stats row instead.  The advisory lock
   * makes this read-then-update sequence atomic against concurrent
   * direct inserts. */
  StringInfoData stats_update;
  initStringInfo(&stats_update);
  appendStringInfo(&stats_update,
                   "UPDATE ducklake.ducklake_table_stats "
                   "SET next_row_id  = next_row_id  + %llu, "
                   "    record_count = record_count + %llu "
                   "WHERE table_id = %llu",
                   (unsigned long long)nrows, (unsigned long long)nrows, (unsigned long long)table_id);
  ret = SPI_execute(stats_update.data, false, 0);
  if (ret != SPI_OK_UPDATE) {
    SPI_finish();
    elog(ERROR, "DirectInsertReservation: stats UPDATE failed: %d", ret);
  }

  if (SPI_processed == 0) {
    StringInfoData stats_insert;
    initStringInfo(&stats_insert);
    appendStringInfo(&stats_insert,
                     "INSERT INTO ducklake.ducklake_table_stats "
                     "(table_id, record_count, next_row_id, file_size_bytes) "
                     "VALUES (%llu, %llu, %llu, 0)",
                     (unsigned long long)table_id, (unsigned long long)nrows, (unsigned long long)nrows);
    ret = SPI_execute(stats_insert.data, false, 0);
    if (ret != SPI_OK_INSERT) {
      SPI_finish();
      elog(ERROR, "DirectInsertReservation: stats INSERT failed: %d", ret);
    }
    /* row_id_start stays 0 -- the freshly inserted stats row started at 0. */
  }

  SPI_finish();
  return row_id_start;
}

uint64_t GetNextSnapshotId() {
  int ret;
  uint64_t next_snapshot_id = 1; // Default to 1 if no snapshots exist yet

  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "SPI_connect failed: %d", ret);
    return next_snapshot_id;
  }

  const char *query = "SELECT snapshot_id + 1 FROM ducklake.ducklake_snapshot "
                      "ORDER BY snapshot_id DESC LIMIT 1";

  ret = SPI_execute(query, true, 1);
  if (ret == SPI_OK_SELECT && SPI_processed > 0) {
    HeapTuple tuple = SPI_tuptable->vals[0];
    bool isnull;
    Datum snapshot_id_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
    if (!isnull) {
      next_snapshot_id = DatumGetInt64(snapshot_id_datum);
    }
  }

  SPI_finish();
  return next_snapshot_id;
}

/* File-local helper for DirectInsertReservation: reserve a fresh
 * snapshot_id via INSERT ... ON CONFLICT DO NOTHING RETURNING with a
 * bounded retry loop.  `max_retries` is the count of *re-attempts* after
 * the first try, so 0 means single attempt with no retry.
 *
 * The LEFT JOIN against `(SELECT 1)` guarantees the outer SELECT always
 * produces exactly one row even when ducklake_snapshot is empty (in which
 * case latest.* are NULL and the COALESCE defaults take over).  All four
 * reads of `latest` come from the same MVCC instant inside the INSERT, so
 * schema_version / next_catalog_id / next_file_id stay consistent with
 * the chosen snapshot_id. */
static uint64_t ReserveSnapshotIdImpl(DirectInsertPattern pattern_for_stats) {
  const int max_retries = direct_insert_max_retries;

  static const char *insert_query = "INSERT INTO ducklake.ducklake_snapshot "
                                    "(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) "
                                    "SELECT COALESCE(latest.snapshot_id,     0) + 1, "
                                    "       NOW(), "
                                    "       COALESCE(latest.schema_version,  0), "
                                    "       COALESCE(latest.next_catalog_id, 1), "
                                    "       COALESCE(latest.next_file_id,    0) "
                                    "  FROM (SELECT 1) AS d "
                                    "  LEFT JOIN ("
                                    "    SELECT snapshot_id, schema_version, next_catalog_id, next_file_id "
                                    "      FROM ducklake.ducklake_snapshot "
                                    "     ORDER BY snapshot_id DESC LIMIT 1"
                                    "  ) latest ON true "
                                    "ON CONFLICT (snapshot_id) DO NOTHING "
                                    "RETURNING snapshot_id";

  uint64_t base_wait_us = 100UL * 1000UL; /* 100 ms */

  /* Loop covers max_retries + 1 attempts: the initial try plus
   * max_retries re-attempts. */
  for (int attempt = 0; attempt <= max_retries; attempt++) {
    int ret;
    if ((ret = SPI_connect()) < 0) {
      elog(ERROR, "SPI_connect failed: %d", ret);
    }

    ret = SPI_execute(insert_query, false, 0);
    /* PG returns SPI_OK_INSERT_RETURNING for INSERT ... RETURNING even
     * when ON CONFLICT DO NOTHING resulted in zero rows inserted; the
     * conflict case is signalled by SPI_processed == 0. */
    if (ret != SPI_OK_INSERT_RETURNING) {
      SPI_finish();
      elog(ERROR, "DirectInsertReservation: snapshot INSERT failed: %d", ret);
    }

    if (SPI_processed > 0) {
      uint64_t snapshot_id = 0;
      HeapTuple tuple = SPI_tuptable->vals[0];
      bool isnull;
      Datum d = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
      if (!isnull) {
        snapshot_id = DatumGetInt64(d);
      }
      SPI_finish();
      return snapshot_id;
    }

    /* Lost the PK race: ON CONFLICT DO NOTHING swallowed our row. */
    SPI_finish();
    DirectInsertStatsBump(pattern_for_stats, DI_R_RETRY);

    if (attempt >= max_retries) {
      ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                      errmsg("ducklake direct insert: snapshot_id reservation failed "
                             "after %d retr%s",
                             max_retries, max_retries == 1 ? "y" : "ies"),
                      errhint("Concurrent commit pressure on ducklake_snapshot; "
                              "retry the statement or raise ducklake.direct_insert_max_retries.")));
    }

    /* Exponential backoff (1.5x) with +/-25% jitter to avoid thundering
     * herd.  base_wait_us starts at 100ms and grows; the floor of 4us
     * defends against future changes that might lower the start point. */
    if (base_wait_us < 4) {
      base_wait_us = 4;
    }
    long jitter_us = ((long)random() % (long)(base_wait_us / 2)) - (long)(base_wait_us / 4);
    long sleep_us = (long)base_wait_us + jitter_us;
    if (sleep_us < 1000) {
      sleep_us = 1000;
    }
    pg_usleep(sleep_us);
    base_wait_us = base_wait_us + (base_wait_us >> 1); /* *= 1.5 */
  }

  /* unreachable */
  return 0;
}

/* COPY FROM commit path.  Distinct from the direct-insert path because
 * COPY does not know its row count up-front and cannot use the
 * DirectInsertReservation API.  Has the same MAX(snapshot_id)+1 and
 * read-then-update stats races as the pre-#191 direct-insert path; a
 * chunked reservation migration for COPY FROM is tracked separately. */
void CreateSnapshotForCopyFrom(uint64_t snapshot_id, uint64_t table_id, int64_t rows_inserted) {
  int ret;

  elog(DEBUG1, "CreateSnapshotForCopyFrom: creating snapshot %llu", (unsigned long long)snapshot_id);

  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "CreateSnapshotForCopyFrom: SPI_connect failed: %d", ret);
    return;
  }

  /* Read the latest snapshot via primary-key index backward scan (O(1))
   * rather than MAX() over the full table.  We carry its schema_version
   * forward: direct insert is a data-only change, so the new snapshot
   * must preserve the global catalog view (which tables are visible).
   * Using a per-table schema_version here would effectively roll back
   * the catalog and hide tables created after this one. */
  const char *query_state = "SELECT COALESCE(next_catalog_id, 1), COALESCE(next_file_id, 0), "
                            "       COALESCE(schema_version, 0) "
                            "FROM ducklake.ducklake_snapshot "
                            "ORDER BY snapshot_id DESC LIMIT 1";

  uint64_t next_catalog_id = 1;
  uint64_t next_file_id = 0;
  uint64_t schema_version = 0;

  ret = SPI_execute(query_state, true, 1);
  if (ret == SPI_OK_SELECT && SPI_processed > 0) {
    HeapTuple tuple = SPI_tuptable->vals[0];
    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    bool isnull;

    Datum catalog_id_datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);
    if (!isnull) {
      next_catalog_id = DatumGetInt64(catalog_id_datum);
    }

    Datum file_id_datum = SPI_getbinval(tuple, tupdesc, 2, &isnull);
    if (!isnull) {
      next_file_id = DatumGetInt64(file_id_datum);
    }

    Datum schema_version_datum = SPI_getbinval(tuple, tupdesc, 3, &isnull);
    if (!isnull) {
      schema_version = DatumGetInt64(schema_version_datum);
    }
  }

  StringInfoData snapshot_insert;
  initStringInfo(&snapshot_insert);
  appendStringInfo(&snapshot_insert,
                   "INSERT INTO ducklake.ducklake_snapshot "
                   "(snapshot_id, snapshot_time, schema_version, next_catalog_id, "
                   "next_file_id) "
                   "VALUES (%llu, NOW(), %llu, %llu, %llu)",
                   (unsigned long long)snapshot_id, (unsigned long long)schema_version,
                   (unsigned long long)next_catalog_id, (unsigned long long)next_file_id);

  ret = SPI_execute(snapshot_insert.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    elog(ERROR, "CreateSnapshotForCopyFrom: failed to insert snapshot: %d", ret);
  }

  StringInfoData changes_insert;
  initStringInfo(&changes_insert);
  appendStringInfo(&changes_insert,
                   "INSERT INTO ducklake.ducklake_snapshot_changes "
                   "(snapshot_id, changes_made, author, commit_message, commit_extra_info) "
                   "VALUES (%llu, 'inlined_data_insert', NULL, NULL, NULL)",
                   (unsigned long long)snapshot_id);

  ret = SPI_execute(changes_insert.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    elog(ERROR, "CreateSnapshotForCopyFrom: failed to insert snapshot changes: %d", ret);
  }

  StringInfoData stats_update;
  initStringInfo(&stats_update);
  appendStringInfo(&stats_update,
                   "UPDATE ducklake.ducklake_table_stats "
                   "SET next_row_id = next_row_id + %lld, "
                   "    record_count = record_count + %lld "
                   "WHERE table_id = %llu",
                   (long long)rows_inserted, (long long)rows_inserted, (unsigned long long)table_id);

  ret = SPI_execute(stats_update.data, false, 0);
  if (ret != SPI_OK_UPDATE) {
    elog(ERROR, "CreateSnapshotForCopyFrom: failed to update table stats: %d", ret);
  }

  if (SPI_processed == 0) {
    StringInfoData stats_insert;
    initStringInfo(&stats_insert);
    appendStringInfo(&stats_insert,
                     "INSERT INTO ducklake.ducklake_table_stats "
                     "(table_id, record_count, next_row_id, file_size_bytes) "
                     "VALUES (%llu, %lld, %lld, 0)",
                     (unsigned long long)table_id, (long long)rows_inserted, (long long)rows_inserted);

    ret = SPI_execute(stats_insert.data, false, 0);
    if (ret != SPI_OK_INSERT) {
      elog(ERROR, "CreateSnapshotForCopyFrom: failed to insert table stats: %d", ret);
    }

    StringInfoData col_stats_insert;
    initStringInfo(&col_stats_insert);
    appendStringInfo(&col_stats_insert,
                     "INSERT INTO ducklake.ducklake_table_column_stats "
                     "(table_id, column_id, contains_null, contains_nan, "
                     "min_value, max_value, extra_stats) "
                     "SELECT %llu, column_id, NULL, NULL, NULL, NULL, NULL "
                     "FROM ducklake.ducklake_column "
                     "WHERE table_id = %llu AND end_snapshot IS NULL",
                     (unsigned long long)table_id, (unsigned long long)table_id);

    ret = SPI_execute(col_stats_insert.data, false, 0);
    if (ret != SPI_OK_INSERT) {
      elog(ERROR, "CreateSnapshotForCopyFrom: failed to insert column stats: %d", ret);
    }
  }

  SPI_finish();
}

DirectInsertReservation::DirectInsertReservation(uint64_t table_id, uint64_t nrows, DirectInsertPattern pattern)
    : table_id_(table_id), row_id_start_(ReserveRowIdRangeImpl(table_id, nrows)),
      snapshot_id_(ReserveSnapshotIdImpl(pattern)) {
}

void DirectInsertReservation::Commit() {
  int ret;

  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "DirectInsertReservation::Commit: SPI_connect failed: %d", ret);
    return;
  }

  /* The ducklake_snapshot row was already inserted by ReserveSnapshotIdImpl;
   * ducklake_table_stats was already advanced by ReserveRowIdRangeImpl.
   * Here we only record the change set, then idempotently bootstrap
   * ducklake_table_column_stats so DuckLake's stats LEFT JOIN finds the
   * column rows.  The bootstrap runs unconditionally because the WHERE
   * NOT EXISTS subselect makes it cheap when rows already exist and the
   * only correctness risk is a concurrent path racing for the same
   * (table_id, column_id) pair, which is harmless here. */
  StringInfoData changes_insert;
  initStringInfo(&changes_insert);
  appendStringInfo(&changes_insert,
                   "INSERT INTO ducklake.ducklake_snapshot_changes "
                   "(snapshot_id, changes_made, author, commit_message, commit_extra_info) "
                   "VALUES (%llu, 'inlined_data_insert', NULL, NULL, NULL)",
                   (unsigned long long)snapshot_id_);
  ret = SPI_execute(changes_insert.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    SPI_finish();
    elog(ERROR, "DirectInsertReservation::Commit: snapshot_changes INSERT failed: %d", ret);
  }

  StringInfoData col_stats_insert;
  initStringInfo(&col_stats_insert);
  appendStringInfo(&col_stats_insert,
                   "INSERT INTO ducklake.ducklake_table_column_stats "
                   "(table_id, column_id, contains_null, contains_nan, "
                   " min_value, max_value, extra_stats) "
                   "SELECT %llu, c.column_id, NULL, NULL, NULL, NULL, NULL "
                   "  FROM ducklake.ducklake_column c "
                   " WHERE c.table_id = %llu AND c.end_snapshot IS NULL "
                   "   AND NOT EXISTS ("
                   "     SELECT 1 FROM ducklake.ducklake_table_column_stats ts "
                   "      WHERE ts.table_id = c.table_id AND ts.column_id = c.column_id"
                   "   )",
                   (unsigned long long)table_id_, (unsigned long long)table_id_);
  ret = SPI_execute(col_stats_insert.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    SPI_finish();
    elog(ERROR, "DirectInsertReservation::Commit: column_stats INSERT failed: %d", ret);
  }

  SPI_finish();
}

} // namespace pgducklake
