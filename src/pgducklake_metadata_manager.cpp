/*
 * pgducklake_metadata_manager.cpp — PostgreSQL-backed DuckLake metadata
 * manager.
 *
 * Implements DuckLake metadata operations by translating DuckDB requests into
 * SQL against the ducklake_* metadata tables in PostgreSQL.
 */

#include "pgducklake/pgducklake_metadata_manager.hpp"

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

static void InsertSPITupleTableIntoChunk(duckdb::DataChunk &output,
                                         SPITupleTable *tuptable,
                                         idx_t start_idx, int num_tuples) {
  D_ASSERT(tuptable);
  D_ASSERT(start_idx + num_tuples <= tuptable->numvals);

  if (num_tuples == 0) {
    return;
  }

  int natts = tuptable->tupdesc->natts;

  for (int duckdb_output_index = 0; duckdb_output_index < natts;
       duckdb_output_index++) {
    auto &result = output.data[duckdb_output_index];
    auto attr = TupleDescAttr(tuptable->tupdesc, duckdb_output_index);

    for (int row = 0; row < num_tuples; row++) {
      HeapTuple tuple = tuptable->vals[start_idx + row];
      bool isnull = false;
      Datum datum = SPI_getbinval(tuple, tuptable->tupdesc,
                                  duckdb_output_index + 1, &isnull);
      if (isnull) {
        auto &array_mask = duckdb::FlatVector::Validity(result);
        array_mask.SetInvalid(row);
      } else {
        if (attr->attlen == -1) {
          bool should_free = false;
          Datum detoasted_value = DetoastPostgresDatum(
              reinterpret_cast<varlena *>(datum), &should_free);
          ConvertPostgresToDuckValue(attr->atttypid, detoasted_value, result,
                                     row);
          if (should_free) {
            pfree(DatumGetPointer(detoasted_value));
          }
        } else {
          ConvertPostgresToDuckValue(attr->atttypid, datum, result, row);
        }
      }
    }
  }
}


/*
 * RAII guard for pg_duckdb's GlobalProcessLock.  DuckLake metadata reads run
 * on a DuckDB worker thread that shares the PG backend with other DuckDB
 * threads.  We must hold this lock while calling any PG API (SPI, snapshots,
 * etc.) to prevent concurrent access from other DuckDB threads.
 */
class GlobalProcessLockGuard {
public:
  GlobalProcessLockGuard() { pgduckdb::DuckdbLockGlobalProcess(); }
  ~GlobalProcessLockGuard() { pgduckdb::DuckdbUnlockGlobalProcess(); }
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
  ForceExecutionGuard()
      : saved_(pgduckdb::DuckdbSetForceExecution(false)) {}
  ~ForceExecutionGuard() { pgduckdb::DuckdbSetForceExecution(saved_); }
  ForceExecutionGuard(const ForceExecutionGuard &) = delete;
  ForceExecutionGuard &operator=(const ForceExecutionGuard &) = delete;

private:
  bool saved_;
};

static duckdb::unique_ptr<duckdb::QueryResult>
CreateSPIResult(const duckdb::string &query) {
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
    duckdb::ErrorData error(duckdb::ExceptionType::IO,
                            "SPI execution failed: " + error_message);
    return duckdb::make_uniq<duckdb::MaterializedQueryResult>(std::move(error));
  }

  if (ret < 0) {
    PopActiveSnapshot();
    SPI_finish();
    duckdb::ErrorData error(
        duckdb::ExceptionType::IO,
        "SPI execution failed: " +
            duckdb::string(SPI_result_code_string(ret)));
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
    auto empty_collection =
        duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator);

    return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
        ConvertSPIResultToDuckStatementType(ret), properties, names,
        std::move(empty_collection), client_properties);
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
  auto collection_p =
      duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator, types);

  // Convert SPI rows to DuckDB DataChunks and append them
  for (idx_t row_idx = 0; row_idx < num_rows; row_idx += STANDARD_VECTOR_SIZE) {
    idx_t chunk_size =
        duckdb::MinValue<int>(STANDARD_VECTOR_SIZE, num_rows - row_idx);
    auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
    chunk->Initialize(allocator, types, chunk_size);
    InsertSPITupleTableIntoChunk(*chunk, tuptable, row_idx, chunk_size);

    chunk->SetCardinality(chunk_size);
    collection_p->Append(*chunk);
  }

  PopActiveSnapshot();
  SPI_finish();

  // Create and return the MaterializedQueryResult
  duckdb::StatementProperties properties;
  return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
      duckdb::StatementType::SELECT_STATEMENT, properties, names,
      std::move(collection_p), client_properties);
}

/*
 * Substitute DuckLake catalog/schema placeholders with the PostgreSQL schema
 * constants. We avoid calling transaction.GetCatalog() here because during
 * DuckLake initialization (FinalizeLoad → InitializeDuckLake → Execute), the
 * AttachedDatabase is not yet reachable via the db_manager.
 */
static void SubstituteCatalogPlaceholders(duckdb::string &query) {
  query = duckdb::StringUtil::Replace(query, "{METADATA_CATALOG}",
                                      "\"" PGDUCKLAKE_PG_SCHEMA "\"");
  query =
      duckdb::StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}",
                                  "\"" PGDUCKLAKE_DUCKDB_CATALOG "\"");
  query = duckdb::StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}",
                                      "'" PGDUCKLAKE_DUCKDB_CATALOG "'");
  query = duckdb::StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}",
                                      "'" PGDUCKLAKE_PG_SCHEMA "'");
  query = duckdb::StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}",
                                      "\"" PGDUCKLAKE_PG_SCHEMA "\"");
}

/*
 * Execute a write query in a subtransaction and convert any PostgreSQL ERROR
 * into a duckdb::TransactionException. This allows DuckLake's FlushChanges()
 * retry loop to intercept duplicate-key / unique-constraint failures that
 * arise from concurrent commits, rather than having a PostgreSQL longjmp
 * bypass the C++ catch block and crash the backend.
 */
static duckdb::unique_ptr<duckdb::QueryResult>
CreateSPIExecuteInSubtransaction(const duckdb::string &query) {
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
      error_message =
          duckdb::string("SPI execute failed: ") + SPI_result_code_string(ret);
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
  auto empty_collection =
      duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator);
  return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
      duckdb::StatementType::EXECUTE_STATEMENT, properties, names,
      std::move(empty_collection), client_properties);
}

PgDuckLakeMetadataManager::PgDuckLakeMetadataManager(
    duckdb::DuckLakeTransaction &transaction_)
    : duckdb::PostgresMetadataManager(transaction_) {}

PgDuckLakeMetadataManager::~PgDuckLakeMetadataManager() {}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Query(duckdb::string query) {
  SubstituteCatalogPlaceholders(query);
  return CreateSPIResult(query);
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Query(duckdb::DuckLakeSnapshot snapshot,
                                 duckdb::string query) {
  DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
  return Query(query);
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Execute(duckdb::string query) {
  SubstituteCatalogPlaceholders(query);
  return CreateSPIResult(query);
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Execute(duckdb::DuckLakeSnapshot snapshot,
                                   duckdb::string query) {
  DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
  return Execute(query);
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::ExecuteCommit(duckdb::DuckLakeSnapshot snapshot,
                                         duckdb::string query) {
  DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
  SubstituteCatalogPlaceholders(query);
  return CreateSPIExecuteInSubtransaction(query);
}

bool PgDuckLakeMetadataManager::IsInitialized() {

  auto tup =
      SearchSysCache1(NAMESPACENAME, CStringGetDatum(PGDUCKLAKE_PG_SCHEMA));

  if (!HeapTupleIsValid(tup))
    return false;

  auto nspoid = ((Form_pg_namespace)GETSTRUCT(tup))->oid;
  ReleaseSysCache(tup);

  auto rel = table_open(RelationRelationId, AccessShareLock);

  ScanKeyData scankey;

  ScanKeyInit(&scankey, Anum_pg_class_relnamespace, BTEqualStrategyNumber,
              F_OIDEQ, ObjectIdGetDatum(nspoid));

  auto scan = systable_beginscan(
      rel, ClassNameNspIndexId, /* pg_class_relname_nsp_index */
      true, NULL, 1, &scankey);

  bool found = false;

  while (HeapTupleIsValid(tup = systable_getnext(scan))) {
    Form_pg_class classForm = (Form_pg_class)GETSTRUCT(tup);
    const char *relname = NameStr(classForm->relname);

    /* Match LIKE 'ducklake_%' */
    if (strncmp(relname, "ducklake_", 9) == 0 &&
        classForm->relkind == RELKIND_RELATION) {
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
  ::SetConfigOption("duckdb.force_execution", "false", PGC_USERSET,
                    PGC_S_SESSION);

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
      elog(ERROR, "SPI_exec CREATE TRIGGER failed: %s",
           SPI_result_code_string(ret));
  }

  AtEOXact_GUC(false, save_nestlevel);
  PopActiveSnapshot();
  SPI_finish();
}

bool PgDuckLakeMetadataManager::IsInitialized(
    duckdb::DuckLakeOptions & /*options*/) {
  bool initialized = IsInitialized();
  if (initialized)
    EnsureSnapshotTrigger();
  return initialized;
}

void PgDuckLakeMetadataManager::InitializeDuckLake(
    bool has_explicit_schema, duckdb::DuckLakeEncryption encryption) {
  DuckLakeMetadataManager::InitializeDuckLake(has_explicit_schema, encryption);
  EnsureSnapshotTrigger();
}

bool PgDuckLakeMetadataManager::TypeIsNativelySupported(
    const duckdb::LogicalType &type) {
  // The upstream PostgresMetadataManager marks VARCHAR as not natively
  // supported because Postgres cannot store null bytes in TEXT columns.
  // In our SPI path, VARCHAR values go through normal text I/O, so the
  // bytea encoding that the upstream uses causes type mismatches
  // ("column is of type bytea but expression is of type text") and hex
  // display of text data.  Treat VARCHAR as natively supported here.
  if (type.id() == duckdb::LogicalTypeId::VARCHAR) {
    return true;
  }
  return duckdb::PostgresMetadataManager::TypeIsNativelySupported(type);
}

duckdb::string
PgDuckLakeMetadataManager::GetColumnTypeInternal(const duckdb::LogicalType &type) {
  if (type.id() == duckdb::LogicalTypeId::VARCHAR) {
    return "TEXT";
  }
  return duckdb::PostgresMetadataManager::GetColumnTypeInternal(type);
}

duckdb::string PgDuckLakeMetadataManager::GetInlinedTableQueries(
    duckdb::DuckLakeSnapshot commit_snapshot,
    const duckdb::DuckLakeTableInfo &table, duckdb::string &inlined_tables,
    duckdb::string &inlined_table_queries) {
  auto table_name = DuckLakeMetadataManager::GetInlinedTableQueries(
      commit_snapshot, table, inlined_tables, inlined_table_queries);

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
    inlined_table_queries += duckdb::StringUtil::Format(
        "\nGRANT ALL ON {METADATA_CATALOG}.%s TO %s;",
        duckdb::SQLIdentifier(table_name), roles);
  }

  return table_name;
}

// Helper functions for direct insert optimization
bool GetTableInliningInfo(Oid table_oid, uint64_t *table_id_out,
                          uint64_t *schema_version_out) {
  int ret;
  bool result = false;

  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "SPI_connect failed: %d", ret);
    return false;
  }

  HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(table_oid));
  if (!HeapTupleIsValid(tp)) {
    SPI_finish();
    return false;
  }

  Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
  char *table_name = NameStr(reltup->relname);
  Oid schema_oid = reltup->relnamespace;
  ReleaseSysCache(tp);

  HeapTuple ntp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(schema_oid));
  if (!HeapTupleIsValid(ntp)) {
    SPI_finish();
    return false;
  }

  Form_pg_namespace nstup = (Form_pg_namespace)GETSTRUCT(ntp);
  char *schema_name = NameStr(nstup->nspname);
  ReleaseSysCache(ntp);

  StringInfoData query;
  initStringInfo(&query);
  appendStringInfo(
      &query,
      "SELECT dt.table_id, idt.schema_version "
      "FROM ducklake.ducklake_table dt "
      "JOIN ducklake.ducklake_schema ds ON dt.schema_id = ds.schema_id "
      "LEFT JOIN ducklake.ducklake_inlined_data_tables idt ON idt.table_id = "
      "dt.table_id "
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

    Datum table_id_datum =
        SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
    if (!isnull) {
      uint64_t table_id = DatumGetInt64(table_id_datum);

      Datum schema_version_datum =
          SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &isnull);
      if (!isnull) {
        uint64_t schema_version = DatumGetInt64(schema_version_datum);
        *table_id_out = table_id;
        *schema_version_out = schema_version;
        result = true;
      }
    }
  }

  SPI_finish();
  return result;
}

uint64_t GetNextRowIdForTable(uint64_t table_id, uint64_t schema_version) {
  int ret;
  uint64_t next_row_id = 0;

  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "SPI_connect failed: %d", ret);
    return 0;
  }

  StringInfoData query;
  initStringInfo(&query);
  appendStringInfo(&query,
                   "SELECT COALESCE(MAX(row_id), -1) + 1 "
                   "FROM ducklake.ducklake_inlined_data_%llu_%llu",
                   (unsigned long long)table_id,
                   (unsigned long long)schema_version);

  ret = SPI_execute(query.data, true, 1);
  if (ret == SPI_OK_SELECT && SPI_processed > 0) {
    HeapTuple tuple = SPI_tuptable->vals[0];
    bool isnull;
    Datum row_id_datum =
        SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
    if (!isnull) {
      next_row_id = DatumGetInt64(row_id_datum);
    }
  }

  SPI_finish();
  return next_row_id;
}

uint64_t GetNextSnapshotId() {
  int ret;
  uint64_t next_snapshot_id = 1; // Default to 1 if no snapshots exist yet

  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "SPI_connect failed: %d", ret);
    return next_snapshot_id;
  }

  const char *query = "SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM "
                      "ducklake.ducklake_snapshot";

  ret = SPI_execute(query, true, 1);
  if (ret == SPI_OK_SELECT && SPI_processed > 0) {
    HeapTuple tuple = SPI_tuptable->vals[0];
    bool isnull;
    Datum snapshot_id_datum =
        SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
    if (!isnull) {
      next_snapshot_id = DatumGetInt64(snapshot_id_datum);
    }
  }

  SPI_finish();
  return next_snapshot_id;
}

void CreateSnapshotForDirectInsert(uint64_t snapshot_id,
                                   uint64_t schema_version) {
  int ret;

  elog(DEBUG1,
       "CreateSnapshotForDirectInsert: creating snapshot %llu with "
       "schema_version %llu",
       (unsigned long long)snapshot_id, (unsigned long long)schema_version);

  if ((ret = SPI_connect()) < 0) {
    elog(ERROR, "CreateSnapshotForDirectInsert: SPI_connect failed: %d", ret);
    return;
  }

  const char *query_state =
      "SELECT COALESCE(MAX(next_catalog_id), 1) AS next_catalog_id, "
      "COALESCE(MAX(next_file_id), 0) AS next_file_id "
      "FROM ducklake.ducklake_snapshot";

  uint64_t next_catalog_id = 1;
  uint64_t next_file_id = 0;

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
  }

  StringInfoData snapshot_insert;
  initStringInfo(&snapshot_insert);
  appendStringInfo(
      &snapshot_insert,
      "INSERT INTO ducklake.ducklake_snapshot "
      "(snapshot_id, snapshot_time, schema_version, next_catalog_id, "
      "next_file_id) "
      "VALUES (%llu, NOW(), %llu, %llu, %llu)",
      (unsigned long long)snapshot_id, (unsigned long long)schema_version,
      (unsigned long long)next_catalog_id, (unsigned long long)next_file_id);

  elog(DEBUG1, "CreateSnapshotForDirectInsert: executing %s",
       snapshot_insert.data);
  ret = SPI_execute(snapshot_insert.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    elog(ERROR, "CreateSnapshotForDirectInsert: failed to insert snapshot: %d",
         ret);
  }

  // Build INSERT for ducklake_snapshot_changes
  // Use a simple description for the changes_made field
  StringInfoData changes_insert;
  initStringInfo(&changes_insert);
  appendStringInfo(
      &changes_insert,
      "INSERT INTO ducklake.ducklake_snapshot_changes "
      "(snapshot_id, changes_made, author, commit_message, commit_extra_info) "
      "VALUES (%llu, 'inlined_data_insert', NULL, NULL, NULL)",
      (unsigned long long)snapshot_id);

  elog(DEBUG1, "CreateSnapshotForDirectInsert: executing %s",
       changes_insert.data);
  ret = SPI_execute(changes_insert.data, false, 0);
  if (ret != SPI_OK_INSERT) {
    elog(ERROR,
         "CreateSnapshotForDirectInsert: failed to insert snapshot changes: %d",
         ret);
  }

  SPI_finish();
  elog(DEBUG1,
       "CreateSnapshotForDirectInsert: successfully created snapshot %llu",
       (unsigned long long)snapshot_id);
}

} // namespace pgducklake
