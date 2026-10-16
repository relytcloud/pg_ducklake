/* PostgreSQL-backed DuckLake metadata manager: DuckDB metadata requests -> SQL on ducklake_* tables. */

#include "pgducklake/catalog_sync.hpp"
#include "pgducklake/constants.hpp"
#include "pgducklake/duckdb_manager.hpp"
#include "pgducklake/guc.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "pgddb/pgddb_types.hpp"
#include "pgddb/pgddb_utils.hpp"

#include <common/ducklake_types.hpp>
#include <common/ducklake_util.hpp>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/enums/statement_type.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <storage/ducklake_metadata_info.hpp>
#include <storage/ducklake_stats.hpp>

extern "C" {
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
}

// pgddb_process_lock.hpp transitively pulls postgres.h, so it follows the PG header block.
#include "pgddb/pgddb_process_lock.hpp"

namespace pgducklake {
static duckdb::StatementType
ConvertSPIResultToDuckStatementType(int result) {
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
		return duckdb::StatementType::INVALID_STATEMENT;
	}
}

static duckdb::unique_ptr<duckdb::MaterializedQueryResult>
CreateEmptyResult(duckdb::StatementType type) {
	duckdb::vector<duckdb::string> names;
	duckdb::StatementProperties properties;
	duckdb::ClientProperties client_properties;
	auto &allocator = duckdb::Allocator::DefaultAllocator();
	auto empty_collection = duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator);
	return duckdb::make_uniq<duckdb::MaterializedQueryResult>(type, properties, names, std::move(empty_collection),
	                                                          client_properties);
}

/*
 * SPI_finish() and PopActiveSnapshot() cannot ereport (spi.c, snapmgr.c), so these destructors
 * are safe to run while a C++ exception unwinds. Acquisition goes through PostgresFunctionGuard
 * because SPI_connect() and GetTransactionSnapshot() can.
 * Two objects rather than one: a destructor runs only for a fully constructed object, so a
 * failed snapshot push still releases the SPI connection.
 */
struct SpiConnectionScope {
	SpiConnectionScope() {
		PostgresFunctionGuard(SPI_connect);
	}
	~SpiConnectionScope() {
		SPI_finish();
	}
	SpiConnectionScope(const SpiConnectionScope &) = delete;
	SpiConnectionScope &operator=(const SpiConnectionScope &) = delete;
};

struct ActiveSnapshotScope {
	ActiveSnapshotScope() {
		Snapshot snapshot = PostgresFunctionGuard(GetTransactionSnapshot);
		PostgresFunctionGuard(PushActiveSnapshot, snapshot);
	}
	~ActiveSnapshotScope() {
		PopActiveSnapshot();
	}
	ActiveSnapshotScope(const ActiveSnapshotScope &) = delete;
	ActiveSnapshotScope &operator=(const ActiveSnapshotScope &) = delete;
};

/*
 * Catch PG ERRORs in a subtransaction: a bare longjmp catch leaks
 * ActiveSnapshot/executor resources. CurrentResourceOwner must be restored by
 * hand after release/rollback; the GUC nest level stays outside the subxact.
 */
static int
SPIExecuteInSubtransaction(const duckdb::string &query, bool &had_error, duckdb::string &error_message) {
	MemoryContext old_context = CurrentMemoryContext;
	ResourceOwner old_owner = CurrentResourceOwner;
	int ret = -1;
	had_error = false;

	/* GUC_ACTION_SAVE, not SetConfigOption()'s GUC_ACTION_SET: a metadata query can be issued while a
	 * libpgduckdb PostgresTableReader has put the backend in PG parallel mode, and guc.c rejects
	 * GUC_ACTION_SET there. SAVE is exempt because a parallel worker pops it too; PG itself relies on
	 * that in execute_extension_script(). */
	/* Suppress NOTICEs: DuckLake re-runs CREATE TABLE IF NOT EXISTS, whose NOTICE would leak to the client. */
	int save_nestlevel = NewGUCNestLevel();
	::set_config_option("client_min_messages", "warning", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

	SetAllowSubtransaction(true);
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(old_context);

	PG_TRY();
	{
		ret = SPI_execute(query.c_str(), false, 0);
		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(old_context);
		ErrorData *edata = CopyErrorData();
		error_message = edata->message;
		FreeErrorData(edata);
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		had_error = true;
	}
	PG_END_TRY();

	SetAllowSubtransaction(false);
	MemoryContextSwitchTo(old_context);
	CurrentResourceOwner = old_owner;

	AtEOXact_GUC(false, save_nestlevel);
	return ret;
}

/*
 * Every PG call that can ereport(ERROR) belongs here, not in the caller: the guard's sigsetjmp is
 * in its own frame, so a longjmp out of this function skips this frame entirely. Nothing here may
 * rely on a local destructor, and hoisting a PG call into the caller reopens the leak this split
 * exists to close.
 * Query passed by pointer: __PostgresFunctionGuard__ takes its arguments by value.
 */
static duckdb::unique_ptr<duckdb::QueryResult>
CreateSPIResultBody(const duckdb::string *query_ptr) {
	duckdb::string error_message;
	bool had_error = false;
	int ret = SPIExecuteInSubtransaction(*query_ptr, had_error, error_message);

	if (had_error) {
		duckdb::ErrorData error(duckdb::ExceptionType::IO, "SPI execution failed: " + error_message);
		return duckdb::make_uniq<duckdb::MaterializedQueryResult>(std::move(error));
	}

	if (ret < 0) {
		duckdb::ErrorData error(duckdb::ExceptionType::IO,
		                        "SPI execution failed: " + duckdb::string(SPI_result_code_string(ret)));
		return duckdb::make_uniq<duckdb::MaterializedQueryResult>(std::move(error));
	}

	SPITupleTable *tuptable = SPI_tuptable;
	if (!tuptable) {
		return CreateEmptyResult(ConvertSPIResultToDuckStatementType(ret));
	}

	TupleDesc tupdesc = tuptable->tupdesc;
	int num_columns = tupdesc->natts;
	uint64 num_rows = tuptable->numvals;

	duckdb::vector<duckdb::LogicalType> types;
	duckdb::vector<duckdb::string> names;

	for (int i = 0; i < num_columns; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		D_ASSERT(!attr->attisdropped);

		names.push_back(NameStr(attr->attname));

		types.push_back(pgddb::ConvertPostgresToDuckColumnType(attr));
	}

	duckdb::ClientProperties client_properties;
	auto &allocator = duckdb::Allocator::DefaultAllocator();
	auto collection_p = duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator, types);

	// Reusable chunk, append state, and per-column append fn table so the loop below allocates nothing.
	duckdb::DataChunk chunk;
	chunk.Initialize(allocator, types, STANDARD_VECTOR_SIZE);
	duckdb::ColumnDataAppendState append_state;
	collection_p->InitializeAppend(append_state);

	auto values = (Datum *)palloc(num_columns * sizeof(Datum));
	auto deform_nulls = (bool *)palloc(num_columns * sizeof(bool));
	auto column_append = (pgddb::PostgresToDuckValueFn *)palloc(num_columns * sizeof(pgddb::PostgresToDuckValueFn));
	for (int i = 0; i < num_columns; i++) {
		column_append[i] = pgddb::GetPostgresToDuckValueFn(TupleDescAttr(tupdesc, i)->atttypid, chunk.data[i]);
	}

	for (idx_t row_idx = 0; row_idx < num_rows; row_idx += STANDARD_VECTOR_SIZE) {
		idx_t chunk_size = duckdb::MinValue<idx_t>(STANDARD_VECTOR_SIZE, num_rows - row_idx);
		chunk.Reset();
		for (idx_t row = 0; row < chunk_size; row++) {
			HeapTuple tuple = tuptable->vals[row_idx + row];
			heap_deform_tuple(tuple, tupdesc, values, deform_nulls);
			for (int col = 0; col < num_columns; col++) {
				auto &result = chunk.data[col];
				if (deform_nulls[col]) {
					duckdb::FlatVector::Validity(result).SetInvalid(row);
				} else {
					column_append[col](result, values[col], row);
				}
			}
		}
		chunk.SetCardinality(chunk_size);
		collection_p->Append(append_state, chunk);
	}

	duckdb::StatementProperties properties;
	return duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::StatementType::SELECT_STATEMENT, properties,
	                                                          names, std::move(collection_p), client_properties);
}

static duckdb::unique_ptr<duckdb::QueryResult>
CreateSPIResult(const duckdb::string &query) {
	elog(DEBUG1, "Creating SPI result for query: %s", query.c_str());

	std::lock_guard<std::recursive_mutex> lock(pgddb::GlobalProcessLock::GetLock());
	pgddb::PostgresScopedStackReset scoped_stack_reset;

	try {
		SpiConnectionScope spi;
		ActiveSnapshotScope snapshot;
		return PostgresFunctionGuard(CreateSPIResultBody, &query);
	} catch (const std::exception &ex) {
		/* Repackage rather than propagate: DuckLake's metadata manager treats an error-carrying
		 * QueryResult as a value and branches on it. */
		return duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::ErrorData(ex));
	}
}

/* Avoids transaction.GetCatalog(): during init the AttachedDatabase is not yet reachable via db_manager. */
static void
SubstitutePgCatalogPlaceholders(duckdb::string &query) {
	query = duckdb::StringUtil::Replace(query, "{METADATA_CATALOG}", "\"" PGDUCKLAKE_PG_SCHEMA "\"");
	query =
	    duckdb::StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}", "\"" PGDUCKLAKE_DUCKDB_CATALOG "\"");
	query = duckdb::StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}", "'" PGDUCKLAKE_DUCKDB_CATALOG "'");
	query = duckdb::StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}", "'" PGDUCKLAKE_PG_SCHEMA "'");
	query = duckdb::StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}", "\"" PGDUCKLAKE_PG_SCHEMA "\"");
}

/*
 * Below the guard frame -- see CreateSPIResultBody.
 * Failures become duckdb::TransactionException so DuckLake's FlushChanges() retry loop can
 * intercept unique-violations from concurrent commits; a C++ throw from here propagates through
 * the guard unchanged.
 */
static duckdb::unique_ptr<duckdb::QueryResult>
CreateSPIExecuteInSubtransactionBody(const duckdb::string *query_ptr) {
	duckdb::string error_message;
	bool had_error = false;
	int ret = SPIExecuteInSubtransaction(*query_ptr, had_error, error_message);

	if (!had_error && ret < 0) {
		error_message = duckdb::string("SPI execute failed: ") + SPI_result_code_string(ret);
		had_error = true;
	}

	if (had_error) {
		throw duckdb::TransactionException("%s", error_message.c_str());
	}

	return CreateEmptyResult(duckdb::StatementType::EXECUTE_STATEMENT);
}

static duckdb::unique_ptr<duckdb::QueryResult>
CreateSPIExecuteInSubtransaction(const duckdb::string &query) {
	elog(DEBUG1, "CreateSPIExecuteInSubtransaction: %s", query.c_str());

	std::lock_guard<std::recursive_mutex> lock(pgddb::GlobalProcessLock::GetLock());
	pgddb::PostgresScopedStackReset scoped_stack_reset;

	SpiConnectionScope spi;
	// PRE_COMMIT of a pipelined implicit txn (extended protocol) has no active snapshot; SPI needs one pushed.
	ActiveSnapshotScope snapshot;

	return PostgresFunctionGuard(CreateSPIExecuteInSubtransactionBody, &query);
}

PgDuckLakeMetadataManager::PgDuckLakeMetadataManager(duckdb::DuckLakeTransaction &transaction_)
    : duckdb::PostgresMetadataManager(transaction_) {
}

PgDuckLakeMetadataManager::~PgDuckLakeMetadataManager() {
}

/* find()-guarded: GetCatalog() is unsafe during init, and these placeholders never appear in init queries. */
static void
SubstitutePathPlaceholders(duckdb::string &query, duckdb::DuckLakeTransaction &transaction) {
	if (query.find("{DATA_PATH}") == duckdb::string::npos && query.find("{METADATA_PATH}") == duckdb::string::npos) {
		return;
	}
	auto &catalog = transaction.GetCatalog();
	query =
	    duckdb::StringUtil::Replace(query, "{DATA_PATH}", duckdb::DuckLakeUtil::SQLLiteralToString(catalog.DataPath()));
	query = duckdb::StringUtil::Replace(query, "{METADATA_PATH}",
	                                    duckdb::DuckLakeUtil::SQLLiteralToString(catalog.MetadataPath()));
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Query(duckdb::string query) {
	SubstitutePathPlaceholders(query, transaction);
	SubstitutePgCatalogPlaceholders(query);
	return CreateSPIResult(query);
}

/* Mirrors the static GetProjection() in ducklake_metadata_manager.cpp. */
static duckdb::string
BuildProjection(const duckdb::vector<duckdb::string> &columns_to_read) {
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

/* Route through DuckDB, not SPI: PostgresTableReader holds GlobalProcessLock per 32-tuple batch, not whole op. */
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
	return transaction.ExecuteRaw(query);
}

/* Same DuckDB routing as ReadInlinedData, but keeps deleted rows (no end_snapshot filter) for deletion vectors.
 * Unlike ReadInlinedData (which filters to the single live version per row_id), this read keeps ALL versions,
 * so a row_id can appear multiple times. The flush's delete-position query derives ordinals from
 * ROW_NUMBER() OVER (ORDER BY row_id, begin_snapshot) - the physical read order MUST match that, or the
 * positional delete file tombstones the wrong version and the latest value silently reverts. Over a Postgres
 * heap scan, ORDER BY row_id alone leaves version ties to physical/TID order, not begin_snapshot - so the
 * begin_snapshot tiebreaker is required here (mirrors upstream ducklake 8dd38ce0). */
duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::ReadAllInlinedDataForFlush(duckdb::DuckLakeSnapshot snapshot,
                                                      const duckdb::string &inlined_table_name,
                                                      const duckdb::vector<duckdb::string> &columns_to_read) {
	auto projection = BuildProjection(columns_to_read);
	auto query = duckdb::StringUtil::Format(R"(
SELECT %s
FROM pgduckdb."%s".%s inlined_data
WHERE %llu >= begin_snapshot
ORDER BY row_id, begin_snapshot;)",
	                                        projection, PGDUCKLAKE_PG_SCHEMA, duckdb::SQLIdentifier(inlined_table_name),
	                                        (unsigned long long)snapshot.snapshot_id);
	elog(DEBUG1, "ReadAllInlinedDataForFlush via DuckDB: %s", query.c_str());
	return transaction.ExecuteRaw(query);
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Query(duckdb::DuckLakeSnapshot snapshot, duckdb::string query) {
	DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
	return Query(query);
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Execute(duckdb::string query) {
	SubstitutePathPlaceholders(query, transaction);
	SubstitutePgCatalogPlaceholders(query);
	return CreateSPIResult(query);
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Execute(duckdb::DuckLakeSnapshot snapshot, duckdb::string query) {
	DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
	return Execute(query);
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::ExecuteCommit(duckdb::DuckLakeSnapshot snapshot, duckdb::string query) {
	DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
	SubstitutePgCatalogPlaceholders(query);
	/* Skip the snapshot sync trigger: nothing to reverse-sync, and it crashes on a DuckDB worker thread
	 * (PG's InterruptHoldoffCount is not thread-safe). */
	SkipSnapshotSyncGuard sync_guard;
	return CreateSPIExecuteInSubtransaction(query);
}

bool
PgDuckLakeMetadataManager::IsInitialized() {

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

		if (strncmp(relname, "ducklake_", 9) == 0 && classForm->relkind == RELKIND_RELATION) {
			found = true;
			break;
		}
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return found;
}

/* Below the guard frame -- see CreateSPIResultBody. */
static void
EnsureSnapshotTriggerBody() {
	auto save_nestlevel = NewGUCNestLevel();
	::SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);

	duckdb::string error_message;
	bool had_error = false;
	int ret = SPIExecuteInSubtransaction(R"(
		SELECT 1 FROM pg_trigger t
		JOIN pg_class c ON t.tgrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		WHERE n.nspname = 'ducklake'
		  AND c.relname = 'ducklake_snapshot'
		  AND t.tgname = 'ducklake_snapshot_sync_trigger'
		)",
	                                     had_error, error_message);

	if (!had_error && ret == SPI_OK_SELECT && SPI_processed == 0) {
		// OR REPLACE: two backends can race the probe; the loser must not error on the duplicate trigger.
		ret = SPIExecuteInSubtransaction(R"(
		CREATE OR REPLACE TRIGGER ducklake_snapshot_sync_trigger
		AFTER INSERT ON ducklake.ducklake_snapshot
		FOR EACH ROW
		EXECUTE FUNCTION ducklake._snapshot_trigger()
		)",
		                                 had_error, error_message);
	}

	AtEOXact_GUC(false, save_nestlevel);

	if (had_error || ret < 0) {
		if (!had_error) {
			error_message = SPI_result_code_string(ret);
		}
		throw duckdb::IOException("EnsureSnapshotTrigger failed: %s", error_message.c_str());
	}
}

/* Raw SPI: runs inside DuckDB's ATTACH path, where re-entering DuckDB would recurse infinitely. */
void
PgDuckLakeMetadataManager::EnsureSnapshotTrigger() {
	std::lock_guard<std::recursive_mutex> lock(pgddb::GlobalProcessLock::GetLock());
	pgddb::PostgresScopedStackReset scoped_stack_reset;

	SpiConnectionScope spi;
	ActiveSnapshotScope snapshot;

	/* No repackaging: this function already reports failure by throwing, so the guard's exception
	 * can propagate as-is. */
	PostgresFunctionGuard(EnsureSnapshotTriggerBody);
}

bool
PgDuckLakeMetadataManager::MetadataExists() {
	// Base MetadataExists probes ducklake_metadata, aborting the PG txn when absent; scan pg_class instead.
	bool initialized = IsInitialized();
	if (initialized)
		EnsureSnapshotTrigger();
	return initialized;
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::AttachMetadata(const duckdb::string & /*attach_query*/) {
	// Metadata lives in PG via SPI, nothing to ATTACH; return empty success so Initialize() reaches MetadataExists().
	return CreateEmptyResult(duckdb::StatementType::SELECT_STATEMENT);
}

void
PgDuckLakeMetadataManager::InitializeDuckLake(bool has_explicit_schema, duckdb::DuckLakeEncryption encryption) {
	DuckLakeMetadataManager::InitializeDuckLake(has_explicit_schema, encryption);
	EnsureSnapshotTrigger();
}

duckdb::string
PgDuckLakeMetadataManager::GetInlinedTableQueries(duckdb::DuckLakeSnapshot commit_snapshot,
                                                  const duckdb::DuckLakeTableInfo &table,
                                                  duckdb::string &inlined_tables,
                                                  duckdb::string &inlined_table_queries) {
	auto table_name =
	    DuckLakeMetadataManager::GetInlinedTableQueries(commit_snapshot, table, inlined_tables, inlined_table_queries);

	// Grant predefined roles so SPI metadata queries succeed regardless of who created the inlined data table.
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

duckdb::string
PgDuckLakeMetadataManager::GenerateFileColumnStatsCTEBody(const duckdb::CTERequirement &req,
                                                          duckdb::TableIndex table_id) {
	// Plain-SQL form runs directly under SPI; the base wraps it in postgres_query(), not a real PG function.
	return DuckLakeMetadataManager::GenerateFileColumnStatsCTEBody(req, table_id);
}

TableInliningState
GetTableInliningState(Oid table_oid, uint64_t *table_id_out, uint64_t *schema_version_out, int64_t *row_limit_out) {
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
	char *table_name = pstrdup(NameStr(reltup->relname));
	Oid schema_oid = reltup->relnamespace;
	ReleaseSysCache(tp);

	HeapTuple ntp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(schema_oid));
	if (!HeapTupleIsValid(ntp)) {
		SPI_finish();
		return TI_NO_TABLE;
	}

	Form_pg_namespace nstup = (Form_pg_namespace)GETSTRUCT(ntp);
	char *schema_name = pstrdup(NameStr(nstup->nspname));
	ReleaseSysCache(ntp);

	/* Schema-bumping DDL keeps the old inlined-data row, so read the MAX(schema_version) one. */
	// Names go as query parameters, not interpolated: they are data values here and may contain quotes.
	const char *query = "SELECT dt.table_id, "
	                    "       (SELECT MAX(idt.schema_version) "
	                    "        FROM ducklake.ducklake_inlined_data_tables idt "
	                    "        WHERE idt.table_id = dt.table_id), "
	                    "       (SELECT m.value::bigint "
	                    "        FROM ducklake.ducklake_metadata m "
	                    "        WHERE m.key = 'data_inlining_row_limit' "
	                    "        AND m.scope IS NULL) "
	                    "FROM ducklake.ducklake_table dt "
	                    "JOIN ducklake.ducklake_schema ds ON dt.schema_id = ds.schema_id "
	                    "WHERE dt.table_name = $1 "
	                    "AND ds.schema_name = $2 "
	                    "AND dt.end_snapshot IS NULL "
	                    "AND ds.end_snapshot IS NULL "
	                    "LIMIT 1";
	Oid arg_types[2] = {TEXTOID, TEXTOID};
	Datum arg_values[2] = {CStringGetTextDatum(table_name), CStringGetTextDatum(schema_name)};

	ret = SPI_execute_with_args(query, 2, arg_types, arg_values, NULL, true, 1);
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

		/* col 1: MAX inlined schema_version (NULL if no inlined_data_tables row) */
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

bool
GetTableInliningInfo(Oid table_oid, uint64_t *table_id_out, uint64_t *schema_version_out) {
	return GetTableInliningState(table_oid, table_id_out, schema_version_out, NULL) == TI_OK;
}

uint64_t
GetNextRowIdForTable(uint64_t table_id, uint64_t schema_version) {
	int ret;
	uint64_t next_row_id = 0;

	if ((ret = SPI_connect()) < 0) {
		elog(ERROR, "SPI_connect failed: %d", ret);
		return 0;
	}

	/* Fall back to MAX(row_id) + 1 when no stats row exists yet (first insert). */
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

uint64_t
GetNextSnapshotId() {
	int ret;
	uint64_t next_snapshot_id = 1;

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

namespace {

/* How the inlined column yields DuckLake min/max values. */
enum class BoundsKind {
	NONE,     // bounds not maintained
	ORDERED,  // native PG ordering matches DuckDB's
	STRING,   // bytea compared bytewise, as DuckDB compares strings
	FLOATING, // NaN tracked as contains_nan, not as a bound
	CAST,     // varchar holding DuckDB text, ordered via a PG cast
};

struct DirectInsertColumnStats {
	uint64_t column_id;
	std::string column_name;
	std::string column_type;
	BoundsKind kind;
	const char *order_cast;

	bool observed_null = false;
	bool observed_nan = false;
	bool has_value = false;
	std::string min_value;
	std::string max_value;

	duckdb::DuckLakeGlobalColumnStatsInfo stats;
};

void
SetBoundsKind(DirectInsertColumnStats &col) {
	col.kind = BoundsKind::NONE;
	col.order_cast = nullptr;
	duckdb::LogicalType type;
	try {
		type = duckdb::DuckLakeTypes::FromString(col.column_type);
	} catch (...) {
		return;
	}
	switch (type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
	case duckdb::LogicalTypeId::TINYINT:
	case duckdb::LogicalTypeId::SMALLINT:
	case duckdb::LogicalTypeId::INTEGER:
	case duckdb::LogicalTypeId::BIGINT:
	case duckdb::LogicalTypeId::UTINYINT:
	case duckdb::LogicalTypeId::USMALLINT:
	case duckdb::LogicalTypeId::UINTEGER:
	case duckdb::LogicalTypeId::DECIMAL:
	case duckdb::LogicalTypeId::TIME:
	case duckdb::LogicalTypeId::UUID:
		col.kind = BoundsKind::ORDERED;
		break;
	case duckdb::LogicalTypeId::VARCHAR:
		col.kind = BoundsKind::STRING;
		break;
	case duckdb::LogicalTypeId::FLOAT:
	case duckdb::LogicalTypeId::DOUBLE:
		col.kind = BoundsKind::FLOATING;
		break;
	case duckdb::LogicalTypeId::UBIGINT:
	case duckdb::LogicalTypeId::HUGEINT:
	case duckdb::LogicalTypeId::UHUGEINT:
		col.kind = BoundsKind::CAST;
		col.order_cast = "numeric";
		break;
	case duckdb::LogicalTypeId::DATE:
		col.kind = BoundsKind::CAST;
		col.order_cast = "date";
		break;
	case duckdb::LogicalTypeId::TIMESTAMP:
	case duckdb::LogicalTypeId::TIMESTAMP_SEC:
	case duckdb::LogicalTypeId::TIMESTAMP_MS:
	case duckdb::LogicalTypeId::TIMESTAMP_NS:
		col.kind = BoundsKind::CAST;
		col.order_cast = "timestamp";
		break;
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		col.kind = BoundsKind::CAST;
		col.order_cast = "timestamptz";
		break;
	default:
		break;
	}
}

/* Merge one column's contribution with DuckLake's own rules; makes no PG calls. */
void
MergeDirectInsertColumnStats(DirectInsertColumnStats &col, bool was_empty) {
	auto &stats = col.stats;
	bool seed_bounds = was_empty || stats.has_contains_null;
	stats.has_contains_null = was_empty || stats.has_contains_null;
	stats.contains_null = stats.contains_null || col.observed_null;
	if (col.kind == BoundsKind::FLOATING) {
		stats.has_contains_nan = was_empty || stats.has_contains_nan;
		stats.contains_nan = stats.contains_nan || col.observed_nan;
	}
	if (col.kind == BoundsKind::NONE) {
		/* No safe bounds for this type: mark everything unknown. */
		stats.has_contains_null = stats.has_contains_nan = stats.has_min = stats.has_max = false;
		return;
	}
	if (!col.has_value) {
		return;
	}

	try {
		auto type = duckdb::DuckLakeTypes::FromString(col.column_type);
		auto current = duckdb::DuckLakeColumnStats::FromGlobalStats(type, stats);
		duckdb::DuckLakeColumnStats contribution(type);
		contribution.min = duckdb::Value(col.min_value).DefaultCastAs(type).ToString();
		contribution.max = duckdb::Value(col.max_value).DefaultCastAs(type).ToString();
		contribution.has_min = contribution.has_max = contribution.any_valid = true;
		current.MergeStats(contribution);

		/* Persisted NULL bounds with unknown contains_null are unknown, not "no values yet". */
		stats.has_min = (seed_bounds || stats.has_min) && current.has_min;
		stats.has_max = (seed_bounds || stats.has_max) && current.has_max;
		stats.min_val = current.min;
		stats.max_val = current.max;
	} catch (...) {
		stats.has_min = stats.has_max = false;
	}
}

/* Fold the rows a direct insert just inlined into ducklake_table_column_stats. Runs inside SPI. */
void
UpdateColumnStatsForDirectInsert(uint64_t snapshot_id, uint64_t table_id, int64_t rows_inserted) {
	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query, "SELECT record_count FROM ducklake.ducklake_table_stats WHERE table_id = %llu",
	                 (unsigned long long)table_id);
	if (SPI_execute(query.data, false, 1) != SPI_OK_SELECT || SPI_processed != 1) {
		elog(ERROR, "CreateSnapshotForDirectInsert: failed to read table stats");
	}
	bool isnull;
	/* All-NULL stats of an empty table mean "no values yet"; otherwise they mean unknown. */
	bool was_empty =
	    DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull)) == rows_inserted;

	resetStringInfo(&query);
	appendStringInfo(&query, R"(
SELECT c.column_id, c.column_name, c.column_type,
       s.contains_null, s.contains_nan, s.min_value, s.max_value
FROM ducklake.ducklake_column c
JOIN ducklake.ducklake_table_column_stats s ON s.table_id = c.table_id AND s.column_id = c.column_id
WHERE c.table_id = %llu AND c.end_snapshot IS NULL AND c.parent_column IS NULL
ORDER BY c.column_order)",
	                 (unsigned long long)table_id);
	if (SPI_execute(query.data, false, 0) != SPI_OK_SELECT) {
		elog(ERROR, "CreateSnapshotForDirectInsert: failed to read column stats");
	}

	std::vector<DirectInsertColumnStats> cols(SPI_processed);
	for (uint64 i = 0; i < SPI_processed; i++) {
		HeapTuple tuple = SPI_tuptable->vals[i];
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		auto &col = cols[i];
		bool isnull;
		col.column_id = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 1, &isnull));
		col.column_name = SPI_getvalue(tuple, tupdesc, 2);
		col.column_type = SPI_getvalue(tuple, tupdesc, 3);
		Datum d = SPI_getbinval(tuple, tupdesc, 4, &isnull);
		col.stats.has_contains_null = !isnull;
		col.stats.contains_null = !isnull && DatumGetBool(d);
		d = SPI_getbinval(tuple, tupdesc, 5, &isnull);
		col.stats.has_contains_nan = !isnull;
		col.stats.contains_nan = !isnull && DatumGetBool(d);
		char *min_value = SPI_getvalue(tuple, tupdesc, 6);
		char *max_value = SPI_getvalue(tuple, tupdesc, 7);
		col.stats.has_min = min_value != nullptr;
		col.stats.min_val = min_value ? min_value : "";
		col.stats.has_max = max_value != nullptr;
		col.stats.max_val = max_value ? max_value : "";
		SetBoundsKind(col);
	}
	if (cols.empty()) {
		return;
	}

	resetStringInfo(&query);
	appendStringInfo(&query, R"(
SELECT table_name FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = %llu ORDER BY schema_version DESC LIMIT 1)",
	                 (unsigned long long)table_id);
	if (SPI_execute(query.data, false, 1) != SPI_OK_SELECT || SPI_processed != 1) {
		elog(ERROR, "CreateSnapshotForDirectInsert: inlined data table not found for table %llu",
		     (unsigned long long)table_id);
	}
	std::string inlined_table = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

	/* Per column: has NULL, min, max, has NaN. */
	resetStringInfo(&query);
	appendStringInfoString(&query, "SELECT ");
	for (size_t i = 0; i < cols.size(); i++) {
		auto &col = cols[i];
		const char *c = quote_identifier(col.column_name.c_str());
		std::string not_null = std::string(c) + " IS NOT NULL";
		std::string key = c;
		std::string value = c;
		const char *out = "::text";
		if (col.kind == BoundsKind::FLOATING) {
			not_null += std::string(" AND ") + c + " <> 'NaN'";
			out = "::float8";
		} else if (col.kind == BoundsKind::CAST) {
			key = std::string(c) + "::" + col.order_cast;
		}
		auto pick = [&](const char *dir) {
			std::string v =
			    "(array_agg(" + value + " ORDER BY " + key + " " + dir + ") FILTER (WHERE " + not_null + "))[1]";
			return col.kind == BoundsKind::STRING ? "convert_from(" + v + ", 'UTF8')" : v + out;
		};
		appendStringInfo(&query, "%sbool_or(%s IS NULL), %s, %s, %s", i ? ", " : "", c,
		                 col.kind == BoundsKind::NONE ? "NULL" : pick("ASC").c_str(),
		                 col.kind == BoundsKind::NONE ? "NULL" : pick("DESC").c_str(),
		                 col.kind == BoundsKind::FLOATING ? (std::string("bool_or(") + c + " = 'NaN')").c_str()
		                                                  : "NULL");
	}
	appendStringInfo(&query, " FROM ducklake.%s WHERE begin_snapshot = %llu", quote_identifier(inlined_table.c_str()),
	                 (unsigned long long)snapshot_id);
	if (SPI_execute(query.data, false, 1) != SPI_OK_SELECT || SPI_processed != 1) {
		elog(ERROR, "CreateSnapshotForDirectInsert: failed to compute column stats");
	}

	HeapTuple tuple = SPI_tuptable->vals[0];
	TupleDesc tupdesc = SPI_tuptable->tupdesc;
	for (size_t i = 0; i < cols.size(); i++) {
		auto &col = cols[i];
		int base = (int)i * 4;
		bool isnull;
		Datum d = SPI_getbinval(tuple, tupdesc, base + 1, &isnull);
		col.observed_null = !isnull && DatumGetBool(d);
		d = SPI_getbinval(tuple, tupdesc, base + 4, &isnull);
		col.observed_nan = !isnull && DatumGetBool(d);
		if (col.kind == BoundsKind::FLOATING) {
			Datum min_d = SPI_getbinval(tuple, tupdesc, base + 2, &isnull);
			if (!isnull) {
				Datum max_d = SPI_getbinval(tuple, tupdesc, base + 3, &isnull);
				char buf[64];
				snprintf(buf, sizeof(buf), "%.17g", DatumGetFloat8(min_d));
				col.min_value = buf;
				snprintf(buf, sizeof(buf), "%.17g", DatumGetFloat8(max_d));
				col.max_value = buf;
				col.has_value = true;
			}
		} else {
			char *min_value = SPI_getvalue(tuple, tupdesc, base + 2);
			char *max_value = SPI_getvalue(tuple, tupdesc, base + 3);
			if (min_value && max_value) {
				col.min_value = min_value;
				col.max_value = max_value;
				col.has_value = true;
			}
		}
	}

	for (auto &col : cols) {
		MergeDirectInsertColumnStats(col, was_empty);
	}

	for (auto &col : cols) {
		auto &stats = col.stats;
		Oid argtypes[6] = {INT8OID, INT8OID, BOOLOID, BOOLOID, TEXTOID, TEXTOID};
		Datum values[6] = {Int64GetDatum((int64)table_id),
		                   Int64GetDatum((int64)col.column_id),
		                   BoolGetDatum(stats.contains_null),
		                   BoolGetDatum(stats.contains_nan),
		                   stats.has_min ? CStringGetTextDatum(stats.min_val.c_str()) : (Datum)0,
		                   stats.has_max ? CStringGetTextDatum(stats.max_val.c_str()) : (Datum)0};
		char nulls[6] = {' ',
		                 ' ',
		                 stats.has_contains_null ? ' ' : 'n',
		                 stats.has_contains_nan ? ' ' : 'n',
		                 stats.has_min ? ' ' : 'n',
		                 stats.has_max ? ' ' : 'n'};
		int ret = SPI_execute_with_args(R"(
UPDATE ducklake.ducklake_table_column_stats
SET contains_null = $3, contains_nan = $4, min_value = $5, max_value = $6
WHERE table_id = $1 AND column_id = $2)",
		                                6, argtypes, values, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			elog(ERROR, "CreateSnapshotForDirectInsert: failed to update column stats: %d", ret);
		}
	}
}

} // namespace

void
CreateSnapshotForDirectInsert(uint64_t snapshot_id, uint64_t table_id, int64_t rows_inserted) {
	int ret;

	elog(DEBUG1, "CreateSnapshotForDirectInsert: creating snapshot %llu", (unsigned long long)snapshot_id);

	if ((ret = SPI_connect()) < 0) {
		elog(ERROR, "CreateSnapshotForDirectInsert: SPI_connect failed: %d", ret);
		return;
	}

	/* Carry the latest schema_version forward: direct insert is data-only, and bumping it would roll
	 * back the global catalog view and hide tables created after this one. */
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

	/* next_file_id + 1 mirrors upstream's inlined-only commits: DuckLake keys
	 * its table-stats cache on (next_file_id, schema_version, table_id), so a
	 * data change that does not advance it leaves stale cached stats (and a
	 * later normal-path commit would reuse row ids from the stale next_row_id). */
	StringInfoData snapshot_insert;
	initStringInfo(&snapshot_insert);
	appendStringInfo(&snapshot_insert,
	                 "INSERT INTO ducklake.ducklake_snapshot "
	                 "(snapshot_id, snapshot_time, schema_version, next_catalog_id, "
	                 "next_file_id) "
	                 "VALUES (%llu, NOW(), %llu, %llu, %llu)",
	                 (unsigned long long)snapshot_id, (unsigned long long)schema_version,
	                 (unsigned long long)next_catalog_id, (unsigned long long)(next_file_id + 1));

	elog(DEBUG1, "CreateSnapshotForDirectInsert: executing %s", snapshot_insert.data);
	ret = SPI_execute(snapshot_insert.data, false, 0);
	if (ret != SPI_OK_INSERT) {
		elog(ERROR, "CreateSnapshotForDirectInsert: failed to insert snapshot: %d", ret);
	}

	/* Must be spelled 'inlined_insert:<table_id>': DuckLake's ParseChangesMade
	 * rejects unknown change types, aborting any concurrent commit retry. */
	StringInfoData changes_insert;
	initStringInfo(&changes_insert);
	appendStringInfo(&changes_insert,
	                 "INSERT INTO ducklake.ducklake_snapshot_changes "
	                 "(snapshot_id, changes_made, author, commit_message, commit_extra_info) "
	                 "VALUES (%llu, 'inlined_insert:%llu', NULL, NULL, NULL)",
	                 (unsigned long long)snapshot_id, (unsigned long long)table_id);

	elog(DEBUG1, "CreateSnapshotForDirectInsert: executing %s", changes_insert.data);
	ret = SPI_execute(changes_insert.data, false, 0);
	if (ret != SPI_OK_INSERT) {
		elog(ERROR, "CreateSnapshotForDirectInsert: failed to insert snapshot changes: %d", ret);
	}

	/* A new stats row must also populate ducklake_table_column_stats:
	 * TransformGlobalStatsRow reads the LEFT JOINed column_id with no null check. */
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
		elog(ERROR, "CreateSnapshotForDirectInsert: failed to update table stats: %d", ret);
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
			elog(ERROR, "CreateSnapshotForDirectInsert: failed to insert table stats: %d", ret);
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
			elog(ERROR, "CreateSnapshotForDirectInsert: failed to insert column stats: %d", ret);
		}

		elog(DEBUG1, "CreateSnapshotForDirectInsert: created new stats row for table %llu",
		     (unsigned long long)table_id);
	}
	UpdateColumnStatsForDirectInsert(snapshot_id, table_id, rows_inserted);

	SPI_finish();
	elog(DEBUG1, "CreateSnapshotForDirectInsert: successfully created snapshot %llu", (unsigned long long)snapshot_id);
}

} // namespace pgducklake
