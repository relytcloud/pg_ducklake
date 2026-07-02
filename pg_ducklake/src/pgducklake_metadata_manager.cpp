/* PostgreSQL-backed DuckLake metadata manager: DuckDB metadata requests -> SQL on ducklake_* tables. */

#include "pgducklake/catalog_sync.hpp"
#include "pgducklake/constants.hpp"
#include "pgducklake/duckdb_manager.hpp"
#include "pgducklake/guc.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"

#include <cstring>

#include "pgddb/pgddb_types.hpp"
#include "pgddb/pgddb_utils.hpp"

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

	/* Suppress NOTICEs: DuckLake re-runs CREATE TABLE IF NOT EXISTS, whose NOTICE would leak to the client. */
	int save_nestlevel = NewGUCNestLevel();
	::SetConfigOption("client_min_messages", "warning", PGC_USERSET, PGC_S_SESSION);

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

static duckdb::unique_ptr<duckdb::QueryResult>
CreateSPIResult(const duckdb::string &query) {
	elog(DEBUG1, "Creating SPI result for query: %s", query.c_str());

	std::lock_guard<std::recursive_mutex> lock(pgddb::GlobalProcessLock::GetLock());
	pgddb::PostgresScopedStackReset scoped_stack_reset;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	duckdb::string error_message;
	bool had_error = false;
	int ret = SPIExecuteInSubtransaction(query, had_error, error_message);

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

	SPITupleTable *tuptable = SPI_tuptable;
	if (!tuptable) {
		PopActiveSnapshot();
		SPI_finish();
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

	PopActiveSnapshot();
	SPI_finish();

	duckdb::StatementProperties properties;
	return duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::StatementType::SELECT_STATEMENT, properties,
	                                                          names, std::move(collection_p), client_properties);
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
 * Convert PG ERRORs into duckdb::TransactionException so DuckLake's
 * FlushChanges() retry loop can intercept unique-violations from concurrent
 * commits instead of a PG longjmp bypassing the C++ catch.
 */
static duckdb::unique_ptr<duckdb::QueryResult>
CreateSPIExecuteInSubtransaction(const duckdb::string &query) {
	elog(DEBUG1, "CreateSPIExecuteInSubtransaction: %s", query.c_str());

	std::lock_guard<std::recursive_mutex> lock(pgddb::GlobalProcessLock::GetLock());
	pgddb::PostgresScopedStackReset scoped_stack_reset;

	SPI_connect();
	// PRE_COMMIT of a pipelined implicit txn (extended protocol) has no active snapshot; SPI needs one pushed.
	PushActiveSnapshot(GetTransactionSnapshot());

	duckdb::string error_message;
	bool had_error = false;
	int ret = SPIExecuteInSubtransaction(query, had_error, error_message);

	PopActiveSnapshot();

	if (!had_error && ret < 0) {
		error_message = duckdb::string("SPI execute failed: ") + SPI_result_code_string(ret);
		had_error = true;
	}

	SPI_finish();

	if (had_error) {
		throw duckdb::TransactionException("%s", error_message.c_str());
	}

	return CreateEmptyResult(duckdb::StatementType::EXECUTE_STATEMENT);
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

/* Same DuckDB routing as ReadInlinedData, but keeps deleted rows (no end_snapshot filter) for deletion vectors. */
duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::ReadAllInlinedDataForFlush(duckdb::DuckLakeSnapshot snapshot,
                                                      const duckdb::string &inlined_table_name,
                                                      const duckdb::vector<duckdb::string> &columns_to_read) {
	auto projection = BuildProjection(columns_to_read);
	auto query = duckdb::StringUtil::Format(R"(
SELECT %s
FROM pgduckdb."%s".%s inlined_data
WHERE %llu >= begin_snapshot
ORDER BY row_id;)",
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

/* Raw SPI: runs inside DuckDB's ATTACH path, where re-entering DuckDB would recurse infinitely. */
void
PgDuckLakeMetadataManager::EnsureSnapshotTrigger() {
	std::lock_guard<std::recursive_mutex> lock(pgddb::GlobalProcessLock::GetLock());
	pgddb::PostgresScopedStackReset scoped_stack_reset;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	auto save_nestlevel = NewGUCNestLevel();
	::SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);

	// SPIExecuteInSubtransaction keeps a PG ERROR from longjmp-ing past the held lock_guard (skips C++ dtors).
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
	PopActiveSnapshot();
	SPI_finish();

	if (had_error || ret < 0) {
		if (!had_error) {
			error_message = SPI_result_code_string(ret);
		}
		throw duckdb::IOException("EnsureSnapshotTrigger failed: %s", error_message.c_str());
	}
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

	StringInfoData snapshot_insert;
	initStringInfo(&snapshot_insert);
	appendStringInfo(&snapshot_insert,
	                 "INSERT INTO ducklake.ducklake_snapshot "
	                 "(snapshot_id, snapshot_time, schema_version, next_catalog_id, "
	                 "next_file_id) "
	                 "VALUES (%llu, NOW(), %llu, %llu, %llu)",
	                 (unsigned long long)snapshot_id, (unsigned long long)schema_version,
	                 (unsigned long long)next_catalog_id, (unsigned long long)next_file_id);

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

	SPI_finish();
	elog(DEBUG1, "CreateSnapshotForDirectInsert: successfully created snapshot %llu", (unsigned long long)snapshot_id);
}

} // namespace pgducklake
