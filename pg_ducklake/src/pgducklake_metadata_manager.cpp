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
#include <duckdb/parser/keyword_helper.hpp>
#include <storage/ducklake_partition_data.hpp>
#include <storage/ducklake_table_entry.hpp>

extern "C" {
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
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
	/* DuckLake-generated SQL calls DuckDB-dialect functions (month(), murmur3_32(), ...) unqualified;
	 * resolve them to the ducklake-schema UDFs deterministically, independent of the caller's search_path. */
	::set_config_option("search_path", "ducklake", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

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

/*
 * The flush's deleted-rows filter runs over SPI against the inlined heap table, so each partition
 * column must be rendered as PG SQL that recovers the DuckLake-typed value from its storage type:
 *  - VARCHAR is stored as BYTEA; the upstream CAST(col AS VARCHAR) would yield PG's hex form
 *    ('\x6170706c65'), silently mis-hashing every bucket/identity comparison. convert_from()
 *    restores the original string.
 *  - BLOB is stored as BYTEA; pass it through raw (murmur3_32(bytea) hashes the bytes directly,
 *    matching DuckDB, which hashes a BLOB's raw bytes).
 *  - Types stored as VARCHAR (date/timestamp family) keep the upstream CAST to their DuckDB type
 *    name, which PG parses from the stored text form.
 *  - Native types cast via GetColumnTypeInternal so the type name is PG-parseable (e.g. DuckDB
 *    DOUBLE -> DOUBLE PRECISION).
 * The transform wrapper (month(...), (murmur3_32(...) & ...) % N) then resolves to the ducklake
 * schema UDFs via the search_path forced in SPIExecuteInSubtransaction.
 */
duckdb::vector<duckdb::string>
PgDuckLakeMetadataManager::GetFlushPartitionSQLExpressions(const duckdb::DuckLakeTableEntry &table) {
	duckdb::vector<duckdb::string> result;
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		return result;
	}
	for (auto &field : partition_data->fields) {
		auto &col = table.GetColumnByFieldId(field.field_id);
		auto col_name = duckdb::KeywordHelper::WriteOptionallyQuoted(col.GetName());
		auto type = col.GetType();
		duckdb::string rendered;
		if (type.id() == duckdb::LogicalTypeId::VARCHAR) {
			rendered = "convert_from(" + col_name + ", 'UTF8')";
		} else if (type.id() == duckdb::LogicalTypeId::BLOB) {
			rendered = col_name;
		} else if (GetColumnTypeInternal(type) == "VARCHAR") {
			rendered = "CAST(" + col_name + " AS " + type.ToString() + ")";
		} else {
			rendered = "CAST(" + col_name + " AS " + GetColumnTypeInternal(type) + ")";
		}
		/* DuckDB hashes DECIMALs wider than 18 digits (INT128 storage) by their fixed-scale
		 * string form; ducklake.murmur3_32(numeric) implements the narrow unscaled-long rule. */
		if (field.transform.type == duckdb::DuckLakeTransformType::BUCKET &&
		    type.id() == duckdb::LogicalTypeId::DECIMAL && duckdb::DecimalType::GetWidth(type) > 18) {
			rendered = "CAST(" + rendered + " AS TEXT)";
		}
		result.push_back(duckdb::DuckLakePartitionUtils::GetPartitionSQLExpression(field.transform, rendered));
	}
	return result;
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

struct InlinedTypmodCoercion {
	FmgrInfo finfo;
	int nargs;
	int32 typmod;
};

/* Resolved through PG's own coercion lookup so the stored value matches a
 * plain INSERT. */
InlinedTypmodCoercion *
MakeInlinedTypmodCoercion(Oid inlined_type, int32_t inlined_typmod) {
	if (inlined_typmod < 0) {
		return NULL;
	}

	Oid funcid = InvalidOid;
	if (find_typmod_coercion_function(inlined_type, &funcid) != COERCION_PATH_FUNC || !OidIsValid(funcid)) {
		return NULL;
	}

	HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup)) {
		return NULL;
	}
	int nargs = ((Form_pg_proc)GETSTRUCT(proctup))->pronargs;
	ReleaseSysCache(proctup);

	if (nargs != 2 && nargs != 3) {
		return NULL;
	}

	InlinedTypmodCoercion *coercion = (InlinedTypmodCoercion *)palloc0(sizeof(InlinedTypmodCoercion));
	fmgr_info(funcid, &coercion->finfo);
	coercion->nargs = nargs;
	coercion->typmod = inlined_typmod;
	return coercion;
}

Datum
ApplyInlinedTypmodCoercion(const InlinedTypmodCoercion *coercion, Datum value) {
	FmgrInfo *finfo = const_cast<FmgrInfo *>(&coercion->finfo);

	if (coercion->nargs == 3) {
		return FunctionCall3(finfo, value, Int32GetDatum(coercion->typmod), BoolGetDatum(false));
	}
	return FunctionCall2(finfo, value, Int32GetDatum(coercion->typmod));
}

Relation
OpenInlinedDataTable(uint64_t table_id, uint64_t schema_version, int lockmode, bool missing_ok) {
	char relname[NAMEDATALEN];
	snprintf(relname, sizeof(relname), "ducklake_inlined_data_%llu_%llu", (unsigned long long)table_id,
	         (unsigned long long)schema_version);

	Oid ducklake_nsp = get_namespace_oid("ducklake", false);
	Oid relid = get_relname_relid(relname, ducklake_nsp);
	if (!OidIsValid(relid)) {
		if (missing_ok) {
			return NULL;
		}
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("inlined data table \"%s\" does not exist", relname),
		                errhint("Call ducklake.ensure_inlined_data_table() first.")));
	}

	return table_open(relid, lockmode);
}

} // namespace pgducklake
