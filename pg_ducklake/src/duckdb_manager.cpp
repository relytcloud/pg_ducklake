#include "pgducklake/constants.hpp"
#include "pgducklake/ducklake_secret.hpp"
#include "pgducklake/duckdb_manager.hpp"
#include "pgducklake/functions.hpp"
#include "pgducklake/guc.hpp"

#include <cstring>
#include <filesystem>

#include "pgddb/catalog/pgddb_storage.hpp"
#include "pgddb/pg/transactions.hpp"

#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <duckdb/transaction/transaction_context.hpp>
#include <ducklake_extension.hpp>

extern "C" {
#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "pgddb/pgddb_ruleutils.h"
}

#include "pgddb/utility/cpp_wrapper.hpp"

void
ducklake_detach_catalog() {
	try {
		pgducklake::DuckDBQueryOrThrow("DETACH DATABASE IF EXISTS " PGDUCKLAKE_DUCKDB_CATALOG);
	} catch (const std::exception &e) {
		elog(WARNING, "Failed to detach DuckLake catalog: %s", pgducklake::DuckDBErrorMessage(e).c_str());
	}
}

void
ducklake_attach_catalog() {
	/* METADATA_CATALOG points the metadata connection's search path at the
	 * pgducklake catalog itself, since pg_ducklake keeps metadata in PostgreSQL
	 * rather than a separate DuckDB database, so DuckDB-native queries resolve
	 * system functions through normal catalog search. */
	duckdb::string query =
	    "ATTACH 'ducklake:" PGDUCKLAKE_DUCKDB_CATALOG ":' AS " PGDUCKLAKE_DUCKDB_CATALOG
	    "(METADATA_SCHEMA " PGDUCKLAKE_PG_SCHEMA_QUOTED ", METADATA_CATALOG " PGDUCKLAKE_DUCKDB_CATALOG;
	/* First-time init: pass DATA_PATH so DuckLake stores it in catalog metadata. */
	auto data_path = duckdb::StringUtil::Format("%s/pg_ducklake", DataDir);
	try {
		std::filesystem::create_directory(data_path);
	} catch (const std::filesystem::filesystem_error &e) {
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
		                errmsg("failed to create DuckLake data directory \"%s\": %s", data_path.c_str(), e.what())));
	}
	query += ", DATA_PATH '" + data_path + "'";
	/* Subsequent ATTACHes omit DATA_PATH so DuckLake reads it from stored
	 * metadata, avoiding mismatch if the path was changed (e.g. to an S3 bucket). */
	query += ")";

	elog(DEBUG1, "Executing query: %s", query.c_str());

	try {
		pgducklake::DuckDBQueryOrThrow(query);
	} catch (const std::exception &e) {
		elog(ERROR, "Failed to attach DuckLake catalog: %s", pgducklake::DuckDBErrorMessage(e).c_str());
	}
}

namespace pgducklake {
void ResetDirectInsertCaches();
} // namespace pgducklake

class PostgresScannerExtension : public duckdb::Extension {
public:
	std::string
	Name() override {
		return "postgres_scanner";
	}
	void Load(duckdb::ExtensionLoader &loader) override;
};

namespace pgducklake {

void
DuckDBManager::OnPostInit(duckdb::ClientContext &context) {
	// httpfs et al. are not bundled; auto-install+load them on first use so
	// remote read_csv/read_parquet and s3:// paths work without a manual LOAD.
	DuckDBQueryOrThrow(context, "SET autoinstall_known_extensions = true");
	DuckDBQueryOrThrow(context, "SET autoload_known_extensions = true");

	// The "pgduckdb" PostgresStorageExtension is registered + attached by the
	// kernel's DuckDBManager::Initialize, before this runs.
	database->LoadStaticExtension<duckdb::DucklakeExtension>();
	database->LoadStaticExtension<PostgresScannerExtension>();
	pgducklake::ResetDirectInsertCaches();
	pgducklake::RegisterDucklakeFunctions(*context.db);

	ducklake_attach_catalog();
}

void
DuckDBManager::RefreshConnectionState(duckdb::ClientContext &context) {
	// Reconcile ducklake_default_table_path on every GetConnection: a per-table
	// WITH override wins over the ducklake.default_table_path session GUC. Only
	// issue SET/RESET when the desired value changes, so the common (no path)
	// case is free and a cleared override self-corrects on the next connection.
	std::string desired_table_path;
	if (has_table_path_override_) {
		desired_table_path = table_path_override_;
	} else if (default_table_path && default_table_path[0] != '\0') {
		desired_table_path = default_table_path;
	}
	if (desired_table_path != last_pushed_table_path_) {
		try {
			if (desired_table_path.empty()) {
				DuckDBQueryOrThrow(context, "RESET ducklake_default_table_path");
			} else {
				DuckDBQueryOrThrow(context, "SET ducklake_default_table_path = " +
				                                duckdb::KeywordHelper::WriteQuoted(desired_table_path));
			}
			last_pushed_table_path_ = desired_table_path;
		} catch (const std::exception &e) {
			elog(WARNING, "failed to sync ducklake table path to DuckDB: %s", DuckDBErrorMessage(e).c_str());
		}
	}

	// Re-emit S3/Azure secrets from the catalog when a SERVER/USER MAPPING changed.
	if (!secrets_valid_) {
		DropSecrets(context);
		LoadSecrets(context);
		secrets_valid_ = true;
	}
}

void
DuckDBManager::LoadSecrets(duckdb::ClientContext &context) {
	auto queries = InvokeCPPFunc(pgducklake::ListCreateSecretQueries);
	ListCell *lc;
	foreach (lc, queries) {
		QueryOrThrow(context, (const char *)lfirst(lc));
	}
}

void
DuckDBManager::DropSecrets(duckdb::ClientContext &context) {
	auto secrets = QueryOrThrow(context, "SELECT name FROM duckdb_secrets() WHERE name LIKE 'pgducklake_secret_%';");
	while (auto chunk = secrets->Fetch()) {
		for (size_t i = 0, s = chunk->size(); i < s; ++i) {
			QueryOrThrow(context, duckdb::StringUtil::Format("DROP SECRET %s;", chunk->GetValue(0, i).ToString()));
		}
	}
}

void
DuckDBManager::InvalidateSecretsIfInitialized() {
	if (IsInitialized()) {
		instance_->secrets_valid_ = false;
	}
}

void
DuckDBManager::SetTablePathOverride(const std::string &path) {
	// Flag only; RefreshConnectionState applies it on the CREATE's GetConnection.
	has_table_path_override_ = true;
	table_path_override_ = path;
}

void
DuckDBManager::ClearTablePathOverride() {
	has_table_path_override_ = false;
	table_path_override_.clear();
}

} // namespace pgducklake

namespace pgducklake {

duckdb::unique_ptr<DuckDBManager> DuckDBManager::instance_;

static bool g_force_single_thread = false;

void
ForceSingleThreadedDuckDB() {
	g_force_single_thread = true;
}

bool
DuckDBManager::IsInitialized() {
	return instance_ != nullptr && instance_->database != nullptr;
}

DuckDBManager &
DuckDBManager::Get() {
	if (!instance_) {
		instance_ = duckdb::make_uniq<DuckDBManager>();
	}
	if (!instance_->database) {
		instance_->duckdb_threads = g_force_single_thread ? 1 : threads;
		instance_->Initialize();
	}
	return *instance_;
}

void
DuckDBManager::Reset() {
	if (!instance_) {
		return;
	}
	instance_->connection = nullptr;
	delete instance_->database;
	instance_->database = nullptr;
	// The fresh instance has none of the state we cached for the old one; force
	// RefreshConnectionState to re-push secrets and the table path next time.
	instance_->secrets_valid_ = false;
	instance_->last_pushed_table_path_.clear();
	instance_->has_table_path_override_ = false;
	instance_->table_path_override_.clear();
}

static duckdb::Connection *
GetConnectionForScan(bool force_transaction) {
	return DuckDBManager::Get().GetConnection(force_transaction);
}

void
InitDuckDBManager() {
	::pgddb::pgddb_get_connection_hook = GetConnectionForScan;
}

} // namespace pgducklake

namespace pgducklake {

/*
 * pgddb_db_and_schema_hook impl. Claims only relations on the ducklake table AM,
 * routing them to the DuckLake catalog; returns nullptr otherwise so the kernel
 * falls back to its "pgduckdb" storage catalog for heap/foreign tables and views.
 */
static List *
DbAndSchemaForDucklake(const char *postgres_schema_name, const char *table_am_name) {
	if (table_am_name == nullptr || strcmp(PGDUCKLAKE_TABLE_AM, table_am_name) != 0)
		return nullptr;
	return list_make2((void *)PGDUCKLAKE_DUCKDB_CATALOG, (void *)postgres_schema_name);
}

/*
 * pgddb_function_name_hook impl: rewrites a ducklake-only function to
 * "system.main.<func_name>" so DuckDB resolves the wrapper macro registered
 * under DEFAULT_SCHEMA; returns NULL otherwise for standard name resolution.
 */
static char *
DucklakeFunctionName(Oid function_oid, bool *use_variadic_p) {
	if (!IsDucklakeOnlyFunction(function_oid))
		return nullptr;
	if (use_variadic_p)
		*use_variadic_p = false;
	char *func_name = get_func_name(function_oid);
	// ducklake.query() wraps DuckDB's built-in query() table function; deparse
	// directly rather than routing through a system.main wrapper macro.
	if (std::strcmp(func_name, "query") == 0)
		return pstrdup("query");
	return psprintf("system.main.%s", quote_identifier(func_name));
}

void
InitRuleutilsHooks() {
	Register_pgddb_db_and_schema(DbAndSchemaForDucklake); // object-scoped: ducklake table AM
	// Heap/view/foreign relations fall back to the kernel's "pgduckdb" storage catalog.
	Register_pgddb_function_name(DucklakeFunctionName);
	// column_type_name (variant -> VARIANT) is registered in ducklake_types.cpp.
}

/*
 * PG -> DuckDB transaction sync. DuckLake keeps its own DuckLakeTransaction on
 * the pgducklake catalog (snapshot ids, inlined inserts). Without mirroring PG's
 * PRE_COMMIT / ABORT to DuckDB, that transaction stays open after each implicit-
 * autocommit statement, so later statements see a stale view and other backends
 * never observe the writes (metadata Iterators rely on a committed snapshot_id).
 */
static void
DuckLakeXactCallback_Cpp(XactEvent event) {
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT) {
		// An error escaping between SetAllowSubtransaction(true) and its normal
		// reset would leave the gate open and silently disable the SAVEPOINT
		// guard; every such escape ends in an abort, so close it here.
		SetAllowSubtransaction(false);
	}
	if (!pgducklake::DuckDBManager::IsInitialized()) {
		return;
	}
	auto *connection = pgducklake::DuckDBManager::Get().GetConnectionUnsafe();
	if (!connection) {
		return;
	}
	auto &context = *connection->context;
	if (!context.transaction.HasActiveTransaction()) {
		return;
	}
	switch (event) {
	case XACT_EVENT_PRE_COMMIT:
	case XACT_EVENT_PARALLEL_PRE_COMMIT:
		context.transaction.Commit();
		break;
	case XACT_EVENT_ABORT:
	case XACT_EVENT_PARALLEL_ABORT:
		context.transaction.Rollback(nullptr);
		break;
	case XACT_EVENT_PREPARE:
	case XACT_EVENT_PRE_PREPARE:
	case XACT_EVENT_COMMIT:
	case XACT_EVENT_PARALLEL_COMMIT:
		break;
	}
}

/*
 * C boundary: a DuckLake commit failure throws C++ through CallXactCallbacks (a
 * plain C frame), which would call std::terminate and SIGABRT the backend.
 * InvokeCPPFunc converts it into a regular PG ERROR so the transaction aborts.
 */
static void
DuckLakeXactCallback(XactEvent event, void * /*arg*/) {
	InvokeCPPFunc(DuckLakeXactCallback_Cpp, event);
}

/*
 * Gate flipped on only around DuckLake's metadata-commit
 * BeginInternalSubTransaction (its retry loop catches unique-violations); any
 * other SAVEPOINT during an active DuckDB transaction is rejected below.
 */
bool allow_subtransaction = false;

void
SetAllowSubtransaction(bool allow) {
	allow_subtransaction = allow;
}

/*
 * SAVEPOINT guard: while DuckDB holds an active transaction, a SAVEPOINT (or
 * BeginInternalSubTransaction without the allow_subtransaction gate) throws so
 * the inconsistency surfaces now instead of corrupting PG's snapshot bookkeeping
 * at parent commit time.
 */
static void
DuckLakeSubXactCallback_Cpp(SubXactEvent event) {
	if (!pgducklake::DuckDBManager::IsInitialized()) {
		return;
	}
	auto *connection = pgducklake::DuckDBManager::Get().GetConnectionUnsafe();
	if (!connection) {
		return;
	}
	auto &context = *connection->context;
	if (!context.transaction.HasActiveTransaction()) {
		return;
	}
	if (event == SUBXACT_EVENT_START_SUB && !allow_subtransaction) {
		throw duckdb::NotImplementedException("SAVEPOINT is not supported in DuckDB");
	}
}

/* Same C boundary as DuckLakeXactCallback: the throw above must become a PG
 * ERROR, not an uncaught C++ exception in a C frame. */
static void
DuckLakeSubXactCallback(SubXactEvent event, SubTransactionId /*my_subid*/, SubTransactionId /*parent_subid*/,
                        void * /*arg*/) {
	InvokeCPPFunc(DuckLakeSubXactCallback_Cpp, event);
}

void
RegisterXactCallback() {
	::pgddb::pg::RegisterXactCallback(DuckLakeXactCallback, nullptr);
	::pgddb::pg::RegisterSubXactCallback(DuckLakeSubXactCallback, nullptr);
}

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBQueryOrThrow(duckdb::ClientContext &context, const std::string &query) {
	auto res = context.Query(query, false);
	if (res->HasError()) {
		res->ThrowError();
	}
	return res;
}

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBQueryOrThrow(duckdb::Connection &connection, const std::string &query) {
	return DuckDBQueryOrThrow(*connection.context, query);
}

duckdb::unique_ptr<duckdb::QueryResult>
DuckDBQueryOrThrow(const std::string &query) {
	auto *connection = DuckDBManager::Get().GetConnection();
	return DuckDBQueryOrThrow(*connection, query);
}

std::string
DuckDBErrorMessage(const std::exception &e) {
	const char *what = e.what();
	// QueryResult::ThrowError() exceptions carry a JSON ErrorData blob; unwrap
	// it like cpp_wrapper does. Non-duckdb exceptions keep their plain message.
	if (what && what[0] == '{') {
		return duckdb::ErrorData(what).Message();
	}
	return what ? what : "unknown error";
}

} // namespace pgducklake

/*
 * DuckDB admin UDFs in the ducklake schema. ducklake.duckdb_query(text) has no C
 * entry point here: it is a ducklake_only_function SQL stub the planner rewrites
 * (DucklakeFunctionName) to DuckDB's query() table function.
 */
extern "C" {

PG_FUNCTION_INFO_V1(ducklake_recycle_ddb);
Datum
ducklake_recycle_ddb(PG_FUNCTION_ARGS) {
	// Recycling tears down a DuckDB instance that may hold a transaction tied
	// to the current PG transaction.
	::pgddb::pg::PreventInTransactionBlock(true, "ducklake.recycle_ddb()");
	pgducklake::DuckDBManager::Reset();
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_duckdb_raw_query);
Datum
ducklake_duckdb_raw_query(PG_FUNCTION_ARGS) {
	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("query must not be NULL")));
	const char *query = text_to_cstring(PG_GETARG_TEXT_PP(0));
	try {
		pgducklake::DuckDBQueryOrThrow(query);
	} catch (const std::exception &e) {
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s", pgducklake::DuckDBErrorMessage(e).c_str())));
	}
	PG_RETURN_VOID();
}

} // extern "C"
