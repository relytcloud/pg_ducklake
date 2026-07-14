/*
 * pgducklake.cpp -- PostgreSQL extension bootstrap entry points.
 */

#include "pgducklake/constants.hpp"
#include "pgducklake/duckdb_manager.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"

#include <storage/ducklake_metadata_manager.hpp>

#include "pgddb/pgddb_node.hpp"
#include "pgddb/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"

#include "commands/extension.h"
#include "fmgr.h"
}

namespace pgducklake {

// Bootstrap entry points wired up by _PG_init; declared here since it is their only caller.
void InitGUCs();
void InitMaintenanceWorker();
void InitDuckdbWorker();
void InitDirectInsertStatsShmem();
void InitDuckDBManager();
void RegisterDirectInsertNode();
void InitTableAmHook();
void InitHooks();
void InitRuleutilsHooks();
void InitTypeHooks();
void RegisterXactCallback();
void InitFDW();
void InitSecrets();

} // namespace pgducklake

extern "C" {

#ifdef PG_MODULE_MAGIC_EXT
#ifndef PG_DUCKLAKE_VERSION
// Fallback for static analysis; normally defined by the build system.
#define PG_DUCKLAKE_VERSION "unknown"
#endif
PG_MODULE_MAGIC_EXT(.name = "pg_ducklake", .version = PG_DUCKLAKE_VERSION);
#else
PG_MODULE_MAGIC;
#endif

void
_PG_init(void) {
	// Register metadata manager factory in DuckLake's process-global registry.
	duckdb::DuckLakeMetadataManager::Register(PGDUCKLAKE_DUCKDB_CATALOG, pgducklake::PgDuckLakeMetadataManager::Create);
	pgducklake::InitGUCs();
	pgducklake::InitMaintenanceWorker();
	pgducklake::InitDuckdbWorker();
	pgducklake::InitDirectInsertStatsShmem();
	pgducklake::InitDuckDBManager();
	pgddb::InitNode("DuckLakeScan");
	pgducklake::RegisterDirectInsertNode();
	pgducklake::InitTableAmHook();
	pgducklake::InitHooks();
	pgducklake::InitRuleutilsHooks();
	pgducklake::InitTypeHooks();
	// Mirror PG transaction events to DuckDB's DuckLake transaction.
	pgducklake::RegisterXactCallback();
	pgducklake::InitFDW();
	pgducklake::InitSecrets();
}

/*
 * ducklake_initialize() -- SQL bootstrap run once during CREATE EXTENSION. It
 * forces DuckDB init (whose OnPostInit attaches the pgducklake DuckLake
 * catalog) and, on DROP+CREATE within one backend, re-attaches it.
 */
DECLARE_PG_FUNCTION(ducklake_initialize) {
	elog(LOG, "ducklake_initialize() called");

	if (!creating_extension) {
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("ducklake_initialize() can only be called during "
		                                                          "CREATE EXTENSION")));
	}

	if (pgducklake::PgDuckLakeMetadataManager::IsInitialized()) {
		ereport(ERROR,
		        (errcode(ERRCODE_DUPLICATE_SCHEMA), errmsg("DuckLake reserved schema \"ducklake\" is already in use")));
	}

	// First CREATE: SELECT 1 triggers Initialize() -> OnPostInit() -> attach. On
	// DROP+CREATE in one backend DuckDB is already alive, so OnPostInit does not
	// re-run and we must re-attach (the catalog was detached during DROP).
	bool duckdb_already_initialized = pgducklake::DuckDBManager::IsInitialized();

	pgducklake::DuckDBQueryOrThrow("SELECT 1");

	if (duckdb_already_initialized) {
		ducklake_attach_catalog();
	}

	PG_RETURN_VOID();
}

} // extern "C"
