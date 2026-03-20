/*
 * pgducklake.cpp -- PostgreSQL extension bootstrap entry points.
 *
 * Defines module metadata and _PG_init(), wiring GUC registration, pg_duckdb
 * callback registration, and pg_ducklake hook initialization.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"
#include "storage/ducklake_metadata_manager.hpp"

#include "pgducklake/pgducklake_direct_insert.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_fdw.hpp"
#include "pgducklake/pgducklake_functions.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_hooks.hpp"
#include "pgduckdb/pgduckdb_contracts.hpp"

extern "C" {
#include "postgres.h"

#include "fmgr.h"
}

extern "C" {

#ifdef PG_MODULE_MAGIC_EXT
#ifndef PG_DUCKLAKE_VERSION
// Should always be defined via build system, but keep a fallback here for
// static analysis tools etc.
#define PG_DUCKLAKE_VERSION "unknown"
#endif
PG_MODULE_MAGIC_EXT(.name = "pg_ducklake", .version = PG_DUCKLAKE_VERSION);
#else
PG_MODULE_MAGIC;
#endif

void _PG_init(void) {
  // Register metadata manager factory in DuckLake's process-global registry.
  duckdb::DuckLakeMetadataManager::Register(
      PGDUCKLAKE_DUCKDB_CATALOG,
      pgducklake::PgDuckLakeMetadataManager::Create);
  // Register callback for deferred static extension loading
  pgduckdb::RegisterDuckdbLoadExtension(ducklake_load_extension);
  // Register pg_ducklake's DuckDB-only functions with pg_duckdb's metadata cache
  pgducklake::RegisterDuckdbOnlyFunctions();
  // Register ducklake.variant as a passthrough type (OID resolved lazily)
  pgduckdb::RegisterPassthroughType(PGDUCKLAKE_PG_SCHEMA, "variant", "VARIANT");
  // Register DuckLake GUCs
  pgducklake::RegisterGUCs();
  // Register custom scan node methods
  pgducklake::RegisterDirectInsertNode();
  // Install pg_ducklake planner/utility hooks.
  pgducklake::InitHooks();
  // Register FDW callbacks and hooks.
  pgducklake::InitFDW();
}

} // extern "C"
