/*
 * pgducklake.cpp — PostgreSQL extension bootstrap entry points.
 *
 * Defines module metadata and _PG_init(), wiring GUC registration and pg_duckdb
 * callback registration for DuckLake extension loading.
 */

#include "pgducklake/pgducklake_direct_insert.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_direct_insert.hpp"

extern "C" {
#include "postgres.h"

#include "fmgr.h"
#include "optimizer/planner.h"

#include "pgduckdb/pgduckdb_contracts.h"

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

planner_hook_type prev_planner_hook = NULL;

void _PG_init(void) {
  // Register callback for deferred static extension loading
  RegisterDuckdbLoadExtension(ducklake_load_extension);
  // Register DuckLake GUCs
  pg_ducklake::RegisterGUCs();
  // Register custom scan node methods
  pg_ducklake::RegisterDirectInsertNode();
  // Install planner hook after pg_duckdb (runs first due to LIFO order)
  prev_planner_hook = planner_hook;
  planner_hook = pg_ducklake::DucklakeDirectInsertPlannerHook;
}

} // extern "C"
