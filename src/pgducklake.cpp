#include "pgducklake/pgducklake_guc.hpp"
#include "pgduckdb/pgduckdb_contracts.h"

extern "C" {
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#ifdef PG_MODULE_MAGIC_EXT
#ifndef PG_DUCKDB_VERSION
// Should always be defined via build system, but keep a fallback here for
// static analysis tools etc.
#define PG_DUCKDB_VERSION "unknown"
#endif
PG_MODULE_MAGIC_EXT(.name = "pg_duckdb", .version = PG_DUCKDB_VERSION);
#else
PG_MODULE_MAGIC;
#endif

// Forward declaration of C interface functions
void ducklake_load_extension(void *db, void *context);

void _PG_init(void) {
  // Register callback for deferred static extension loading
  RegisterDuckdbLoadExtension(ducklake_load_extension);
  // Register DuckLake GUCs
  pg_ducklake::RegisterGUCs();
}

} // extern "C"
