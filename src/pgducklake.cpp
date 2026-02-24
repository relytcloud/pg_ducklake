#include "pgducklake/pgducklake_guc.hpp"

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
void ducklake_init_extension(void);
void ducklake_load_extension(void *db, void *context);

typedef void (*DuckDBLoadExtension)(void *db, void *context);
bool RegisterDuckdbLoadExtension(DuckDBLoadExtension extension);

void _PG_init(void) {
  // Register the DuckLake metadata manager (eager, no DuckDB instance needed)
  ducklake_init_extension();
  // Register callback for deferred static extension loading
  RegisterDuckdbLoadExtension(ducklake_load_extension);
  // Register DuckLake GUCs
  pg_ducklake::RegisterGUCs();
}

} // extern "C"
