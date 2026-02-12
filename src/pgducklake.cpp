extern "C" {
#include "postgres.h"

#include "fmgr.h"

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

// Defined in pgducklake_duckdb.cpp
extern void ducklake_load_extension(void);

void
_PG_init(void) {
	// Load DuckLake extension into pg_duckdb's DuckDB instance at initialization
	ducklake_load_extension();
}

} // extern "C"
