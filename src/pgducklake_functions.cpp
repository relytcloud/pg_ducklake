/*
 * pgducklake_functions.cpp -- DuckLake read-only function exposing.
 *
 * Exposes upstream DuckLake read-only functions as PostgreSQL functions in
 * the `ducklake` schema. Three layers are involved:
 *
 *   C++ side (this file)
 *     - Registers function names with pg_duckdb via RegisterDuckdbOnlyFunction
 *       so the planner routes queries to DuckDB.
 *     - Implements C-level PG functions that build and execute DuckDB queries
 *       (e.g., cleanup_old_files which returns a scalar value).
 *
 *   DuckDB side (pgducklake_duckdb.cpp)
 *     - Registers wrapper table macros in system.main that inject the catalog
 *       constant and delegate to ducklake_<name>() global functions.
 *
 *   SQL side (pg_ducklake--0.1.0.sql)
 *     - Defines the actual PG function signatures as DuckDB-only C stubs.
 *
 * === Function Mapping Rules ===
 *
 * pg_duckdb's DuckDB-only routing rewrites a PG function call to:
 *
 *     system.main.<pg_function_name>(args...)
 *
 * DuckLake extension registers its functions globally as ducklake_<name>
 * with a catalog arg. Wrapper table macros in system.main bridge the gap:
 *
 *   PG function: ducklake.snapshots()
 *     -> DuckDB-only routing: system.main.snapshots()
 *     -> Wrapper macro: FROM ducklake_snapshots('pgducklake')
 *
 * Pattern A -- catalog-level (no table arg):
 *   PG:    ducklake.snapshots()                        [DuckDB-only C stub]
 *   Macro: snapshots() -> ducklake_snapshots(catalog)
 *
 * Pattern B -- table-scoped (text args):
 *   PG:    ducklake.list_files(schema text, table text)  [DuckDB-only C stub]
 *   Macro: list_files(schema, table) -> ducklake_list_files(catalog, table, schema => schema)
 *
 * Pattern C -- C-implemented (scalar return, complex logic):
 *   Direct:  ducklake.cleanup_old_files(older_than interval)  [C function]
 */

#include "pgducklake/pgducklake_functions.hpp"
#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"

#include <string>

#include <duckdb/common/string_util.hpp>

#include "pgducklake/utility/cpp_wrapper.hpp"

extern "C" {
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
}

#include "pgduckdb/pgduckdb_contracts.hpp"

namespace pgducklake {

void RegisterDuckdbOnlyFunctions() {
  pgduckdb::RegisterDuckdbOnlyExtension("pg_ducklake");
  // Existing functions
  pgduckdb::RegisterDuckdbOnlyFunction("options");
  pgduckdb::RegisterDuckdbOnlyFunction("time_travel");
  // Snapshot functions
  pgduckdb::RegisterDuckdbOnlyFunction("snapshots");
  pgduckdb::RegisterDuckdbOnlyFunction("current_snapshot");
  pgduckdb::RegisterDuckdbOnlyFunction("last_committed_snapshot");
  // Metadata functions
  pgduckdb::RegisterDuckdbOnlyFunction("table_info");
  pgduckdb::RegisterDuckdbOnlyFunction("list_files");
  // Data change feed functions
  pgduckdb::RegisterDuckdbOnlyFunction("table_insertions");
  pgduckdb::RegisterDuckdbOnlyFunction("table_deletions");
  pgduckdb::RegisterDuckdbOnlyFunction("table_changes");
}

} // namespace pgducklake

extern "C" {

/*
 * cleanup_old_files(older_than interval) -> bigint
 *
 * Clean up old data files no longer referenced by the current snapshot.
 * When older_than is NULL, all scheduled files are cleaned.
 */
DECLARE_PG_FUNCTION(ducklake_cleanup_old_files) {
  std::string query;

  if (PG_ARGISNULL(0)) {
    query = "SELECT count(*) FROM "
            "ducklake_cleanup_old_files(" PGDUCKLAKE_DUCKDB_CATALOG_QUOTED ", "
            "cleanup_all => true)";
  } else {
    Interval *interval = PG_GETARG_INTERVAL_P(0);
    char *interval_str = DatumGetCString(
        DirectFunctionCall1(interval_out, IntervalPGetDatum(interval)));

    query = duckdb::StringUtil::Format(
        "SELECT count(*) FROM "
        "ducklake_cleanup_old_files(" PGDUCKLAKE_DUCKDB_CATALOG_QUOTED ", "
        "older_than => now() - INTERVAL '%s')",
        interval_str);
  }

  const char *error_msg = nullptr;
  int result = pgducklake::ExecuteDuckDBQuery(query.c_str(), &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to clean up old files: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  /* TODO: parse result count from DuckDB query result */
  PG_RETURN_INT64(0);
}

} // extern "C"
