#pragma once

/*
 * pgducklake_functions.hpp -- DuckLake function exposing.
 *
 * Registers DuckDB-only functions with pg_duckdb so that pg_duckdb's planner
 * can route them to DuckDB for execution, and registers wrapper table macros
 * in DuckDB's system.main catalog that bridge PG function names to
 * ducklake_<name>(catalog, ...) globals.
 */

namespace duckdb {
class DatabaseInstance;
}

namespace pgducklake {

void RegisterDuckdbOnlyFunctions();
void RegisterWrapperMacros(duckdb::DatabaseInstance &db);
void RegisterCleanupFunction(duckdb::DatabaseInstance &db);
void RegisterFlushInlinedDataFunction(duckdb::DatabaseInstance &db);

} // namespace pgducklake
