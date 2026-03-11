#pragma once

/*
 * pgducklake_functions.hpp -- DuckLake read-only function exposing.
 *
 * Registers DuckDB-only functions with pg_duckdb so that pg_duckdb's planner
 * can route them to DuckDB for execution.
 */

namespace pgducklake {

void RegisterDuckdbOnlyFunctions();

} // namespace pgducklake
