#pragma once

/*
 * pgducklake_duckdb_query.hpp — Helpers for executing DuckDB queries via
 * pg_duckdb
 *
 * Provides wrappers around duckdb.raw_query() for executing DuckDB queries
 * from pg_ducklake code.
 */

namespace pgducklake {

/*
 * Execute a DuckDB query via pg_duckdb's duckdb.raw_query() UDF.
 * Returns 0 on success, 1 on error.
 * On error, sets *errmsg_out to the error message (if non-null).
 */
int ExecuteDuckDBQuery(const char *query, const char **errmsg_out);

} // namespace pgducklake
