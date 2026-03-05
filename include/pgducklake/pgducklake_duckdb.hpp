#pragma once

/*
 * pgducklake_duckdb.hpp -- C++ interface for DuckDB/DuckLake operations
 *
 * Provides functions for DuckLake extension lifecycle management.
 * Query execution against DuckDB is done via pg_duckdb's raw_query() UDF
 * through PostgreSQL's SPI, not through direct DuckDB instance access.
 */

namespace duckdb {
class DuckDB;
}

/*
 * Callback invoked by pg_duckdb during DuckDBManager::Initialize().
 * Receives a reference to the DuckDB instance and loads
 * the DuckLake static extension into it.
 */
void ducklake_load_extension(duckdb::DuckDB &db);

/* Returns the DuckDB instance, used by FDW for column inference. */
duckdb::DuckDB *ducklake_get_duckdb_database();
