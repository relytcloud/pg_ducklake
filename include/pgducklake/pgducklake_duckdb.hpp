#pragma once

/*
 * pgducklake_duckdb.hpp — Interface for DuckDB/DuckLake operations
 *
 * Provides functions for DuckLake extension lifecycle management and
 * direct DuckDB instance access (used by FDW for column inference).
 * General query execution against DuckDB is done via pg_duckdb's raw_query()
 * UDF through PostgreSQL's SPI.
 */

namespace duckdb {
class DuckDB;
}

/*
 * Callback invoked by pg_duckdb during DuckDBManager::Initialize().
 * Receives a pointer to the DuckDB instance (duckdb::DuckDB*) and loads
 * the DuckLake static extension into it.
 */
void ducklake_load_extension(void *db, void *context);

/*
 * Returns the DuckDB instance pointer stored during ducklake_load_extension().
 * Used by FDW column inference to probe remote schemas via a temporary
 * DuckDB connection. Returns nullptr before initialization.
 */
duckdb::DuckDB *ducklake_get_duckdb_database();
