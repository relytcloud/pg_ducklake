#pragma once

/*
 * pgducklake_duckdb.hpp — High-level C++ interface for DuckDB/DuckLake operations
 *
 * This header provides a clean C++ interface for interacting with pg_duckdb's
 * DuckDB instance and managing the DuckLake extension lifecycle.
 *
 * IMPORTANT: This header includes DuckDB headers. Only include it in DuckDB-facing
 * translation units (not in PostgreSQL-facing code).
 */

#include "duckdb.hpp"

namespace pgducklake {

/**
 * Manager class for DuckDB/DuckLake operations.
 * Provides access to pg_duckdb's DuckDB instance.
 *
 * Note: DuckLake extension is loaded during _PG_init(), so it's already
 * available when you use this class.
 */
class DuckLakeManager {
public:
	/**
	 * Get reference to pg_duckdb's DuckDB database instance.
	 * Use this to access DuckDB APIs directly.
	 */
	static duckdb::DuckDB &GetDatabase();

	/**
	 * Create a new connection to the DuckDB database.
	 * Each connection maintains its own transaction state.
	 */
	static duckdb::unique_ptr<duckdb::Connection> CreateConnection();

	/**
	 * Execute a query and return whether it succeeded.
	 * @param query SQL query to execute
	 * @param errmsg_out If non-null, set to error message on failure
	 * @return 0 on success, 1 on error
	 */
	static int ExecuteQuery(const char *query, const char **errmsg_out);

private:
	DuckLakeManager() = delete; // Static-only class
};

} // namespace pgducklake
