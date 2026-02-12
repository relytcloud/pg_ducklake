/*
 * pgducklake_duckdb.cpp — DuckDB-facing translation unit
 *
 * This file includes DuckDB and DuckLake headers but NEVER PostgreSQL headers.
 * It provides the high-level C++ interface for DuckDB/DuckLake operations.
 *
 * Access to pg_duckdb's DuckDB instance is via GetDuckDBDatabase(), an extern "C"
 * function exported by pg_duckdb with __attribute__((visibility("default"))).
 */

#include "pgducklake/pgducklake_duckdb.hpp"

#include "duckdb/main/database.hpp"
#include "ducklake_extension.hpp"

// Imported from pg_duckdb — returns duckdb::DuckDB* as void*
extern "C" void *GetDuckDBDatabase(void);

namespace pgducklake {

// Track whether DuckLake has been loaded
static bool ducklake_loaded = false;

// Thread-local storage for error messages
static thread_local std::string last_error;

void
DuckLakeManager::EnsureLoaded() {
	if (ducklake_loaded) {
		return;
	}

	auto &db = GetDatabase();
	db.LoadStaticExtension<duckdb::DucklakeExtension>();
	ducklake_loaded = true;
}

duckdb::DuckDB &
DuckLakeManager::GetDatabase() {
	auto *db_ptr = static_cast<duckdb::DuckDB *>(GetDuckDBDatabase());
	return *db_ptr;
}

duckdb::unique_ptr<duckdb::Connection>
DuckLakeManager::CreateConnection() {
	return duckdb::make_uniq<duckdb::Connection>(GetDatabase());
}

int
DuckLakeManager::ExecuteQuery(const char *query, const char **errmsg_out) {
	try {
		auto conn = CreateConnection();
		auto result = conn->Query(query);

		if (result->HasError()) {
			last_error = result->GetError();
			if (errmsg_out) {
				*errmsg_out = last_error.c_str();
			}
			return 1;
		}

		return 0;
	} catch (const std::exception &e) {
		last_error = e.what();
		if (errmsg_out) {
			*errmsg_out = last_error.c_str();
		}
		return 1;
	}
}

} // namespace pgducklake

/*
 * C interface functions for backward compatibility or C-only callers.
 * These simply delegate to the DuckLakeManager class.
 */

extern "C" void
ducklake_ensure_loaded(void) {
	pgducklake::DuckLakeManager::EnsureLoaded();
}

extern "C" int
ducklake_execute_query(const char *query, const char **errmsg_out) {
	return pgducklake::DuckLakeManager::ExecuteQuery(query, errmsg_out);
}
