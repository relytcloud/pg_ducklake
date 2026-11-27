#pragma once

#include "pgddb/pgddb_duckdb.hpp"

#include <exception>
#include <string>

namespace pgducklake {

class DuckDBManager : public ::pgddb::DuckDBManager {
public:
	static bool IsInitialized();
	static DuckDBManager &Get();
	static void Reset();
	// Reset() if the instance was created but OnPostInit() never completed.
	// Called at top-level transaction end.
	static void ResetIfPostInitFailed();

	// Mark cached secrets stale (next GetConnection reloads them). No-op if the
	// instance is not yet initialized. Called from syscache invalidation.
	static void InvalidateSecretsIfInitialized();

	// Per-table CREATE ... WITH (ducklake.table_path=...) override. While set, it
	// takes precedence over the ducklake.default_table_path session GUC in
	// RefreshConnectionState (which would otherwise re-push the default on the
	// next GetConnection and clobber the override before the CREATE runs).
	void SetTablePathOverride(const std::string &path);
	void ClearTablePathOverride();

protected:
	void OnPostInit(duckdb::ClientContext &context) override;
	// Syncs ducklake.default_table_path and reloads S3/Azure secrets per
	// GetConnection so a runtime SET / catalog change reaches the next statement;
	// OnPostInit runs once per instance and would miss it.
	void RefreshConnectionState(duckdb::ClientContext &context) override;
	// Also begin a DuckDB transaction while a ducklake-only function statement is
	// planned/executed, so its bind and execute share one transaction (DuckLake
	// compaction binds a weak_ptr to the bind-time transaction it reads at execute).
	bool ShouldBeginTransaction() override;

private:
	void DropSecrets(duckdb::ClientContext &context);
	void LoadSecrets(duckdb::ClientContext &context);

	bool secrets_valid_ = false;
	// Set when OnPostInit() returns. Initialize() assigns `database` before it
	// calls OnPostInit(), so an error part-way through (a cancel or timeout
	// while attaching the DuckLake catalog) would otherwise leave an instance
	// that counts as initialized but has no "pgducklake" catalog, and nothing
	// would ever attach it.
	bool post_init_done_ = false;
	bool has_table_path_override_ = false;
	std::string table_path_override_;
	// Last value pushed to DuckDB's ducklake_default_table_path, so
	// RefreshConnectionState only issues SET/RESET when it actually changes
	// (and self-corrects after an override is cleared, even on the error path).
	std::string last_pushed_table_path_;
	static duckdb::unique_ptr<DuckDBManager> instance_;
};

/* Throws a duckdb exception on error (the DECLARE_PG_FUNCTION guard turns it into
 * a PG error). The (query) overload runs on DuckDBManager::Get()'s cached connection. */
duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(duckdb::ClientContext &context, const std::string &query);
duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(duckdb::Connection &connection, const std::string &query);
duckdb::unique_ptr<duckdb::QueryResult> DuckDBQueryOrThrow(const std::string &query);

/* Unwraps the JSON-serialized duckdb ErrorData in e.what() to the plain message. */
std::string DuckDBErrorMessage(const std::exception &e);

} // namespace pgducklake

/* Detach the "pgducklake" DuckLake catalog (utility hook, after DROP EXTENSION). */
void ducklake_detach_catalog();

/* Attach the "pgducklake" DuckLake catalog (OnPostInit and ducklake_initialize). */
void ducklake_attach_catalog();

namespace pgducklake {

/* Allow opening a PG subtransaction while DuckDB has an active transaction
 * (SUBXACT_EVENT_START_SUB guard; e.g. DuckLake FlushChanges retry loop). */
void SetAllowSubtransaction(bool allow);

/* Pin this process's DuckDB instance to one thread, ignoring ducklake.threads.
 * Called by the maintenance worker; must run before the instance initializes. */
void ForceSingleThreadedDuckDB();

/* Set per-statement in the planner hook: force a DuckDB transaction for queries
 * that call a ducklake-only function, so bind and execute share one. */
void SetForceScanTransaction(bool force);

} // namespace pgducklake
