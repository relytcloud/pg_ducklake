#pragma once

#include "duckdb.hpp"

namespace pgddb {

namespace ddb {
bool DidWrites(duckdb::ClientContext &context);
} // namespace ddb

class DuckDBManager {
public:
	DuckDBManager()
	    : database(nullptr), connection(nullptr), default_dbname("<!UNSET!>"), duckdb_temp_directory(strdup("")),
	      duckdb_extension_directory(strdup("")), duckdb_max_temp_directory_size(strdup("")), duckdb_maximum_memory(0),
	      duckdb_threads(1) {
	}
	virtual ~DuckDBManager() = default;

	inline const std::string &
	GetDefaultDBName() const {
		return default_dbname;
	}

	inline duckdb::DuckDB &
	GetDatabase() {
		return *database;
	}

	duckdb::Connection *GetConnection(bool force_transaction = false);
	duckdb::unique_ptr<duckdb::Connection> CreateConnection();

	// Cached connection without GetConnection's transaction/refresh handling.
	virtual duckdb::Connection *
	GetConnectionUnsafe() {
		return connection.get();
	}

private:
	DuckDBManager(const DuckDBManager &) = delete;
	DuckDBManager &operator=(const DuckDBManager &) = delete;

protected:
	void Initialize();
	virtual void
	OnInit(duckdb::DBConfig & /*config*/) {
	}
	virtual void
	OnPostInit(duckdb::ClientContext & /*context*/) {
	}
	virtual void
	RefreshConnectionState(duckdb::ClientContext & /*context*/) {
	}

	virtual std::string
	ConnectionString() {
		return {};
	}

	virtual void
	RequireExecution() {
	}

	// Whether GetConnection should open a DuckDB transaction. Default uses PG's
	// IsInTransactionBlock at top-level; pg_duckdb overrides it so DuckDB joins
	// the outer PG transaction when called from inside a plpgsql function.
	virtual bool ShouldBeginTransaction();

	/*
	 * FIXME: raw pointer, not unique_ptr: the MotherDuck extension ABORTs when
	 * the DuckDB database destructor runs at process exit, crashing Postgres.
	 */
	duckdb::DuckDB *database;
	duckdb::unique_ptr<duckdb::Connection> connection;
	std::string default_dbname;

	char *duckdb_temp_directory;
	char *duckdb_extension_directory;
	// Max data in DuckDB's temp_directory when set (e.g., 1GB)
	char *duckdb_max_temp_directory_size;
	// Max memory DuckDB can use in MB (e.g., 4096 for 4GB)
	int duckdb_maximum_memory;
	// Max DuckDB threads per backend. Defaults to 1 so all DuckDB work runs on
	// the backend's main thread: PG routines reached from DuckDB execution
	// (e.g. PostgresTableReader scans) are not thread-safe off it. -1 leaves
	// DuckDB's default (all cores).
	int duckdb_threads;
	// Register and ATTACH the "pgduckdb" heap catalog. The duckdb worker turns
	// this off: its DuckDB threads must never reach a PostgreSQL heap scan.
	bool attach_postgres_catalog = true;

public:
	static duckdb::unique_ptr<duckdb::QueryResult> QueryOrThrow(duckdb::ClientContext &context,
	                                                            const std::string &query);
	static inline duckdb::unique_ptr<duckdb::QueryResult>
	QueryOrThrow(duckdb::Connection &connection, const std::string &query) {
		return QueryOrThrow(*connection.context, query);
	}
	inline duckdb::unique_ptr<duckdb::QueryResult>
	QueryOrThrow(const std::string &query) {
		auto *conn = GetConnectionUnsafe();
		return QueryOrThrow(*conn, query);
	}
};

// Installed by the consumer in _PG_init so the library scan layer can reach a
// connection without owning a singleton.
using PgddbGetConnectionHook = duckdb::Connection *(*)(bool force_transaction);
extern PgddbGetConnectionHook pgddb_get_connection_hook;

duckdb::Connection *GetConnection(bool force_transaction = false);

} // namespace pgddb
