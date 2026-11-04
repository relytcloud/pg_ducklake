#pragma once
// pg_ducklake's shared per-database DuckDB worker (see pgddb/worker/duckdb_worker.hpp).

#include "pgddb/worker/duckdb_worker.hpp"

#include <string>

namespace pgducklake {

class DuckLakeWorker final : public pgddb::DuckdbWorker {
public:
	static DuckLakeWorker &Get();

protected:
	duckdb::DuckDB &Prime() override;
	void BeforeSession() override;
	std::string ServeRequest(const std::string &request) override;

private:
	DuckLakeWorker();
};

// _PG_init: construct the singleton, register its hooks and install the dispatch gate.
void InitDuckdbWorker();

} // namespace pgducklake
