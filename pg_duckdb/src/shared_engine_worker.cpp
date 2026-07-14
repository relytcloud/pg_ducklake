/*
 * The pg_duckdb duckdb worker (heap-table queries).
 *
 * The generic machinery -- per-database worker registry, session-pool dispatch,
 * scan inversion + the scan-producer pool, session threads -- is the kernel
 * pgddb::DuckdbWorker base class. This subclass supplies pg_duckdb's policy: the
 * dispatch gate (read-only heap SELECTs when duckdb.use_shared_worker is on), the
 * engine accessors, and the background-worker entrypoints.
 */

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_guc.hpp"

#include <string>

#include "pgddb/pgddb_planner.hpp"
#include "pgddb/worker/duckdb_worker.hpp"

extern "C" {
#include "postgres.h"

#include "access/xact.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "postmaster/bgworker.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
}

namespace pgduckdb {

class DuckdbWorker final : public pgddb::DuckdbWorker {
public:
	DuckdbWorker()
	    : pgddb::DuckdbWorker(Settings {
	          "PgDuckdbWorker",
	          "pg_duckdb",
	          "pgduckdb_duckdb_worker_main",
	          "pgduckdb_scan_worker_main",
	          "pg_duckdb",
	          &duckdb_max_worker_sessions,
	          &duckdb_arrow_pool_pages,
	          &duckdb_arrow_page_size,
	          &duckdb_scan_pool_size,
	          &duckdb_scan_producers,
	      }) {
	}

protected:
	void
	Configure() override {
		/* Scan inversion makes worker execution PG-free, so the worker can run
		 * multi-threaded (the macOS single-thread workaround only applies to in-backend
		 * PG scans on DuckDB threads, which no longer happen here). Use a fixed small
		 * pool and let that many threads consume each remote scan. Set before the first
		 * DuckDBManager::Get() so it takes effect at instance init. */
		std::string threads = std::to_string(pgduckdb::duckdb_worker_threads);
		SetConfigOption("duckdb.threads", threads.c_str(), PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("duckdb.threads_for_postgres_scan", threads.c_str(), PGC_USERSET, PGC_S_SESSION);
	}

	void
	Prime() override {
		(void)DuckDBManager::Get();
	}

	duckdb::DuckDB &
	Database() override {
		return DuckDBManager::Get().GetDatabase();
	}
};

static DuckdbWorker *
TheWorker() {
	static DuckdbWorker worker;
	return &worker;
}

} // namespace pgduckdb

namespace {

/* Kernel CustomScan dispatch hook: route eligible read-only queries to the worker. */
duckdb::unique_ptr<pgddb::WorkerResultStream>
DispatchToWorker(const Query *query) {
	if (pgddb::DuckdbWorker::InWorker() || !pgduckdb::duckdb_use_shared_worker)
		return nullptr;
	if (!pgddb::DuckdbWorker::Enabled())
		return nullptr; /* not provisioned -> in-process execution */
	if (query->commandType != CMD_SELECT)
		return nullptr;
	if (!ActiveSnapshotSet())
		return nullptr;
	/* The shipped snapshot cannot see this transaction's own uncommitted changes;
	 * only dispatch when nothing has been written yet in this transaction. */
	if (GetTopTransactionIdIfAny() != InvalidTransactionId)
		return nullptr;

	return pgduckdb::TheWorker()->OpenSession(pgddb::DeparseQuery(query));
}

} // namespace

extern "C" {

PGDLLEXPORT void
pgduckdb_duckdb_worker_main(Datum main_arg) {
	pgduckdb::TheWorker()->Main((uint32_t)DatumGetObjectId(main_arg));
}

PGDLLEXPORT void
pgduckdb_scan_worker_main(Datum main_arg) {
	pgduckdb::TheWorker()->ScanWorkerMain((uint32_t)DatumGetObjectId(main_arg));
}

} // extern "C"

namespace pgduckdb {

void
InitDuckdbWorker() {
	TheWorker()->Init();
	pgddb::pgddb_dispatch_to_worker_hook = DispatchToWorker;
}

} // namespace pgduckdb
