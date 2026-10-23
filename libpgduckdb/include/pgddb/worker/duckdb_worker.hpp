#pragma once
// Shared per-database DuckDB worker.
//
// One background worker per database hosts one DuckDB instance and executes
// queries that backends dispatch to it, one session thread per query. Only the
// worker's main thread touches PostgreSQL (registry, dsm, shm_mq, latch);
// session threads and DuckDB scheduler threads reach it through in-process
// queues, and anything the query needs from PostgreSQL (for pg_ducklake: the
// DuckLake metadata SQL) is an RPC back to the dispatching backend, executed
// there inside that backend's own transaction and snapshot.
//
// A dispatch creates a dsm segment (header + two shm_mq rings), posts its
// handle on the worker registry's mailbox and waits for frames. The worker
// drains the mailbox while it has capacity, attaches, validates the nonce in
// the header and spawns the session thread. Segments live while a process is
// attached: backend abort and worker exit detach, nothing else is refcounted.
// specs/tla/SessionLifecycle.tla models the handoff and cleanup,
// specs/tla/WorkerPump.tla the main thread's pump and the RPC round trip.

#include "duckdb.hpp"

#include <string>
#include <utility>
#include <vector>

#include "pgddb/pg/declarations.hpp"
#include "pgddb/worker/worker_protocol.hpp"

#include "pgddb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgddb {

// Backend-side handle on a dispatched query. Destroying it releases the session.
class RemoteResult {
public:
	virtual ~RemoteResult() = default;
	// Result column types; blocks until the worker reports them.
	virtual const duckdb::vector<duckdb::LogicalType> &Types() = 0;
	// Next chunk, or nullptr once the result is complete. A worker-side failure
	// is rethrown as a duckdb exception of the same type. The chunk references
	// memory owned by the result: it dies at the next Fetch or when the result
	// is destroyed.
	virtual duckdb::unique_ptr<duckdb::DataChunk> Fetch() = 0;
};

class DuckdbWorker {
public:
	struct Settings {
		const char *name;          // shared memory segment name and bgw_type
		const char *library_name;  // bgw_library_name
		const char *function_name; // bgw_function_name; that function calls Main()
		int *max_sessions;         // worker capacity, read each time a session is admitted
		// Result page arena, read when the worker starts: page size in kB, total
		// pages, and pages reserved for each admitted session (the rest is a
		// shared overflow). Admission also waits for the reserved pages.
		int *page_size_kb;
		int *arena_pages;
		int *reserved_pages;
	};

	explicit DuckdbWorker(Settings settings);
	virtual ~DuckdbWorker() = default;

	// _PG_init: shared memory hooks and backend cleanup callbacks.
	void Init();

	// Backend: ship a query to this database's worker, spawning it if needed.
	// Returns once the query is posted; waiting happens in RemoteResult.
	duckdb::unique_ptr<RemoteResult> Dispatch(QueryFrame frame);

	// Worker process body; the extension's bgworker entry point calls it.
	void Main(Oid database_oid);

	struct Stats {
		int pid;
		int active_sessions;
		uint64_t total_sessions;
	};
	// Registry entry for the current database; false while no worker is registered.
	bool GetStats(Stats &stats);

protected:
	// Worker main thread, once at startup, inside a transaction: create and
	// configure the engine.
	virtual duckdb::DuckDB &Prime() = 0;
	// Worker main thread, outside any transaction, before a session is admitted.
	virtual void
	BeforeSession() {
	}
	// Worker session thread: apply the shipped settings to the session's connection.
	virtual void ConfigureSession(duckdb::Connection &con, const QueryFrame &frame);
	// Backend, inside the dispatching statement: answer an RPC from the worker.
	virtual std::string ServeRequest(const std::string &request) = 0;

private:
	friend struct WorkerInternals;
	Settings settings_;
};

// Hook consulted by the CustomScan node before it prepares a query in-process.
// Returns nullptr to execute in-process.
using DispatchHook = duckdb::unique_ptr<RemoteResult> (*)(const Query *query, ParamListInfo params);
extern DispatchHook pgddb_dispatch_hook;

// PG $n parameters as DuckDB values named "n", for a QueryFrame.
std::vector<std::pair<std::string, duckdb::Value>> DispatchParameters(ParamListInfo params);

// True inside a duckdb worker process.
bool InDuckdbWorker();
// Worker: true on the main thread, the only thread that may call PostgreSQL.
bool OnWorkerMainThread();
// Worker: true if `context` is a dispatched session's connection (or a
// connection aliased to one).
bool HasWorkerSession(duckdb::ClientContext &context);
// Worker, any thread: round-trip a request to the backend that dispatched the
// session owning `context`. Throws once that backend is gone.
std::string WorkerSessionRequest(duckdb::ClientContext &context, const std::string &request);

} // namespace pgddb
