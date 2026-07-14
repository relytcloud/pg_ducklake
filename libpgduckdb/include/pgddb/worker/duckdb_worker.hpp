#pragma once
// The duckdb worker: everything generic about running a per-database DuckDB
// background worker (registry + spawn, session-pool dispatch, the backend service
// loop, session threads, the scan-producer pool). An extension subclasses
// DuckdbWorker with its identity and engine accessors, constructs one instance,
// and calls Init() from _PG_init.

#include <cstdint>
#include <memory>
#include <string>

#include "pgddb/worker/transport/session_channel.hpp"
#include "pgddb/worker/worker_dispatch.hpp"

#include <duckdb/common/shared_ptr.hpp>

namespace duckdb {
class ClientContext;
class DuckDB;
} // namespace duckdb

namespace pgddb {

class DuckdbWorker {
public:
	struct Settings {
		const char *shmem_name;            // unique ShmemInitStruct key prefix per extension
		const char *bgw_library;           // the extension's shared library name
		const char *bgw_worker_entrypoint; // extern "C" duckdb-worker entrypoint in the extension
		const char *bgw_scan_entrypoint;   // extern "C" scan-worker entrypoint; nullptr = no scan pool
		const char *display_name;          // prefix for bgworker names and log lines

		// GUC value pointers; the extension registers the GUCs, the worker reads the values.
		int *max_sessions; // session-pool slots = concurrent dispatched queries (PGC_POSTMASTER)
		int *arrow_pool_pages;
		int *arrow_page_size;
		int *scan_pool_size;
		int *scan_producers;
	};

	explicit DuckdbWorker(const Settings &settings);
	virtual ~DuckdbWorker() = default;

	// _PG_init: chain the shmem request/startup hooks, register the backend-side
	// cleanup callbacks, and publish this instance for the entrypoints.
	void Init();

	// Backend: open a session to this database's duckdb worker (spawning it on
	// demand), ship the active snapshot and the SQL, and return the result stream.
	// When the session pool is full the backend WAITS for a slot (cancellable, so
	// statement_timeout applies) -- it never falls back to in-process execution,
	// which would recreate the per-backend engine blow-up under exactly the
	// overload the shared worker exists to bound.
	duckdb::unique_ptr<WorkerResultStream> OpenSession(const std::string &sql);

	// Sessions this database's duckdb worker accepted since it started (diagnostics).
	uint64_t DispatchCount() const;

	// Entrypoint bodies. The extension's extern "C" bgworker functions (PostgreSQL
	// resolves them by name) just call these.
	void Main(uint32_t db_oid);
	void ScanWorkerMain(uint32_t db_oid);

	// True inside this extension's duckdb worker process (dispatch gates use it to
	// never recurse into dispatch).
	static bool InWorker();

	// True when the worker is provisioned (max_worker_sessions > 0). When false no
	// worker shared memory is reserved and dispatch must run in-process.
	static bool Enabled();

	// Backend side: serve an extension-specific control frame the worker sent (e.g.
	// pg_ducklake's metadata RPC). Return false if the frame is not handled. Called
	// by the session's result stream while it services the worker.
	virtual bool
	ServeFrame(SessionChannel & /*ch*/, FrameTag /*tag*/, const char * /*data*/, std::size_t /*len*/) {
		return false;
	}

protected:
	// Set per-worker DuckDB config; runs in the worker right after the bgworker's
	// database connection is initialized, before the engine primes.
	virtual void
	Configure() {
	}
	// Initialize the engine once at worker startup (inside a transaction).
	virtual void Prime() = 0;
	// The shared DuckDB instance each session connects to.
	virtual duckdb::DuckDB &Database() = 0;

private:
	friend struct WorkerAccess; // internal: session threads reach the engine accessors
	Settings settings_;
};

// Functions in pgddb::worker run only inside the duckdb worker process.
namespace worker {

// Worker side: the session owning the DuckDB connection `ctx` executes on, or nullptr.
// Code running on DuckDB scheduler threads (where the thread-local CurrentWorkerSession
// is invisible) resolves its session through the executing ClientContext.
SessionChannel *WorkerSessionForContext(duckdb::ClientContext *ctx);

// The session serving the calling thread: the session thread's thread-local, or -- on a
// DuckDB scheduler thread (e.g. a nested query's bind during execution) -- the session
// keyed by `ctx`. nullptr outside a worker session.
SessionChannel *EffectiveWorkerSession(duckdb::ClientContext *ctx);

// Register `ctx` as served by the same session as `primary` (idempotent). A nested
// connection created during execution (e.g. DuckLake's per-transaction metadata
// connection running inlined-data reads) is otherwise invisible to the registry.
// Takes the shared_ptr so the entry holds a weak_ptr: the worker main loop only
// interrupts a context it can lock, never the raw (possibly stale) map key.
// The caller MUST Unalias when the nested connection's owner dies, passing the
// channel the alias was registered under (EffectiveWorkerSession at alias time):
// the entry is erased only if its channel matches, so a later session whose
// context reused the freed address is never evicted.
void AliasWorkerSessionContext(const duckdb::shared_ptr<duckdb::ClientContext> &ctx, duckdb::ClientContext *primary);
void UnaliasWorkerSessionContext(duckdb::ClientContext *ctx, SessionChannel *channel);

} // namespace worker

} // namespace pgddb
