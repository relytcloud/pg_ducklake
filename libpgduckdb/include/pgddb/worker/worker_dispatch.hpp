#pragma once

#include <cstdint>
#include <string>

#include "pgddb/pg/declarations.hpp"

#include <duckdb/common/types/data_chunk.hpp>

namespace pgddb {

// A pull-based stream of result DataChunks produced by external (shared-worker)
// execution. Fetch() returns the next chunk, or nullptr at end of stream; it
// throws a duckdb::Exception on worker-side error or cancellation.
class WorkerResultStream {
public:
	virtual ~WorkerResultStream() = default;
	virtual duckdb::unique_ptr<duckdb::DataChunk> Fetch() = 0;
};

// Installed by an extension. Given a query, returns a result stream when the query
// should run in the shared worker, or nullptr to execute in-process. The CustomScan
// consults it only for non-EXPLAIN statements; the hook deparses the query itself
// (via DeparseQuery) only once it has decided to dispatch, so ineligible queries
// pay no deparse cost.
using DispatchToWorkerHook = duckdb::unique_ptr<WorkerResultStream> (*)(const Query *query);
extern DispatchToWorkerHook pgddb_dispatch_to_worker_hook;

// A pull-based stream of DataChunks produced by running a PG-heap scan on the
// requesting backend (scan inversion). Next() returns the next chunk, or nullptr at
// end of scan. For a COUNT(*) scan, the first chunk's first cell is the BIGINT count.
// `context` and `output_types` (the scan's projected DuckDB column types) let an
// Arrow-backed implementation import a record batch into a chunk of the right types.
class RemoteScanStream {
public:
	virtual ~RemoteScanStream() = default;
	virtual duckdb::unique_ptr<duckdb::DataChunk> Next(duckdb::ClientContext &context,
	                                                   const duckdb::vector<duckdb::LogicalType> &output_types) = 0;
};

// Extra information the kernel scan supplies for a remote scan, computed once during
// init_global (main thread, relation open). Lets a scan worker pool split the scan
// into disjoint CTID block ranges. `nblocks` is the relation's current block count,
// `where_off` is the byte offset in `scan_sql` right after the FROM <relation> clause
// (where a CTID predicate is spliced), and `rangeable` is true when the scan has no
// ORDER BY/LIMIT (Top-N) and is not a COUNT(*), so block-range splitting is safe.
struct RemoteScanInfo {
	uint32_t nblocks;
	uint32_t where_off;
	bool rangeable;
	// True if the relation is backend-local (temporary): such a scan must run on the
	// requesting backend (inversion), never in separate scan-pool processes that
	// cannot see another backend's temp tables.
	bool local_relation;
};

// Installed by an extension running inside the shared worker. When set, a PG-heap
// scan runs its inner SQL (`scan_sql`, referencing the real relation) on the
// requesting backend (or a scan worker pool) instead of via PostgresTableReader, so
// the worker's execution threads make no PostgreSQL calls. nullptr means "run in-process".
// `context` is the executing connection's ClientContext: the scan's init_global runs on a
// DuckDB scheduler thread (not the session thread), so the worker keys the session's
// channel + shipped snapshot by this context rather than by thread-local state.
using OpenRemoteScanHook = duckdb::unique_ptr<RemoteScanStream> (*)(duckdb::ClientContext &context,
                                                                    const std::string &scan_sql, bool count_only,
                                                                    const RemoteScanInfo &info);
extern OpenRemoteScanHook pgddb_open_remote_scan_hook;

} // namespace pgddb
