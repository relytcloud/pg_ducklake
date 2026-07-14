# Design: the shared per-database duckdb worker

Status: implemented. Vocabulary: see GLOSSARY.md. Formal models of the
concurrency cores: `specs/tla/`.

## Motivation

Without the worker, every backend that offloads a query owns a private DuckDB
instance, so N concurrent clients cost N duckdb memory budgets -- the per-backend
OOM. The shared worker runs **one DuckDB instance per database** in a background
worker process and lets backends dispatch queries to it, bounding memory to one
engine per database regardless of client count.

The worker serves N sessions concurrently. That is possible because worker-side
query execution is **PG-free**: the worker holds no PG transaction and no snapshot,
so no "one process, one snapshot" limit applies. Everything PostgreSQL -- catalog
lookups, DuckLake metadata SQL, heap reads -- runs on the requesting backend (or on
scan workers) under that backend's own MVCC view, reached over shared memory.

## Roles

- **backend** -- the PostgreSQL backend running the client query. Dispatches the
  deparsed query to the worker, then sits in a service loop
  (`BackendSession::Fetch`, duckdb_worker.cpp): it answers the worker's
  catalog / metadata / scan requests and drains result chunks until the query ends.
- **duckdb worker** -- one background worker per database (spawned on demand by the
  first dispatching backend). Hosts the shared DuckDB instance and runs a thread
  per session, each on its own DuckDB connection. No transaction, no snapshot,
  pure compute; anything PG is an RPC back to the backend.
- **scan worker** -- a bounded per-database pool of background workers (own DB
  connection) that execute heap-scan block ranges in parallel and produce Arrow
  scan pages. Spawned by the duckdb worker at startup; never runs DuckDB.

Free functions that run in exactly one of these processes are namespaced by role:
`pgddb::backend`, `pgddb::worker`, and `pgddb::producer` (scan workers). Code
callable from several processes (the `DuckdbWorker` class, transport primitives,
protocol codecs, the shared scan producer) stays directly in `pgddb`.

## DuckdbWorker base class vs. extension subclass

The generic machinery is the kernel **DuckdbWorker base class** (`worker/duckdb_worker.cpp`,
`include/pgddb/worker/duckdb_worker.hpp`): worker registry + spawn, session pool,
backend service loop, session threads, scan inversion + producer pool, cleanup and
failure handling. An extension supplies a `DuckdbWorker::Settings` (shmem key prefix,
bgworker entrypoint names, GUC value pointers, an optional `serve_frame` handler
for extension-specific frames) and `the DuckdbWorker virtuals` (`configure`, `prime`,
`database`). The adapters are thin:

- `pg_duckdb/src/shared_engine_worker.cpp` -- dispatch gate: read-only heap SELECTs
  when `duckdb.use_shared_worker` is on; no extra frames.
- `pg_ducklake/src/shared_engine_worker.cpp` -- dispatch gate: SELECT plus
  single-statement autocommit DML; serves the DuckLake metadata RPC
  (MetaQuery/MetaExec) on the backend.

Each extension gets its own registry, pool, and DuckDB instance (distinct shmem
keys), so two extensions coexist in one cluster without sharing anything.

## Session pool

A fixed array of `max_worker_sessions` **session slots** in main shared memory
(`worker/transport/session_pool.cpp`), sized at postmaster start. No dsm. Each
slot owns a fixed **session channel** region: a header (the cooperative cancel
flag) plus two `shm_mq` rings -- the **control queue** (backend -> worker, 64 KB)
and the **result queue** (worker -> backend, 256 KB).

Slot lifecycle is a refcount: `Acquire` takes a FREE slot (first-fit, under the
pool spinlock) and bumps the slot's **generation**; each side calls `AttachEnd`
when it maps the channel and `DetachEnd` when done; the detach that drops the
count to zero returns the slot to FREE. The backend acquires and attaches in one
go in `DuckdbWorker::OpenSession` (nothing that can fail sits between them),
recreates the rings in place (`SessionChannel::OpenSlot`), and enqueues
`PendingSession {slot index, slot generation}` on the worker's **pending-session
ring** (in the worker registry slot, capacity 1024). The worker's session thread
attaches via `TryAttachEnd(idx, generation)`, which succeeds only if the slot is
still IN_USE at the enqueued generation -- a stale entry, whose backend released
the slot (or whose slot was re-acquired) after enqueueing, is skipped instead of
attaching someone else's session. A full pool or a full pending ring makes the
backend wait, cancellably, for a slot -- dispatch never falls back to in-process
execution for capacity reasons, since a per-backend engine per overflow query is
exactly the memory growth the worker exists to bound.

## Worker registry, spawn, respawn

`DuckdbWorkerShmem` holds `MAX_DUCKDB_WORKERS` (16) per-database slots: db oid,
worker pid + latch, a dispatched counter (diagnostics), and the pending ring.
`EnsureWorkerForMyDatabase` finds or spawns the worker for `MyDatabaseId`; a slot
whose pid no longer answers `kill(pid, 0)` (crash, SIGKILL) is reclaimed and the
worker respawned. The worker publishes its pid + latch into the slot early in its
main; backends poll the slot rather than `WaitForBackgroundWorkerStartup`.

## Frame protocol and channel demux

Frames are tagged (`FrameTag`, session_channel.hpp). Directions:

- control queue (backend -> worker): `Snapshot`, `Sql`, then RPC **replies**:
  `ScanChunk`/`ScanChunkArrow`/`ScanDone`/`ScanError`, `MetaResult`/`MetaError`,
  `DescribeRelResult`/`DescribeRelError`.
- result queue (worker -> backend): `Chunk*` then `Complete`, or `Error`; plus the
  worker's RPC **requests**: `ScanFetch`, `MetaQuery`, `MetaExec`, `DescribeRel`.

Several scans and metadata round-trips share one channel concurrently, so the
worker side demultiplexes the control queue (`SessionChannel::RoutedRecv`,
session_channel.cpp): every scan reply carries a `uint32 scan_id` prefix
(`SendScanReply`, duckdb_worker.cpp) and is routed into a per-scan **demux lane**;
metadata/catalog replies go to the single **meta lane**. `RoutedRecv` holds the
demux lock across receive + route, so each lane preserves wire order; the raw
`shm_mq` poke nests inside the global process lock. Metadata round-trips are
serialized per channel by `MetaRequestMutex` (replies carry no request id, so at
most one is in flight). `CloseScanLane` frees the Arrow pages of frames routed to
a lane but never consumed.

This demux lifts the old constraint that two scans on one channel were only safe
because DuckDB drove them sequentially: concurrent scans (a join's two inputs on
different DuckDB threads) and mid-execution metadata RPCs (DuckLake
`GetFilesForTable` on a scheduler thread) are now safe. Modeled and checked in
`specs/tla/ScanInversion.tla`.

## Catalog RPC

The worker never opens PG catalogs. Binding a PG relation asks the backend via
`DescribeRel` (`WorkerDescribeRelation`, session_protocol.cpp): the backend
answers with a serialized `RelationDesc` (oid, names, temp-ness, cardinality,
block count, columns with DuckDB types) resolved under its own snapshot.

## pg_ducklake metadata RPC

DuckLake metadata SQL runs on the backend, inside the backend's transaction:

- `MetaQuery` -- metadata reads (`ExecuteMetadataQueryLocally`), under the
  backend's snapshot.
- `MetaExec` -- the commit write path (`ExecuteMetadataExecLocally`). Because it
  executes in the backend's transaction, a dispatched DML's DuckLake metadata
  commit is atomic with the backend statement: the dispatch gate only accepts a
  single autocommit statement, so statement commit and metadata commit coincide.

Replies are serialized `QueryResult`s; `MetaError` carries the DuckDB exception
type byte so e.g. a `TransactionException` survives the wire and triggers the
DuckLake conflict-retry on the worker.

## Scan execution: inversion and the producer pool

Heap reads never run in the worker. The kernel remote-scan hook (`OpenRemoteScan`,
duckdb_worker.cpp -- installed only inside the worker) picks one of two paths:

- **scan inversion** (`InversionScanStream`, scan_producer.cpp): the worker sends
  `ScanFetch` requests (first one carries `count_only` + the inner SQL) and keeps a
  read-ahead window of 8 outstanding; the backend (`ServiceScanFetch`,
  duckdb_worker.cpp) runs the scan via `PostgresTableReader` and replies one chunk
  per fetch -- an Arrow batch in a global pool page (`ScanChunkArrow`, zero-copy)
  or, for COUNT(*), one inline serialized chunk. A finished/errored scan is kept
  (not re-opened) so windowed extra fetches get the same terminal reply. Used for
  temp tables (backend-local) and as the fallback when the pool is unavailable.
- **scan producer pool** (`PoolScanStream` + `ProcessScanRange`,
  scan_producer.cpp): the scan is registered in the shared `ScanRing` with its
  inner SQL + the shipped snapshot, split into CTID block ranges (>= 64 blocks
  each, up to `scan_producers` ranges), and the ranges are enqueued on the global
  `ScanQueue`. Scan workers claim ranges, splice a `ctid >= .. AND ctid < ..`
  predicate into the SQL, read under the restored snapshot, and push Arrow pages
  into the scan's ready-ring; the worker's DuckDB threads drain it with no
  per-chunk round-trip. Generation guards make teardown (LIMIT, cancel, error)
  drop in-flight ranges; `ScanRing::Close` reclaims queued pages. Modeled in
  `specs/tla/ScanPool.tla`.

The data transport is Arrow-only (`arrow_codec.cpp`); an unsupported column type
fails the scan loudly rather than silently falling back.

## Snapshot model

The worker holds no PG transaction. It primes the engine exactly once at startup
inside a transaction (extensions, secrets, catalog attach); after that every
session runs transaction-free. The backend ships its active snapshot with the
query (`Snapshot` frame); the worker never restores it -- it only re-ships it to
scan-worker tasks (via the ScanRing), which restore it so all producers read the
dispatching backend's MVCC view. The backend itself answers inversion fetches and
metadata RPCs under its own live transaction. The dispatch gate refuses to
dispatch when the current transaction has already written (the shipped snapshot
could not see those changes).

## Worker threading

A PostgreSQL background worker is single-threaded by contract, so the worker runs
a **thread per session** with every PG-non-thread-safe touch (shm_mq, palloc)
serialized behind the **global process lock**, taken as a non-blocking poke with
the lock dropped between attempts (`SerializedSendResult` /
`SerializedRecvControl` / the recv inside `RoutedRecv`). `MyLatch` stays owned
solely by the worker's main loop. Each session's channel + shipped snapshot are
registered keyed by the executing DuckDB `ClientContext` (`g_session_ctx`),
because a scan's `init_global` runs on a DuckDB scheduler thread where
thread-locals are invisible; binding-phase code still uses the thread-local
`CurrentWorkerSession`, which is correct because binding runs on the session
thread during `con.SendQuery`.

One registry subtlety: DuckLake's per-transaction metadata connection
(`DuckLakeTransaction::GetConnection`) is a separate `ClientContext`.
pg_ducklake's metadata manager **aliases** that nested context onto the owning
session's registry entry (`AliasWorkerSessionContext`) before inlined-data reads
(`ReadInlinedData` / `ReadAllInlinedDataForFlush` run `ExecuteRaw` on the nested
connection, which opens remote heap scans that must invert to the same backend),
and unaliases it in the metadata manager's destructor. The alias must not
outlive the transaction: a freed `ClientContext` address can be reused by a
later session, and a stale alias would route that session's scans to the wrong
backend.

## Lifecycle and failure handling

- **dispatch**: backend ensures the worker, acquires + attaches a slot, enqueues it
  on the pending ring, pokes the worker latch, ships `Snapshot` + `Sql`, and enters
  the service loop. The worker main loop drains the pending ring and spawns a
  session thread per entry (attach, run, detach); finished threads are reaped.
- **backend abort/exit**: open streams are tracked in a backend-local registry;
  the xact-abort callback and `before_shmem_exit` release all of them (cancel the
  worker, detach, `DetachEnd`). Cleanup is **subtransaction-scoped**: each stream
  records `GetCurrentSubTransactionId()` at creation, and a
  `SUBXACT_EVENT_ABORT_SUB` releases only streams created in the aborting
  subtransaction -- a subtransaction abort can be an internal, handled event
  (the backend services a worker's `MetaExec` in a subtransaction that aborts on
  a DuckLake commit conflict and is retried), and killing the servicing stream
  there would free the channel out from under it. A slot claimed but not yet
  owned by a stream is covered by `g_pending_conn_slot`; a pending-ring entry
  whose backend already released the slot is rejected by the worker's
  generation check.
- **cancellation**: the backend sets the channel's cancel flag and pokes the worker
  latch; the worker main loop `Interrupt()`s the `ClientContext` of any session
  whose flag is set, so the query aborts promptly. Worker-side sends/recvs also
  poll the flag and bail.
- **worker death**: the backend's fetch loop probes the worker pid when idle and
  errors out instead of spinning; `EnsureWorkerForMyDatabase` reclaims the dead
  registry slot so the next dispatch respawns the worker.
- **worker shutdown**: on SIGTERM a custom handler only flags + wakes, and the
  main loop sets the draining flag (unblocking session-thread polls) and joins
  all session threads before exiting. The same drain also runs from a
  `before_shmem_exit` callback, so ERROR/FATAL exits -- which `proc_exit`
  without returning through the main loop -- still join every session thread
  and `DetachEnd` cleanly. Hard kills (SIGKILL, segfault) skip the callback,
  but they trigger a postmaster crash-restart that reinitializes shared memory,
  so no refcounts survive them either.
- **capacity waits**: pool full, pending ring full, or worker mid-respawn at enqueue
  time make `DuckdbWorker::OpenSession` wait (interrupt checks between retries, so
  cancel and statement_timeout apply) and, for a dead worker, respawn it. In-process
  execution happens only for statements the dispatch gate excludes by semantics.

The slot/refcount lifecycle is modeled in `specs/tla/ControlProtocol.tla`.

## Known limits

- Data transport is Arrow-only. Covered types (`arrow_codec.cpp`): int2/int4/
  int8, float4/float8, date, bool (bit-packed), timestamp, timestamptz,
  text/varchar, bytea/BLOB (bytea matters because DuckLake inlined-data tables
  store text columns as bytea), and numeric/decimal (32/64/128-bit). Any other
  column type (or > 64 columns) fails the scan loudly. The Arrow page pool must
  be configured (`arrow_pool_pages > 0`) for data scans.
- The engine (secrets, extensions, DuckLake attach) is primed once at worker
  start; secrets created later are not seen until the worker restarts.
- A DuckDB `InternalException` in any session invalidates the shared engine
  (DuckDB marks the database instance invalid), so every session fails until
  the worker is restarted and re-primes.
- At most `MAX_DUCKDB_WORKERS` = 16 databases can have a live duckdb worker.
- pg_ducklake DML dispatch is gated to a single autocommit statement without
  RETURNING (so the statement and its metadata commit coincide); pg_duckdb
  dispatches SELECT only. Neither dispatches after the transaction has written.
- Metadata round-trips are serialized per channel (one in flight); scans are the
  concurrent fast path.
- Provisioning is opt-in: `max_worker_sessions` defaults to 0, which disables the
  worker and reserves no shared memory (registry, session pool, Arrow page pool,
  scan queue, scan ring). Enabling it requires a postmaster restart, since the
  shmem request runs at postmaster start.
- Dispatch bounds **execution** memory only. Every dispatching backend still creates
  its own DuckDB instance to plan and bind (and for the gate fallback), and each
  dispatched query is planned in-process before dispatch, so the plan/bind memory is
  not shared. Moving plan/bind into the worker is future work.
- Scan producers take their own AccessShare locks (they are not in the dispatching
  backend's lock group, unlike PG parallel workers), so a pending exclusive lock can
  queue a producer behind DDL that is itself queued behind the dispatching backend --
  a cycle whose backend edge is a latch wait, invisible to the deadlock detector. It
  is cancellable; set `lock_timeout`/`statement_timeout` where concurrent DDL is
  expected. Producer-side bounded lock waits are future work.
- MetaExec durability window: the worker's in-memory DuckLake state advances when
  MetaExec returns, but durability is the backend's later commit. If the backend dies
  before committing, the metadata rolls back while the shared engine's caches may have
  advanced; later sessions re-read metadata under their own snapshots, which
  re-converges. No cross-session state is trusted without a metadata read.
- A slow result consumer holds its session slot for the whole statement, pinning
  1/`max_worker_sessions` of the database's dispatch capacity.
