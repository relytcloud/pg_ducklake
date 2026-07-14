# Shared DuckDB worker - glossary

Vocabulary for the shared duckdb worker and its shared-memory transport.

Naming principles: (1) follow PostgreSQL and DuckDB names (backend, worker,
connection); (2) avoid generic words (job, stage, task-on-its-own) - prefer
specific names like "scan task" / "scan range" that carry meaning.

## Roles (processes and threads)

- **backend** - a PostgreSQL backend process serving a client. Decides whether to
  offload a query; while one is offloaded it runs the service loop that answers the
  worker's RPCs and can itself produce scan pages.
- **duckdb worker** - the background worker (one per database) that
  hosts the shared DuckDB instance and executes offloaded queries. PG-free during
  execution: no transaction, no snapshot, no catalog access.
- **DuckdbWorker base class** - the kernel machinery an extension embeds to get an engine
  worker: registry + spawn, session pool, service loop, session threads, scan
  producer pool (`duckdb_worker.cpp`). The extension subclass supplies the dispatch
  gate, entrypoints, GUCs, and any extra frames.
- **session thread** - one thread in the duckdb worker per in-flight session; owns
  that session's DuckDB connection. PG touches are serialized behind the global
  process lock.
- **scan worker** - a background worker, in a bounded per-database pool, that only
  executes scan ranges and produces scan pages.

## Units of work

- **session** (duckdb query) - one query a backend dispatches over a connection
  slot: Snapshot + Sql in, Chunk*/Complete or Error out, plus the RPC traffic in
  between. One session per slot at a time.
- **scan task** - a PostgreSQL heap scan (the inner SQL over a real relation) that
  the worker sends back out to be run against PostgreSQL - to the backend
  (inversion) or to scan workers (pool). Identified by a scan_id.
- **scan range** - a contiguous CTID block range `[lo, hi)` of a scan task: the
  unit a scan worker claims from the scan queue. All of a task's ranges feed that
  task's single scan ring.
- **scan page** - one filled Arrow record batch, held in a page from the page pool.
- **shipped snapshot** - the backend's serialized MVCC snapshot, sent with a
  session. The worker never restores it; it re-ships it to scan workers so all
  producers read the dispatching backend's view.

## Shared-memory structures (worker/transport)

All fixed-size regions in main shared memory, created at postmaster start. There
is no dsm segment anywhere in the transport.

- **session pool** - the fixed array of session slots
  (`session_pool.cpp`). `Acquire` takes a FREE slot and bumps its generation;
  each side `AttachEnd`s / `DetachEnd`s, and the detach that drops the attach
  refcount to zero frees the slot.
- **session slot** - one pool entry: the slot state, a **generation**
  (bumped on every `Acquire`, so a stale reference to a released or re-acquired
  slot is detectable), the attach refcount, plus a fixed session-channel region,
  reused across sessions.
- **session channel** - the per-slot transport: a header (cooperative cancel flag)
  plus the control queue and the result queue (`session_channel.cpp`).
- **control queue** - shm_mq ring, backend -> worker: Snapshot, Sql, and the
  backend's RPC replies (scan replies, MetaResult/MetaError, DescribeRel replies).
- **result queue** - shm_mq ring, worker -> backend: result Chunk*/Complete/Error
  and the worker's RPC requests (ScanFetch, MetaQuery, MetaExec, DescribeRel).
- **pending-session ring** - per duckdb worker (in its registry slot): entries of
  {session slot index, slot generation} enqueued by backends, drained by the
  worker main loop, which spawns a session thread per entry. The thread attaches
  via `TryAttachEnd(idx, generation)`, so a stale entry (its backend released the
  slot after enqueueing) is skipped rather than served.
- **page pool** - the global fixed pool of equal-size scan pages; the zero-copy
  Arrow area. A producer *acquires* a page; the consumer *releases* it once DuckDB
  drops the chunk.
- **scan queue** - the global MPMC queue of scan ranges awaiting a scan worker;
  entries are tagged with db oid + scan_id + generation.
- **scan ring** - per scan task; an MPSC ring of ready scan pages. Producers push;
  the worker's DuckDB threads drain. Generation-guarded for teardown.

## Worker-side session registry

- **session context registry** - the worker's map from the executing DuckDB
  `ClientContext` to that session's channel + shipped snapshot. Needed because a
  scan's `init_global` runs on a DuckDB scheduler thread, where the session
  thread's thread-locals are invisible.
- **nested-connection alias** - a second registry key for the same session:
  DuckLake's per-transaction metadata connection is a separate `ClientContext`,
  so pg_ducklake aliases it onto the owning session's entry
  (`AliasWorkerSessionContext`) before inlined-data reads and unaliases it when
  the metadata manager (and so the transaction) is destroyed. A stale alias
  would misroute a later session that reuses the freed context address.

## Channel demux (worker side)

- **demux lane** - the worker-side per-scan reply queue: `RoutedRecv` pulls frames
  off the control queue and routes each to its lane, so several scans and metadata
  RPCs share one channel concurrently. Frames keep wire order within a lane.
- **scan_id-prefixed reply** - every scan reply (ScanChunk/ScanChunkArrow/
  ScanDone/ScanError) carries a uint32 scan_id prefix; the prefix selects the lane
  and is stripped while routing.
- **meta lane** - the single lane for metadata/catalog replies
  (MetaResult/MetaError, DescribeRelResult/DescribeRelError). Metadata round-trips
  are serialized per channel by the meta-request mutex, so replies need no id.

## RPCs (worker -> backend)

- **ScanFetch** - inversion path: "produce the next chunk of scan_id"; the first
  fetch carries count_only + the inner SQL.
- **DescribeRel** - catalog RPC: resolve schema.table into a serialized
  RelationDesc (oid, temp-ness, cardinality, blocks, columns + types) under the
  backend's snapshot.
- **MetaQuery** - pg_ducklake: run DuckLake metadata read SQL on the backend,
  under its snapshot; reply is a serialized QueryResult.
- **MetaExec** - pg_ducklake: run a DuckLake metadata commit write on the backend,
  inside the backend's transaction, so a dispatched DML's metadata commit is
  atomic with the statement. MetaError replies carry the DuckDB exception type.

## Not shared memory

- **arrow codec** - the direct PG-datum -> Arrow producer and the Arrow -> DuckDB
  DataChunk consumer that write and read scan pages (`arrow_codec.cpp`).
