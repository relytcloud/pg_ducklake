# TLA+ models of the shared DuckDB duckdb worker

Formal models of the three concurrency cores of the shared per-database DuckDB
duckdb worker (design: `libpgduckdb/worker/DESIGN.md`, vocabulary:
`libpgduckdb/worker/GLOSSARY.md`). Each TLA+ action is commented with the C++
operation it stands for. Constants are kept small so every check finishes in
seconds.

## Run

```sh
JAR=<path>/tla2tools.jar   # e.g. the VS Code/Cursor TLA+ extension's tools/tla2tools.jar
cd specs/tla
java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -config <cfg> -workers auto <spec>.tla
```

Expected-PASS configs report "No error has been found"; the negative configs
fail exactly as described below.

---

# ControlProtocol.tla: session-pool session lifecycle

Models the lifecycle by which a backend gets a session onto the worker and
the session slot is returned (`DuckdbWorker::OpenSession` / the worker main
loop / `SessionThreadMain` in `libpgduckdb/worker/duckdb_worker.cpp`, refcounts
in `libpgduckdb/worker/transport/session_pool.cpp`): slot Acquire (which
bumps the slot generation) + the backend's AttachEnd + channel OpenSlot (which
resets the header cancel flag), the pending-session ring enqueue of
`{slot, generation}`, the backend's HANDSHAKE send (Snapshot + SQL frames,
after which the result stream exists), the worker draining the ring and a
session thread attaching via `TryAttachEnd(idx, generation)` (stale entries
are skipped) and then PARKING in the handshake receive, both ends
DetachEnd-ing, and the detach that reaches refcount 0 freeing the slot.

The handshake receive is modelled as it is coded (`SerializedRecvControl` in
`RunOneSession`): a nowait-recv poll that observes EITHER a received frame
(`HandshakeOk`) OR the channel cancel flag / worker drain
(`HandshakeCancelled`, on which the thread detaches without running). The
backend's abort-while-pending cleanup is correspondingly split into its two
real steps (`ReleaseOpenSessions`, pending-slot branch): FIRST
`SessionChannel::RequestCancelAt` sets the cancel flag, THEN `DetachEnd`
releases the backend's own refcount. The `AbortSetsCancel` constant drops the
first step to reproduce the pre-fix wedge (C1): a session thread already
parked in the handshake then polls a frame that never arrives, observes no
cancel, and its refcount pins the slot IN_USE forever.

Capacity never falls back to in-process execution: `OpenSession` WAITS. A full
pool means the backend retries `Acquire` in a cancellable loop (interrupt
check + 10ms latch wait) until a slot frees; a full pending ring or a missing
worker at enqueue time means the backend retries the enqueue, respawning a
dead worker via `EnsureWorkerForMyDatabase` first. In the model a waiting
backend simply has no enabled transition until capacity appears (weak fairness
on its acquire/enqueue actions is the retry loop); in-process execution exists
only for statements the dispatch gate excludes semantically, outside this
model. Failure surface: backend abort in every phase (the xact-abort /
shmem-exit cleanup callbacks) -- during the pool-full wait (nothing held yet),
during the ring-full wait (the claimed slot is released), and while pending
(the two-step cancel+detach above, racing an already-attached thread) -- plus
worker crash + registry reclaim/respawn (the pid probe in
`EnsureWorkerForMyDatabase`).

The data plane is modelled separately in ScanInversion.tla.

## What it checks

- `NoUnderflow` / `RefcountBounded` (safety): the attach refcount stays in
  0..2 -- no DetachEnd without a matching attach (which would wrap the C++
  uint32), at most the two legitimate ends.
- `FreeImpliesUnattached` (safety): a FREE slot has no attached ends --
  neither running (`wheld`) nor parked in the handshake (`wwait`) -- no
  double-free, no attach to a slot that was already returned.
- `SingleBackendPerSlot` (safety): two live backends never hold one slot.
- `NoSlotLeak` (liveness): the pool eventually returns (and stays) fully FREE
  -- including a slot whose session thread was parked in the handshake when
  its backend aborted (the C1 fix is load-bearing here).
- `SessionSettles` (liveness): every backend -- including one waiting out a
  full pool or a full pending ring -- is eventually served to completion or
  settles cleanly (abort, worker-death error). No starved wait, no backend
  blocked forever, no enqueued session silently lost.

## Configs

- `ControlProtocol.cfg` -- the real protocol: 3 backends, 2 slots, pending ring
  of 1, so both capacity WAITS are exercised (one backend waits for a free
  slot, one waits holding its slot for the ring to drain); aborts allowed in
  EVERY backend phase including during either wait and while PENDING (the
  stale ring entry the generation check must reject, plus the parked-thread
  race the cancel flag must resolve), `AbortSetsCancel = TRUE`,
  `ValidateGeneration = TRUE`, no worker crash. **PASS** -- including
  `SessionSettles`/`Liveness`, i.e. the waits cannot starve and the handshake
  cannot wedge. Last run: 19,994 states generated, 6,335 distinct, no error.
- `ControlProtocol_handshake_bug.cfg` -- the pre-fix abort-during-handshake
  wedge (C1). `AbortSetsCancel = FALSE`: the old pending-abort path only
  DetachEnd-ed. A session thread attaches (valid entry, right generation) and
  parks in the handshake receive; the backend aborts before sending the
  Snapshot/SQL frames; the parked thread never sees a frame and never sees a
  cancel, so its refcount pins the slot. **FAILS: deadlock at the
  wedged-thread state** (all backends settled, one slot INUSE at refcount 1
  with a thread parked in `wwait`) -- equivalently `NoSlotLeak`/`Liveness`.
  The fix (`RequestCancelAt` before `DetachEnd` in `ReleaseOpenSessions`;
  the handshake poll returns Detached on cancel) is what the passing main
  config exercises under the same adversary.
  Last run: deadlock trace found (trace length and state counts at detection
  vary with -workers).
- `ControlProtocol_staleentry_bug.cfg` -- the pre-fix stale-entry race
  (`ValidateGeneration = FALSE`: the old `SessionThreadMain` attached the
  enqueued slot index unconditionally). A backend abort while PENDING frees
  the slot but leaves its ring entry; serving it attaches a FREE or
  re-acquired slot. **FAILS `FreeImpliesUnattached`** (other interleavings
  reach refcount 3 or an underflow). This race was found by this model and is
  now FIXED in the code with slot generations + `TryAttachEnd` -- the passing
  main config checks the fix under the same adversary.
  Last run: violation found at depth 6 (state counts at detection vary with
  -workers).
- `ControlProtocol_workercrash.cfg` -- counterfactual HARD worker kill: session
  threads (parked or running) vanish without DetachEnd and no shmem reset
  follows, so a slot whose worker end was attached at kill time keeps
  refcount 1 forever. **FAILS: deadlock at the leaked terminal state**
  (equivalently `NoSlotLeak`/`Liveness`). What the real code covers:
  ERROR/FATAL worker exits run a `before_shmem_exit` drain that joins every
  session thread (each DetachEnds) -- those are ordinary `SessionFinish`
  steps in the passing config; SIGKILL/segfault skip the callback but trigger
  a postmaster crash-restart that reinitializes shared memory. The modeled
  leak is the state that would persist if neither mechanism existed -- i.e.
  why the drain matters. Kill *before* the worker attached is handled cleanly
  either way (backend pid-probes and detaches to 0).
  Last run: deadlock trace found (state counts at detection vary with
  -workers).
- `ControlProtocol_bug.cfg` -- the bug class the implemented design prevents:
  Acquire and the backend's AttachEnd as two steps with an untracked crash
  window between them. **FAILS: deadlock at a leaked slot** (INUSE, refcount
  0, nobody will ever detach it). The real code closes the window by putting
  nothing fallible between Acquire and AttachEnd (the snapshot palloc happens
  before Acquire) and tracking the slot for cleanup from the attach onward
  (`g_pending_session_slot`, then the open-stream registry).
  Last run: deadlock trace found (state counts at detection vary with
  -workers).

---

# ScanInversion.tla: scan read-ahead + channel demux

Models the scan-inversion sub-protocol *and the channel demux*:
`InversionScanStream::Next` (worker consumer, `worker/scan_producer.cpp`)
talking to `BackendSession::ServiceScanFetch`/`SendScanReply` (backend
producer, `worker/duckdb_worker.cpp`) over the session channel, with replies
routed by `SessionChannel::RoutedRecv` (`worker/transport/session_channel.cpp`).
Every scan reply carries a uint32 scan_id prefix and is demultiplexed into a
per-scan lane; metadata replies go to the single meta lane. This lifted the
old constraint that two scans on one channel were only safe if DuckDB drove
them sequentially: the model checks two scans running CONCURRENTLY, plus
an interleaved metadata round-trip (`MetadataRoundTrip` in
`session_protocol.cpp`, serialized per channel by `MetaRequestMutex`).

Teardown is modelled as it is coded: the destructor drains outstanding
fetches, but the drain can BAIL (Detached on backend cancel / worker drain),
so `CloseScanLane` can run with replies still on the wire. The close
TOMBSTONES the scan_id (`closed_scans`, the C2a fix) -- `WorkerCloseLane`
moves the stream to a terminal "closed" state -- and `RoutedRecv` DROPS a
late reply for a tombstoned scan, freeing its Arrow page, instead of
re-creating ("resurrecting") the lane and pinning the page forever. The
`DropLateReplies` constant reverts to the pre-fix resurrection.
(`~SessionChannel` also frees pages of frames still buffered in live lanes;
channel teardown is below this model's granularity -- the close covers it.)

## What it checks

- `WindowBounded` / `RequestsBounded` (safety): at most W (WINDOW) fetches
  outstanding per scan; the result FIFO holds at most W per scan + 1 meta
  request.
- `FIFOMatching` (safety): each scan consumes exactly the 0,1,2,... prefix of
  its own produced chunks -- no reorder, loss, duplication, or cross-scan
  mis-route. The demux guarantees this for concurrent scans; `RouteByScanId =
  FALSE` shows it breaking without the demux.
- `MetaLaneClean` (safety): routing never puts a scan reply on the meta lane
  or vice versa.
- `ClosedLaneEmpty` (safety): a closed lane stays empty -- a late reply is
  dropped (page freed), never resurrects the lane. This is the C2a tombstone;
  `DropLateReplies = FALSE` shows it breaking.
- `PageConservation` + the `Quiescent` terminal (safety + liveness): every
  Arrow page handed out is released -- by the import, by `DrainOutstanding`,
  by `CloseScanLane` freeing routed-but-unconsumed frames, or by the
  tombstone dropping a late reply.
- `TerminalStableInv` / `TerminalStable` (safety): a finished/errored backend
  scan is kept, not re-opened; extra windowed fetches get the same terminal.
- `Termination` (liveness): the protocol always reaches the clean terminal
  (every scan's lane closed, FIFOs drained, lanes empty, pages reclaimed, no
  metadata round-trip stuck).

## Configs

- `ScanInversion.cfg` -- one scan, W=2, 2 chunks + ScanDone, Arrow on (1 pool
  page, so fast path and inline fallback both occur), early teardown allowed,
  `DropLateReplies = TRUE` -- includes closes with fetches outstanding, whose
  late replies the tombstone drops. **PASS.**
  Last run: 262 states generated, 139 distinct, no error.
- `ScanInversion_twoscan.cfg` -- two scans CONCURRENT on one channel (a join's
  two inputs; no sequencing assumption) plus one metadata round-trip
  interleaving (a DuckLake GetFilesForTable from a scheduler thread), Arrow +
  teardown on. Replaces the old `ScanInversion_twoscan_seq.cfg`, whose
  "sequential drive" assumption the demux made unnecessary. **PASS.**
  Last run: 493,137 states generated, 136,692 distinct, no error (~10 s).
- `ScanInversion_noroute_bug.cfg` -- the pre-demux bug class (`RouteByScanId =
  FALSE`): a scan consumes the wire head no matter which scan it was intended
  for. Replaces the old `ScanInversion_alias.cfg`. **FAILS `FIFOMatching`**
  with a cross-scan mis-routing trace -- exactly what the scan_id prefix +
  demux lanes prevent. Last run: violation found at depth 10 (state counts at
  detection vary with -workers).
- `ScanInversion_lateclose_bug.cfg` -- the pre-fix late-reply resurrection
  (C2a): `DropLateReplies = FALSE`. A chunk carrying a pool page is still on
  the wire when the torn-down scan's drain bails and `CloseScanLane` runs;
  routing the late reply re-creates the closed lane, where nothing will ever
  consume it -- the page is pinned forever. **FAILS `ClosedLaneEmpty`** (the
  resurrected lane; `Termination` would equally flag the never-freed page).
  Last run: violation found at depth 10 (state counts at detection vary with
  -workers).

---

# ScanPool.tla: shared scan-producer pool

Models the page lifecycle and the producer/consumer/teardown protocol for one
pool scan: scan workers (`ProcessScanRange` in `worker/scan_producer.cpp`,
spawned by `DuckdbWorker::ScanWorkerMain` in `worker/duckdb_worker.cpp`) produce
Arrow pages into the scan's ready-ring (`worker/transport/scan_ring.cpp`,
pages from `worker/transport/page_pool.cpp`); the worker's consumer
(`PoolScanStream::Next`) drains it via `ScanRing::TryNext` and
`ScanRing::Close` reclaims queued pages on teardown.

Producer failure comes in two modelled flavours matching the code:

- `ProducerError` -- a PG ERROR inside `ProcessScanRange`; the PG_TRY there
  releases the held page and calls `ScanRing::SetError`. Always reported.
- `ProducerFatal` -- the scan-worker PROCESS dies (elog FATAL, SIGTERM
  mid-task). FATAL skips PG_CATCH but runs shmem-exit callbacks: the
  `before_shmem_exit` hook in `ScanWorkerMain` errors out the active range
  (`g_active_range` -> `SetError`). The `FatalReportsError` constant toggles
  the hook; FALSE is the pre-fix silent death and the consumer hang.

Cancellation / worker drain is a separate signal (`stopReq`): scan workers do
not poll the session cancel flag, and on a worker drain a QUEUED task may
never be claimed (`StopExit`: no TaskDone, no error). The fixed consumer polls
`IsCancelRequested() || IsWorkerDraining()` in its nothing-ready branch
(`AbortOnCancel`) and errors out, closing the ring; the `ObserveCancel`
constant toggles the poll -- FALSE is the pre-fix consumer that spins forever
on a ring nobody will fill.

(A FATAL while a producer holds a pool page additionally leaks that page --
no callback releases producer-held pages; `ProducerFatal` is modelled between
pages, and the leak is a separate known gap outside this model.)

## What it checks

- `PageConservation` (safety): the four page locations -- free stack, held by a
  producer, queued in the ready-ring, in-flight in the consumer -- always
  partition the pool. Catches page leaks and double-free.
- `RingBounded` (safety): the ready-ring never exceeds its capacity.
- `Termination` (liveness): the scan always reaches a quiescent terminal --
  consumer finished or aborted, every producer stopped, every page back on the
  free stack -- under errors, FATAL deaths, and cancellation with tasks still
  outstanding.

## Configs

- `ScanPool.cfg` -- the fixed protocol under the full failure adversary:
  PG ERRORs (reported via PG_TRY), FATAL deaths (reported via the shmem-exit
  hook, `FatalReportsError = TRUE`), stop requests with abandoned tasks
  (observed by the consumer poll, `ObserveCancel = TRUE`), no external
  rescue. **PASS.** Last run: 3,905 states generated, 1,071 distinct.
- `ScanPool_bug.cfg` -- pre-fix silent producer death: `FatalReportsError =
  FALSE` (no shmem-exit SetError), all other failures off. **FAILS: deadlock**
  at the hung state (`done < NP`, no error, ring empty, consumer draining):
  the consumer polls forever. This is the hang the `before_shmem_exit` hook
  in `ScanWorkerMain` prevents.
  Last run: deadlock trace found (trace length and state counts at detection
  vary with -workers).
- `ScanPool_cancel_bug.cfg` -- pre-fix consumer without the stop poll:
  `ObserveCancel = FALSE`, `AllowCancel = TRUE`, other failures off. A stop
  request arrives and the scan workers exit without claiming their tasks;
  the consumer never observes the cancel and spins on the empty ring.
  **FAILS: deadlock** at the hung state (stop requested, producers exited,
  `done < NP`, no error, ring empty, consumer draining). This is the hang the
  `IsCancelRequested`/`IsWorkerDraining` poll in `PoolScanStream::Next`
  prevents. Last run: deadlock trace found (trace length and state counts at
  detection vary with -workers).
- `ScanPool_teardown.cfg` -- everything at once: cancel/LIMIT teardown, both
  failure flavours, and stop requests mid-flight, exercising `ScanRing::Close`
  and producers releasing held pages. **PASS.** Last run: 5,992 states
  generated, 1,487 distinct.

---

# Model-vs-code findings

Races surfaced by modelling and review; all have since been addressed in the
code, and the models now check the fixes (each fix has a negative config that
fails when the fix's constant is flipped off, so a regression in the code
corresponds to a failing model):

1. **Stale pending-ring entry after an early backend abort** -- FIXED. Pending
   entries now carry `{conn_slot, conn_generation}` (the session slot index +
   generation); `SessionPool::Acquire` bumps the slot generation, and the
   worker's `SessionThreadMain` attaches via `TryAttachEnd(idx, generation)`,
   which rejects a slot that is FREE or at a different generation.
   `ControlProtocol.cfg` (PASS) includes the abort-while-pending adversary;
   `ControlProtocol_staleentry_bug.cfg` (FAIL) preserves the pre-fix behavior
   as the regression illustration.

2. **Session-slot refcount stranded by worker death** -- addressed for the
   reachable cases. The duckdb worker now drains its session threads from a
   `before_shmem_exit` callback, so ERROR/FATAL exits DetachEnd cleanly; hard
   kills (SIGKILL, segfault) skip the callback but cause a postmaster
   crash-restart that reinitializes shared memory, so no stranded refcount
   persists. `ControlProtocol_workercrash.cfg` (FAIL) keeps the counterfactual
   "hard kill, no drain, no shmem reset" as the illustration of why the drain
   exists.

3. **C1: abort during the handshake wedged the session thread** -- FIXED. The
   original model bundled "session thread finishes" into one always-enabled
   action, hiding that the thread was actually parked in the handshake
   receive with no exit when its backend aborted after the enqueue but before
   the frames were sent. The abort path now also sets the channel cancel flag
   (`RequestCancelAt`), which the handshake poll observes
   (`SerializedRecvControl` returns Detached). The refined model splits the
   abort into cancel-then-detach and the handshake wait into
   frame-or-cancel; `ControlProtocol.cfg` (PASS) checks the fix,
   `ControlProtocol_handshake_bug.cfg` (FAIL, deadlock) is the pre-fix wedge.

4. **Silent scan-producer death / unobserved cancellation hung the pool-scan
   consumer** -- FIXED twice over. A FATAL producer exit now reports
   `SetError` from a shmem-exit hook (`g_active_range`), and the consumer's
   nothing-ready branch polls the session cancel flag and the worker draining
   flag. `ScanPool.cfg` (PASS) checks both; `ScanPool_bug.cfg` and
   `ScanPool_cancel_bug.cfg` (FAIL, deadlock) are the two pre-fix hangs.

5. **C2a: late scan replies resurrected closed demux lanes** -- FIXED. The
   original model only allowed `CloseScanLane` after a completed drain, so
   the late-reply case was unreachable. The drain can in fact bail (cancel /
   worker drain), leaving replies on the wire; `CloseScanLane` now tombstones
   the scan_id (`closed_scans`) and `RoutedRecv` drops late replies for
   closed scans, freeing their Arrow pages (`~SessionChannel` also frees
   pages still buffered in lanes). `ScanInversion.cfg` (PASS) includes bailed
   drains; `ScanInversion_lateclose_bug.cfg` (FAIL, `ClosedLaneEmpty`) is the
   pre-fix resurrection leak.

The model treats `DetachEnd` as one atomic step; the code matches it:
`DetachEnd`'s decrement and its free decision both run under the pool
spinlock, so they are atomic against `TryAttachEnd`'s check + increment
(a decrement landing between them could otherwise free a slot the worker
just attached).
