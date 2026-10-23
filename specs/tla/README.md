# TLA+ models of the shared DuckDB worker

Models of the two concurrency cores of the per-database DuckDB worker
(`libpgduckdb/worker/pgddb_worker.cpp`, overview in
`libpgduckdb/include/pgddb/worker/duckdb_worker.hpp`). Each action is
commented with the code it stands for. Constants are small enough that every
config finishes in minutes.

## Run

```sh
JAR=<path>/tla2tools.jar   # https://github.com/tlaplus/tlaplus/releases
cd specs/tla
java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -config <cfg> -workers auto <spec>.tla
```

Configs without `_bug` in their name must report "No error has been found".
Every `_bug` config flips one constant from the implemented behaviour to a
plausible shortcut and fails in the way its header comment states, so a
regression in the code has a model that fails for the same reason.

## SessionLifecycle.tla: cross-process handoff and cleanup

One worker registry entry, its mailbox ring, dsm segments with their attach
refcounts, a worker with a session capacity, and backends that dispatch, abort
and finish. Faults: backend abort in every phase, clean worker exit (SIGTERM,
FATAL, failed start), hard worker death (postmaster shared-memory reset), and
dsm handle reuse after a segment is destroyed.

| Action | Code |
|---|---|
| `SpawnWorker`, `WorkerStart` | `BackendSession::EnsureWorker`, `WorkerMainLoop` publishing `WORKER_RUNNING` |
| `CreateSeg`, `Post` | `BackendSession::Open`: `dsm_create`, nonce, `Post` |
| `Pick` | `AdmitSessions`: `dsm_attach`, nonce check, `shm_mq_attach` |
| `ThreadBlock`, `ThreadUnblock` | `WorkerSessionRequest` and the backend's `ServeRequest` reply |
| `ThreadDone`, `Reap` | `SessionThreadMain` finishing, `ReapSessions` |
| `WakeOnDetach` | `DetachSession` failing the pending RPC |
| `Finish`, `WorkerLost`, `WorkerGone`, `Abort` | `BackendSession::Fetch` on DONE, `WaitForWorker` liveness check, `Release` from the xact callbacks |
| `ExitBegin`, `ExitInterrupt`, `ExitFinish` | `WorkerExitCallback` and `DrainSessions` |
| `Crash` | postmaster crash restart |

Invariants: the worker only touches segments it is attached to
(`WorkerTouchesOnlyAttached`), capacity is honoured (`CapacityOK`), a posted
session is served at most once even when its dsm handle is reused
(`NoDoubleServe`), two backends never hold one segment (`SingleOwner`), no
session thread outlives its worker (`NoSessionWithoutWorker`), the mailbox is
bounded. Liveness: every dispatch settles (`Settles`) and every segment is
eventually destroyed with no session left (`Quiescent`).

| Config | Expected |
|---|---|
| `SessionLifecycle.cfg` (3 backends, capacity 2, mailbox 2, worker exit) | pass |
| `SessionLifecycle_crash.cfg` (hard worker death, 2 dispatches per backend) | pass |
| `SessionLifecycle_nonce_bug.cfg` (`ValidateNonce = FALSE`) | `NoDoubleServe` violated |
| `SessionLifecycle_abortleak_bug.cfg` (`AbortDetaches = FALSE`) | `Quiescent` violated |
| `SessionLifecycle_exitdrain_bug.cfg` (`ExitDrains = FALSE`) | `NoSessionWithoutWorker` violated |
| `SessionLifecycle_detachwake_bug.cfg` (`DetachWakesSession = FALSE`) | deadlock: a session blocked in an RPC whose backend aborted pins the capacity |

## WorkerPump.tla: the main thread's pump

Inside the worker, one main thread owns every PostgreSQL call. Session threads
queue frames in an outbox; the main thread moves them onto the response ring
with non-blocking sends, hands RPC replies from the request ring to the
waiting thread, interrupts sessions whose backend detached and reaps finished
threads. The latch is explicit: producers set it after queueing work, the
loop resets it before scanning.

| Action | Code |
|---|---|
| `Produce`, `Finish`, `WaitOutbox`, `OutboxRoom` | `PushFrame` and the bounded outbox |
| `NeedMeta`, `MetaReturn` | `WorkerSessionRequest` |
| `Interrupted` | the session thread unwinding after `Interrupt()` |
| `MainWake`, `MainScanEmpty`, `MainSleep` | the `WaitLatch` / `ResetLatch` loop in `WorkerMainLoop` |
| `Send`, `Recv`, `ObserveDetach`, `Reap` | `PumpSession` (RPC replies and `CHUNK_FREE` both arrive on the request ring), `DetachSession`, `ReapSessions` |
| `Pull`, `Reply`, `FinishDetach`, `Abort` | `BackendSession::ReceiveOne`, `ServeRequest`, `Release` |

Invariants: queues stay bounded, at most one RPC per session is in flight
(`OneMetaInFlight`, which is why replies carry no request id), a reply on the
ring answers the request that was sent (`ReplyMatchesRequest`), nothing
follows the terminal frame (`TerminalIsLast`), the main thread never parks on
one session (`MainNeverBlocked`), and the loop never sleeps with its latch
clear while a session has work (`NoLostWakeup`). Liveness: every session whose
backend keeps pulling is served and reaped whatever the other backends do.

| Config | Expected |
|---|---|
| `WorkerPump.cfg` (2 sessions, both backends fair) | pass |
| `WorkerPump_stall.cfg` (one backend may stop pulling forever) | pass: the other session still completes |
| `WorkerPump_blockingsend_bug.cfg` (`NonBlockingSend = FALSE`) | `FairSessionsExit` violated: a stalled backend starves the other session |
| `WorkerPump_lostwakeup_bug.cfg` (`ResetLatchFirst = FALSE`) | `NoLostWakeup` violated |
| `WorkerPump_metahang_bug.cfg` (`FailMetaOnDetach = FALSE`) | deadlock: a session waits forever for a reply its backend never sends |

## SharedArena.tla: the result page arena

One arena of result pages shared by every session of a worker. A session
thread takes a page to write a chunk, the backend consumes the chunk and
frees the page, and a stalled backend keeps its pages forever. Each session
owns `ReservedPages` pages that are never lent to another session; the rest
is a shared overflow.

| Action | Code |
|---|---|
| `Produce`, `Block`, `Unblock` | `AcquirePage` and the arena condition variable |
| `Pull`, `Done` | `BackendSession::ReceiveOne` on `CHUNK_REF`, `FreeHeldPage` at the top of `Fetch` |
| `BackendFinish`, `Abort` | `Release`, `ReclaimPages` after the thread is joined |

Invariants: every page is free or held by exactly one session
(`PageConservation`). Liveness: a session whose
backend keeps consuming always completes, whatever the other backends hold.
Two orderings in the code carry the model's guarantees: the backend frees
the page of a consumed chunk before it waits for the next frame, and the
worker reclaims a session's pages only after the backend freed them or
detached (`SessionFinished`), never while the backend may still read one.

| Config | Expected |
|---|---|
| `SharedArena.cfg` (two reserved pages per session, one overflow page, one backend may stall forever) | pass |
| `SharedArena_noreserve_bug.cfg` (one pooled arena, no reservation) | `FairSessionsComplete` violated: the stalled session's pages starve the other |
| `SharedArena_freeafterwait_bug.cfg` (the backend frees a page only when the next chunk arrives) | `FairSessionsComplete` violated: producer and consumer wait for each other |

## What the models found while the design was written

- Every thread exit must set the worker latch, including a thread that
  unwinds after a failed RPC or after cancellation; the first version of the
  pump only set it when a frame was queued, and the main thread never reaped
  the session (`WorkerPump_metahang_bug` trace, now `SessionThreadMain`).
- A reply the backend writes just before aborting is left unread; the
  invariant has to allow it rather than the code having to drain it.
- Checking the dsm header nonce after `dsm_attach` is what makes handle reuse
  harmless; without it a reused handle serves a session twice.
