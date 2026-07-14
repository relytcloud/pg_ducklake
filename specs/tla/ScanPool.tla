-------------------------------- MODULE ScanPool --------------------------------
(***************************************************************************)
(* A TLA+ model of the shared scan-producer pool of the shared DuckDB      *)
(* duckdb worker (libpgduckdb/worker/scan_producer.cpp +                   *)
(* libpgduckdb/worker/duckdb_worker.cpp, over the transport in             *)
(* libpgduckdb/worker/transport/{scan_ring,scan_queue,page_pool}.cpp).     *)
(*                                                                         *)
(* What is modelled: the page lifecycle and the producer/consumer/teardown *)
(* protocol for ONE scan -- the concurrency core where the code reviews    *)
(* found consumer hangs and page leaks.                                    *)
(*                                                                         *)
(*   - A fixed page pool (PagePool::Acquire/Release): a page is in         *)
(*     exactly one place at a time -- free, held by a producer, queued in   *)
(*     the scan's ready-ring, or in-flight in the consumer (referenced by a *)
(*     live DuckDB chunk, freed later by the Arrow release callback).      *)
(*   - N scan workers (DuckdbWorker::ScanWorkerMain), each running one     *)
(*     block-range task (ProcessScanRange): acquire a page, fill it, push  *)
(*     it to the ready-ring (ScanRing::Push); after ChunksPerTask pushes   *)
(*     the task is done (ScanRing::TaskDone).                              *)
(*   - Producer failure comes in two flavours:                             *)
(*       ProducerError -- a PostgreSQL ERROR inside ProcessScanRange. The  *)
(*         PG_TRY there catches the longjmp, releases the held page and    *)
(*         calls ScanRing::SetError: ALWAYS reported.                      *)
(*       ProducerFatal -- the scan-worker PROCESS dies (elog FATAL,        *)
(*         SIGTERM mid-task). FATAL skips PG_CATCH but runs shmem-exit     *)
(*         callbacks: the before_shmem_exit hook in ScanWorkerMain errors  *)
(*         out the active range (g_active_range -> ScanRing::SetError).    *)
(*         FatalReportsError toggles that hook; FALSE is the pre-fix       *)
(*         silent death -- no TaskDone, no SetError -- after which the     *)
(*         consumer polls the ready-ring forever (the hang).               *)
(*   - The duckdb worker's consumer (PoolScanStream::Next) drains the      *)
(*     ready-ring (ScanRing::TryNext), releases pages, and finishes when   *)
(*     every task is done and the ring is empty, or aborts on error /      *)
(*     external cancel, then closes the scan (ScanRing::Close reclaims     *)
(*     queued pages and bumps the generation so in-flight producers stop). *)
(*   - CANCELLATION / WORKER DRAIN: a stop request (the session channel's  *)
(*     cancel flag set by the backend, or the worker's draining flag on    *)
(*     shutdown) can arrive at any time. Scan workers do not poll the      *)
(*     session cancel flag, so they may keep producing; on a worker drain  *)
(*     a QUEUED task may simply never be claimed (StopExit: the scan       *)
(*     worker exits with the task still in the scan queue -- no TaskDone,  *)
(*     no error). The FIXED consumer polls the stop conditions in its      *)
(*     nothing-ready branch (IsCancelRequested / IsWorkerDraining in       *)
(*     PoolScanStream::Next) and errors out, closing the ring; the         *)
(*     ObserveCancel constant toggles that poll -- FALSE is the pre-fix    *)
(*     consumer, which spins on the never-to-be-filled ring forever.       *)
(*                                                                         *)
(* CONSTANTS pick the adversary:                                           *)
(*   AllowFail  - producers may hit a PG ERROR mid-task (always reported). *)
(*   AllowFatal - a producer process may die FATALly mid-task.             *)
(*   FatalReportsError - TRUE: the shmem-exit hook reports the FATAL as a  *)
(*     scan error (the fix). FALSE: silent death (pre-fix): expect FAIL.   *)
(*   AllowCancel - a stop request (backend cancel / worker drain) may      *)
(*     arrive; producers may then exit without claiming their task.        *)
(*   ObserveCancel - TRUE: the consumer's nothing-ready branch polls the   *)
(*     stop conditions (the fix). FALSE: it does not (pre-fix): expect     *)
(*     FAIL when a task is abandoned.                                      *)
(*   AllowAbort - the consumer may externally abort at any time (DuckDB    *)
(*     cancel / LIMIT reached), exercising the teardown/reclaim path.      *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    Producers,      \* set of pool-worker ids; one task each. e.g. {p1, p2}
    NumPages,       \* size of the global page pool. e.g. 2
    RingCap,        \* per-scan ready-ring capacity. e.g. 2
    ChunksPerTask,  \* pages each task produces before TaskDone. e.g. 2
    AllowFail,      \* TRUE: producers may hit a PG ERROR mid-task
    AllowFatal,     \* TRUE: a producer process may die FATALly
    FatalReportsError, \* TRUE: shmem-exit hook -> SetError (the fix)
    AllowCancel,    \* TRUE: a stop request (cancel / worker drain) may arrive
    ObserveCancel,  \* TRUE: consumer polls the stop conditions (the fix)
    AllowAbort      \* TRUE: the consumer may externally abort (cancel/LIMIT)

Pages == 1 .. NumPages

VARIABLES
    free,       \* SUBSET Pages: pages on the free stack
    held,       \* [Producers -> Pages \cup {0}]: page a producer is filling (0 = none)
    ring,       \* Seq(Pages): the scan's ready-ring (head = next to consume)
    inflight,   \* SUBSET Pages: popped by the consumer, referenced by a live chunk
    pstate,     \* [Producers -> {"work","exit"}]
    produced,   \* [Producers -> 0..ChunksPerTask]: pages this task has pushed
    done,       \* number of tasks that reached TaskDone
    errored,    \* BOOLEAN: the scan's error flag
    stopReq,    \* BOOLEAN: backend cancel / worker draining requested
    cstate      \* {"drain","finished","aborted"}: consumer state

vars == <<free, held, ring, inflight, pstate, produced, done, errored, stopReq, cstate>>

NP == Cardinality(Producers)
RingSet  == { ring[i] : i \in DOMAIN ring }
NumHeld  == Cardinality({ p \in Producers : held[p] # 0 })

----------------------------------------------------------------------------
TypeOK ==
    /\ free \subseteq Pages
    /\ held \in [Producers -> Pages \cup {0}]
    /\ ring \in Seq(Pages)
    /\ inflight \subseteq Pages
    /\ pstate \in [Producers -> {"work","exit"}]
    /\ produced \in [Producers -> 0 .. ChunksPerTask]
    /\ done \in 0 .. NP
    /\ errored \in BOOLEAN
    /\ stopReq \in BOOLEAN
    /\ cstate \in {"drain","finished","aborted"}

Init ==
    /\ free = Pages
    /\ held = [p \in Producers |-> 0]
    /\ ring = << >>
    /\ inflight = {}
    /\ pstate = [p \in Producers |-> "work"]
    /\ produced = [p \in Producers |-> 0]
    /\ done = 0
    /\ errored = FALSE
    /\ stopReq = FALSE
    /\ cstate = "drain"

----------------------------------------------------------------------------
(* --- Producer actions (one per pool worker) --- *)

\* PagePool::Acquire: grab a free page to fill the next chunk. A stop request
\* does NOT gate this: scan workers do not poll the session cancel flag.
Acquire(p) ==
    /\ pstate[p] = "work"
    /\ held[p] = 0
    /\ produced[p] < ChunksPerTask
    /\ cstate = "drain"
    /\ ~errored
    /\ free # {}
    /\ \E pg \in free :
         /\ free' = free \ {pg}
         /\ held' = [held EXCEPT ![p] = pg]
    /\ UNCHANGED <<ring, inflight, pstate, produced, done, errored, stopReq, cstate>>

\* ScanRing::Push (scan alive, not errored): enqueue the filled page; the last
\* push is followed by ScanRing::TaskDone.
PushOk(p) ==
    /\ pstate[p] = "work"
    /\ held[p] # 0
    /\ cstate = "drain"
    /\ ~errored
    /\ Len(ring) < RingCap
    /\ ring' = Append(ring, held[p])
    /\ held' = [held EXCEPT ![p] = 0]
    /\ produced' = [produced EXCEPT ![p] = produced[p] + 1]
    /\ IF produced[p] + 1 = ChunksPerTask
         THEN /\ pstate' = [pstate EXCEPT ![p] = "exit"]
              /\ done' = done + 1
         ELSE /\ pstate' = [pstate EXCEPT ![p] = "work"]
              /\ done' = done
    /\ UNCHANGED <<free, inflight, errored, stopReq, cstate>>

\* ScanRing::Push returns false -- the scan was closed (generation bumped) or
\* another producer errored the scan: free the page, stop.
PushTorndown(p) ==
    /\ pstate[p] = "work"
    /\ held[p] # 0
    /\ (cstate = "aborted" \/ errored)
    /\ free' = free \cup {held[p]}
    /\ held' = [held EXCEPT ![p] = 0]
    /\ pstate' = [pstate EXCEPT ![p] = "exit"]
    /\ UNCHANGED <<ring, inflight, produced, done, errored, stopReq, cstate>>

\* Producer notices the scan was torn down before acquiring another page: stop.
StopOnAbort(p) ==
    /\ pstate[p] = "work"
    /\ held[p] = 0
    /\ cstate = "aborted"
    /\ pstate' = [pstate EXCEPT ![p] = "exit"]
    /\ UNCHANGED <<free, held, ring, inflight, produced, done, errored, stopReq, cstate>>

\* A PostgreSQL ERROR inside ProcessScanRange: the PG_TRY catches the longjmp,
\* releases the held page, and reports via ScanRing::SetError -- unconditional.
ProducerError(p) ==
    /\ AllowFail
    /\ pstate[p] = "work"
    /\ free' = free \cup (IF held[p] # 0 THEN {held[p]} ELSE {})
    /\ held' = [held EXCEPT ![p] = 0]
    /\ pstate' = [pstate EXCEPT ![p] = "exit"]
    /\ errored' = TRUE
    /\ UNCHANGED <<ring, inflight, produced, done, stopReq, cstate>>

\* The scan-worker PROCESS dies mid-task (elog FATAL, SIGTERM): PG_CATCH is
\* skipped, but shmem-exit callbacks run -- with the fix the ScanWorkerMain
\* hook errors out g_active_range (FatalReportsError = TRUE); pre-fix the
\* producer just disappears. Modelled between pages (held = 0): a FATAL while
\* holding a page additionally leaks that page (no callback releases it) -- a
\* separate, known gap outside this model's scope.
ProducerFatal(p) ==
    /\ AllowFatal
    /\ pstate[p] = "work"
    /\ held[p] = 0
    /\ pstate' = [pstate EXCEPT ![p] = "exit"]
    /\ errored' = IF FatalReportsError THEN TRUE ELSE errored
    /\ UNCHANGED <<free, held, ring, inflight, produced, done, stopReq, cstate>>

\* Worker drain with the task never claimed: the scan worker exits with the
\* task still queued in the scan queue -- no TaskDone, no SetError. (A drain
\* signal DURING a task surfaces as ProducerFatal instead: SIGTERM ->
\* CHECK_FOR_INTERRUPTS -> FATAL -> the shmem-exit hook.)
StopExit(p) ==
    /\ stopReq
    /\ pstate[p] = "work"
    /\ held[p] = 0
    /\ produced[p] = 0
    /\ pstate' = [pstate EXCEPT ![p] = "exit"]
    /\ UNCHANGED <<free, held, ring, inflight, produced, done, errored, stopReq, cstate>>

----------------------------------------------------------------------------
(* --- Stop request (backend cancel / worker draining) --- *)

CancelRequest ==
    /\ AllowCancel
    /\ ~stopReq
    /\ cstate = "drain"
    /\ stopReq' = TRUE
    /\ UNCHANGED <<free, held, ring, inflight, pstate, produced, done, errored, cstate>>

----------------------------------------------------------------------------
(* --- Consumer actions (the duckdb worker draining the ready-ring) --- *)

\* ScanRing::TryNext == 1: pop a ready page; it becomes a live chunk (in-flight).
\* The real code checks the error flag before the ring, so Pop is disabled once errored.
Pop ==
    /\ cstate = "drain"
    /\ ~errored
    /\ ring # << >>
    /\ inflight' = inflight \cup {Head(ring)}
    /\ ring' = Tail(ring)
    /\ UNCHANGED <<free, held, pstate, produced, done, errored, stopReq, cstate>>

\* The Arrow release callback (or the serialized-path immediate free) returns a page.
\* Can fire any time after the page was popped, including after the scan is terminal.
Release ==
    /\ inflight # {}
    /\ \E pg \in inflight :
         /\ inflight' = inflight \ {pg}
         /\ free' = free \cup {pg}
    /\ UNCHANGED <<held, ring, pstate, produced, done, errored, stopReq, cstate>>

\* ScanRing::TryNext == 0: ring drained and every task done -> end of scan.
Finish ==
    /\ cstate = "drain"
    /\ ~errored
    /\ ring = << >>
    /\ done = NP
    /\ cstate' = "finished"
    /\ UNCHANGED <<free, held, ring, inflight, pstate, produced, done, errored, stopReq>>

\* ScanRing::TryNext == -1: the scan errored -> consumer throws and tears down.
AbortOnError ==
    /\ cstate = "drain"
    /\ errored
    /\ cstate' = "aborted"
    /\ UNCHANGED <<free, held, ring, inflight, pstate, produced, done, errored, stopReq>>

\* The NOTHING-READY branch of PoolScanStream::Next (ring empty, tasks still
\* outstanding, no error): the fixed consumer polls IsCancelRequested /
\* IsWorkerDraining and errors out instead of spinning on a ring nobody may
\* ever fill. ObserveCancel = FALSE removes the poll (the pre-fix hang).
AbortOnCancel ==
    /\ ObserveCancel
    /\ cstate = "drain"
    /\ stopReq
    /\ ~errored
    /\ ring = << >>
    /\ done < NP
    /\ cstate' = "aborted"
    /\ UNCHANGED <<free, held, ring, inflight, pstate, produced, done, errored, stopReq>>

\* DuckDB cancels / a LIMIT is satisfied: the scan stream is destroyed early.
ExternalAbort ==
    /\ AllowAbort
    /\ cstate = "drain"
    /\ cstate' = "aborted"
    /\ UNCHANGED <<free, held, ring, inflight, pstate, produced, done, errored, stopReq>>

\* ScanRing::Close: reclaim every page still queued in the ready-ring.
Unregister ==
    /\ cstate = "aborted"
    /\ ring # << >>
    /\ free' = free \cup RingSet
    /\ ring' = << >>
    /\ UNCHANGED <<held, inflight, pstate, produced, done, errored, stopReq, cstate>>

----------------------------------------------------------------------------
\* The system always reaches a quiescent terminal: the consumer has finished or
\* aborted, every producer has stopped, nothing is queued or in-flight, and every
\* page is back on the free stack.
AllExited == \A p \in Producers : pstate[p] = "exit"

Quiescent ==
    /\ cstate \in {"finished","aborted"}
    /\ ring = << >>
    /\ inflight = {}
    /\ AllExited
    /\ free = Pages

ProducerProgress(p) == Acquire(p) \/ PushOk(p) \/ PushTorndown(p) \/ StopOnAbort(p)
ConsumerProgress    == Pop \/ Release \/ Finish \/ AbortOnError \/ AbortOnCancel \/ Unregister

\* Once quiescent, the scan is fully torn down; stutter here so the intended terminal
\* is not flagged as a deadlock. A genuinely stuck state (a pre-fix hang) is NOT
\* quiescent, has no other successor, and so is still reported as a deadlock.
Terminating == Quiescent /\ UNCHANGED vars

Next ==
    \/ \E p \in Producers : ProducerProgress(p) \/ ProducerError(p) \/ ProducerFatal(p)
                            \/ StopExit(p)
    \/ ConsumerProgress
    \/ CancelRequest
    \/ ExternalAbort
    \/ Terminating

\* Fairness on progress actions only. ProducerError, ProducerFatal, StopExit,
\* CancelRequest, and ExternalAbort are adversarial (never forced): liveness
\* must hold no matter how they interleave. AbortOnCancel is fair progress:
\* the fixed consumer runs the poll on every nothing-ready iteration.
Fairness ==
    /\ \A p \in Producers : WF_vars(ProducerProgress(p))
    /\ WF_vars(ConsumerProgress)

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
(* --- Safety --- *)

\* The four page locations partition the pool: no page is lost, and none is in
\* two places at once (so no double-free and no leak). NoLeak uses NumHeld and
\* Len(ring) (not the deduped sets) so a duplicated page would push the count > NumPages.
NoLeak == Cardinality(free) + NumHeld + Len(ring) + Cardinality(inflight) = NumPages

Disjoint ==
    LET heldSet == { held[p] : p \in Producers } \ {0} IN
    /\ free \cap heldSet  = {}
    /\ free \cap RingSet  = {}
    /\ free \cap inflight = {}
    /\ heldSet \cap RingSet  = {}
    /\ heldSet \cap inflight = {}
    /\ RingSet \cap inflight = {}

PageConservation == NoLeak /\ Disjoint

RingBounded == Len(ring) =< RingCap

----------------------------------------------------------------------------
(* --- Liveness --- *)

\* The system always reaches the quiescent terminal (Quiescent, defined above).
\* This fails iff the consumer can hang (a silent producer death or an
\* unobserved stop request with tasks outstanding) or a page can leak.
Termination == <>Quiescent
=============================================================================
