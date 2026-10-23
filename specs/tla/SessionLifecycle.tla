---------------------------- MODULE SessionLifecycle ----------------------------
(***************************************************************************)
(* Cross-process lifecycle of a session dispatched from a PostgreSQL       *)
(* backend to the per-database DuckDB worker.                              *)
(*                                                                         *)
(* A backend creates a dsm segment (header with a nonce + two shm_mq       *)
(* rings), posts {handle, nonce} on the worker registry's mailbox ring and *)
(* waits for frames.  The worker's main thread pops mailbox entries while  *)
(* it has session capacity, dsm_attach-es the segment (which fails once    *)
(* the segment is gone), checks the nonce in the header and runs a session *)
(* thread.  A segment lives exactly as long as some process is attached    *)
(* (the dsm refcount), so no slot refcount of our own is needed: backend   *)
(* abort detaches (resource owner / shmem-exit hook), worker exit          *)
(* interrupts, joins and detaches every session.                           *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Backends,            \* backend ids
  MaxSessions,         \* worker capacity (max_worker_sessions)
  MailboxCap,          \* mailbox ring capacity
  QueriesPerBackend,   \* dispatches per backend (bounds the state space)
  MaxRpcs,             \* metadata RPC round trips per session (bounds the state space)
  MaxWorkerExits,      \* bound on worker exits and crashes
  WorkerExitAllowed,   \* worker may exit cleanly (SIGTERM, FATAL, failed start)
  WorkerCrashAllowed,  \* worker may die hard (postmaster resets shared memory)
  HandleReuse,         \* a destroyed segment's handle may be handed out again
  \* Bug switches.  TRUE is the implemented behaviour.
  ValidateNonce,       \* worker rejects a mailbox entry whose nonce differs from the header
  AbortDetaches,       \* backend abort detaches its segment
  ExitDrains,          \* worker exit interrupts and joins sessions before leaving
  DetachWakesSession   \* main thread fails a blocked RPC once its backend detached

NoSeg == 0
MaxSegs == Cardinality(Backends) * QueriesPerBackend
SegIds == 1..MaxSegs
Nonces == 0..MaxSegs

VARIABLES
  bstate,     \* backend phase: idle | created | posted
  bseg,       \* segment held by the backend, NoSeg if none
  bcount,     \* dispatches done per backend
  seg,        \* per segment id: bAtt/wAtt (attached ends), wDet (worker end detached), nonce, doneSent, rpcLeft
  nonceCtr,   \* last nonce handed out
  usedIds,    \* segment ids ever created (HandleReuse = FALSE never reuses them)
  mailbox,    \* Seq of [id, nonce]
  wstate,     \* absent | starting | running | exiting
  sessions,   \* per segment id: none | running | blocked | done
  serveCount, \* per nonce: session threads that served it
  exits,      \* worker exits and crashes so far
  wgen,       \* worker generation, bumped each time a worker starts running
  bgen        \* per backend: the worker generation its dispatch was posted to

vars == <<bstate, bseg, bcount, seg, nonceCtr, usedIds, mailbox, wstate, sessions, serveCount, exits, wgen, bgen>>

Alive(id) == seg[id].bAtt \/ seg[id].wAtt
Active == {id \in SegIds : sessions[id] /= "none"}

SegRec == [bAtt: BOOLEAN, wAtt: BOOLEAN, wDet: BOOLEAN, nonce: Nonces,
           doneSent: BOOLEAN, rpcLeft: 0..MaxRpcs]

TypeOK ==
  /\ bstate \in [Backends -> {"idle", "created", "posted"}]
  /\ bseg \in [Backends -> SegIds \cup {NoSeg}]
  /\ bcount \in [Backends -> 0..QueriesPerBackend]
  /\ seg \in [SegIds -> SegRec]
  /\ nonceCtr \in Nonces
  /\ usedIds \subseteq SegIds
  /\ mailbox \in Seq([id: SegIds, nonce: Nonces])
  /\ wstate \in {"absent", "starting", "running", "exiting"}
  /\ sessions \in [SegIds -> {"none", "running", "blocked", "done"}]
  /\ serveCount \in [Nonces -> Nat]
  /\ exits \in 0..MaxWorkerExits
  /\ wgen \in Nat
  /\ bgen \in [Backends -> Nat]

FreshSeg(n) == [bAtt |-> TRUE, wAtt |-> FALSE, wDet |-> FALSE, nonce |-> n,
                doneSent |-> FALSE, rpcLeft |-> MaxRpcs]

Init ==
  /\ bstate = [b \in Backends |-> "idle"]
  /\ bseg = [b \in Backends |-> NoSeg]
  /\ bcount = [b \in Backends |-> 0]
  /\ seg = [id \in SegIds |-> [bAtt |-> FALSE, wAtt |-> FALSE, wDet |-> FALSE, nonce |-> 0,
                               doneSent |-> FALSE, rpcLeft |-> MaxRpcs]]
  /\ nonceCtr = 0
  /\ usedIds = {}
  /\ mailbox = <<>>
  /\ wstate = "absent"
  /\ sessions = [id \in SegIds |-> "none"]
  /\ serveCount = [n \in Nonces |-> 0]
  /\ exits = 0
  /\ wgen = 0
  /\ bgen = [b \in Backends |-> 0]

-----------------------------------------------------------------------------
(* Backend *)

\* EnsureWorker: the registry has no worker for the database, so a backend
\* about to dispatch claims the entry and registers the background worker.
SpawnWorker(b) ==
  /\ wstate = "absent"
  /\ bstate[b] = "idle" /\ bcount[b] < QueriesPerBackend
  /\ wstate' = "starting"
  /\ UNCHANGED <<bstate, bseg, bcount, seg, nonceCtr, usedIds, mailbox, sessions, serveCount, exits, wgen, bgen>>

\* dsm_create: the handle of a destroyed segment may come back (HandleReuse).
CreateSeg(b) ==
  /\ bstate[b] = "idle"
  /\ bcount[b] < QueriesPerBackend
  /\ wstate = "running"
  /\ bgen' = [bgen EXCEPT ![b] = wgen]
  /\ \E id \in SegIds :
       /\ ~Alive(id)
       /\ (HandleReuse \/ id \notin usedIds)
       /\ seg' = [seg EXCEPT ![id] = FreshSeg(nonceCtr + 1)]
       /\ bseg' = [bseg EXCEPT ![b] = id]
       /\ usedIds' = usedIds \cup {id}
  /\ nonceCtr' = nonceCtr + 1
  /\ bstate' = [bstate EXCEPT ![b] = "created"]
  /\ bcount' = [bcount EXCEPT ![b] = @ + 1]
  /\ UNCHANGED <<mailbox, wstate, sessions, serveCount, exits, wgen>>

\* Push {handle, nonce} on the mailbox ring and set the worker latch.
Post(b) ==
  /\ bstate[b] = "created"
  /\ wstate = "running" /\ wgen = bgen[b]
  /\ Len(mailbox) < MailboxCap
  /\ mailbox' = Append(mailbox, [id |-> bseg[b], nonce |-> seg[bseg[b]].nonce])
  /\ bstate' = [bstate EXCEPT ![b] = "posted"]
  /\ UNCHANGED <<bseg, bcount, seg, nonceCtr, usedIds, wstate, sessions, serveCount, exits, wgen, bgen>>

BackendDetach(b) ==
  /\ seg' = [seg EXCEPT ![bseg[b]].bAtt = FALSE]
  /\ bstate' = [bstate EXCEPT ![b] = "idle"]
  /\ bseg' = [bseg EXCEPT ![b] = NoSeg]

\* The Done frame arrived: EndCustomScan detaches.
Finish(b) ==
  /\ bstate[b] = "posted"
  /\ seg[bseg[b]].doneSent
  /\ BackendDetach(b)
  /\ UNCHANGED <<bcount, nonceCtr, usedIds, mailbox, wstate, sessions, serveCount, exits, wgen, bgen>>

\* The worker end detached mid-session (worker exit): the receive returns
\* SHM_MQ_DETACHED and the statement errors out.
WorkerLost(b) ==
  /\ bstate[b] = "posted"
  /\ seg[bseg[b]].wDet
  /\ ~seg[bseg[b]].doneSent
  /\ BackendDetach(b)
  /\ UNCHANGED <<bcount, nonceCtr, usedIds, mailbox, wstate, sessions, serveCount, exits, wgen, bgen>>

\* The worker the dispatch was posted to is gone before it attached: the
\* backend's liveness check errors out (it does not respawn and repost).
WorkerGone(b) ==
  /\ bstate[b] \in {"created", "posted"}
  /\ ~seg[bseg[b]].wAtt
  /\ (wstate /= "running" \/ wgen /= bgen[b])
  /\ BackendDetach(b)
  /\ UNCHANGED <<bcount, nonceCtr, usedIds, mailbox, wstate, sessions, serveCount, exits, wgen, bgen>>

\* ERROR, cancel or FATAL at any point: the resource owner (or the
\* shmem-exit hook) detaches the segment.
Abort(b) ==
  /\ bstate[b] \in {"created", "posted"}
  /\ IF AbortDetaches
       THEN BackendDetach(b)
       ELSE /\ bstate' = [bstate EXCEPT ![b] = "idle"]
            /\ bseg' = [bseg EXCEPT ![b] = NoSeg]
            /\ UNCHANGED seg
  /\ UNCHANGED <<bcount, nonceCtr, usedIds, mailbox, wstate, sessions, serveCount, exits, wgen, bgen>>

-----------------------------------------------------------------------------
(* Worker *)

\* The worker published its pid and latch in the registry.
WorkerStart ==
  /\ wstate = "starting"
  /\ wstate' = "running"
  /\ wgen' = wgen + 1
  /\ UNCHANGED <<bstate, bseg, bcount, seg, nonceCtr, usedIds, mailbox, sessions, serveCount, exits, bgen>>

\* Main loop: pop a mailbox entry while below capacity.  dsm_attach fails for
\* a destroyed segment; the nonce in the header must match the entry.
Pick ==
  /\ wstate = "running"
  /\ mailbox /= <<>>
  /\ Cardinality(Active) < MaxSessions
  /\ LET e == Head(mailbox)
         ok == Alive(e.id) /\ (~ValidateNonce \/ seg[e.id].nonce = e.nonce)
     IN /\ mailbox' = Tail(mailbox)
        /\ IF ok
             THEN /\ seg' = [seg EXCEPT ![e.id].wAtt = TRUE]
                  /\ sessions' = [sessions EXCEPT ![e.id] = "running"]
                  /\ serveCount' = [serveCount EXCEPT ![seg[e.id].nonce] = @ + 1]
             ELSE UNCHANGED <<seg, sessions, serveCount>>
  /\ UNCHANGED <<bstate, bseg, bcount, nonceCtr, usedIds, wstate, exits, wgen, bgen>>

\* A DuckDB thread of the session needs the backend (metadata RPC) and blocks.
ThreadBlock(id) ==
  /\ sessions[id] = "running"
  /\ seg[id].rpcLeft > 0
  /\ seg' = [seg EXCEPT ![id].rpcLeft = @ - 1]
  /\ sessions' = [sessions EXCEPT ![id] = "blocked"]
  /\ UNCHANGED <<bstate, bseg, bcount, nonceCtr, usedIds, mailbox, wstate, serveCount, exits, wgen, bgen>>

\* The backend serviced the RPC.
ThreadUnblock(id) ==
  /\ sessions[id] = "blocked"
  /\ seg[id].bAtt
  /\ sessions' = [sessions EXCEPT ![id] = "running"]
  /\ UNCHANGED <<bstate, bseg, bcount, seg, nonceCtr, usedIds, mailbox, wstate, serveCount, exits, wgen, bgen>>

\* The query finished, or Interrupt() unwound it: the terminal frame is queued.
ThreadDone(id) ==
  /\ sessions[id] = "running"
  /\ sessions' = [sessions EXCEPT ![id] = "done"]
  /\ seg' = [seg EXCEPT ![id].doneSent = TRUE]
  /\ UNCHANGED <<bstate, bseg, bcount, nonceCtr, usedIds, mailbox, wstate, serveCount, exits, wgen, bgen>>

\* Main thread saw SHM_MQ_DETACHED while the session waits for an RPC reply:
\* fail the RPC so the thread unwinds.
WakeOnDetach(id) ==
  /\ DetachWakesSession
  /\ sessions[id] = "blocked"
  /\ ~seg[id].bAtt
  /\ sessions' = [sessions EXCEPT ![id] = "done"]
  /\ UNCHANGED <<bstate, bseg, bcount, seg, nonceCtr, usedIds, mailbox, wstate, serveCount, exits, wgen, bgen>>

\* Main thread joins a finished session thread and dsm_detach-es.
Reap(id) ==
  /\ sessions[id] = "done"
  /\ wstate \in {"running", "exiting"}
  /\ sessions' = [sessions EXCEPT ![id] = "none"]
  /\ seg' = [seg EXCEPT ![id].wAtt = FALSE, ![id].wDet = TRUE]
  /\ UNCHANGED <<bstate, bseg, bcount, nonceCtr, usedIds, mailbox, wstate, serveCount, exits, wgen, bgen>>

\* SIGTERM, FATAL, or a start that fails: the worker begins a clean exit.
ExitBegin ==
  /\ WorkerExitAllowed
  /\ wstate \in {"starting", "running"}
  /\ exits < MaxWorkerExits
  /\ wstate' = "exiting"
  /\ exits' = exits + 1
  /\ UNCHANGED <<bstate, bseg, bcount, seg, nonceCtr, usedIds, mailbox, sessions, serveCount, wgen, bgen>>

\* Drain: Interrupt() every session and fail blocked RPCs.
ExitInterrupt(id) ==
  /\ ExitDrains
  /\ wstate = "exiting"
  /\ sessions[id] \in {"running", "blocked"}
  /\ sessions' = [sessions EXCEPT ![id] = "done"]
  /\ UNCHANGED <<bstate, bseg, bcount, seg, nonceCtr, usedIds, mailbox, wstate, serveCount, exits, wgen, bgen>>

\* proc_exit: with ExitDrains the sessions were joined and detached first.
ExitFinish ==
  /\ wstate = "exiting"
  /\ ExitDrains => Active = {}
  /\ wstate' = "absent"
  /\ UNCHANGED <<bstate, bseg, bcount, seg, nonceCtr, usedIds, mailbox, sessions, serveCount, exits, wgen, bgen>>

\* Hard death (SIGKILL, segfault): the postmaster restarts every process and
\* reinitializes shared memory.
Crash ==
  /\ WorkerCrashAllowed
  /\ wstate \in {"starting", "running", "exiting"}
  /\ exits < MaxWorkerExits
  /\ exits' = exits + 1
  /\ wstate' = "absent"
  /\ sessions' = [id \in SegIds |-> "none"]
  /\ mailbox' = <<>>
  /\ seg' = [id \in SegIds |-> [seg[id] EXCEPT !.bAtt = FALSE, !.wAtt = FALSE, !.wDet = FALSE]]
  /\ bstate' = [b \in Backends |-> "idle"]
  /\ bseg' = [b \in Backends |-> NoSeg]
  /\ UNCHANGED <<bcount, nonceCtr, usedIds, serveCount, wgen, bgen>>

-----------------------------------------------------------------------------

\* An entry nobody waits on any more: its backend aborted (segment gone) or
\* the handle was reused.  The next worker to drain the mailbox rejects it.
StaleEntry(e) == ~(Alive(e.id) /\ seg[e.id].nonce = e.nonce)
MailboxStaleOnly == \A i \in 1..Len(mailbox) : StaleEntry(mailbox[i])

AllDone ==
  /\ \A b \in Backends : bstate[b] = "idle" /\ bcount[b] = QueriesPerBackend
  /\ MailboxStaleOnly
  /\ Active = {}

Terminating == AllDone /\ UNCHANGED vars

Next ==
  \/ \E b \in Backends :
       SpawnWorker(b) \/ CreateSeg(b) \/ Post(b) \/ Finish(b) \/ WorkerLost(b)
       \/ WorkerGone(b) \/ Abort(b)
  \/ WorkerStart \/ Pick \/ ExitBegin \/ ExitFinish \/ Crash
  \/ \E id \in SegIds :
       ThreadBlock(id) \/ ThreadUnblock(id) \/ ThreadDone(id) \/ WakeOnDetach(id)
       \/ Reap(id) \/ ExitInterrupt(id)
  \/ Terminating

\* Faults (Abort, ExitBegin, Crash) are never forced; everything else is.
Fairness ==
  /\ \A b \in Backends :
       /\ WF_vars(SpawnWorker(b)) /\ WF_vars(CreateSeg(b)) /\ WF_vars(Post(b))
       /\ WF_vars(Finish(b)) /\ WF_vars(WorkerLost(b)) /\ WF_vars(WorkerGone(b))
  /\ WF_vars(WorkerStart) /\ WF_vars(Pick) /\ WF_vars(ExitFinish)
  /\ \A id \in SegIds :
       /\ WF_vars(ThreadDone(id)) /\ WF_vars(ThreadUnblock(id)) /\ WF_vars(WakeOnDetach(id))
       /\ WF_vars(Reap(id)) /\ WF_vars(ExitInterrupt(id))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety *)

\* The worker only references segments it is attached to (no use after free).
WorkerTouchesOnlyAttached == \A id \in Active : seg[id].wAtt

CapacityOK == Cardinality(Active) <= MaxSessions

\* A posted session is served by at most one session thread.
NoDoubleServe == \A n \in Nonces : serveCount[n] <= 1

\* Two live backends never hold the same segment.
SingleOwner ==
  \A b1, b2 \in Backends : (b1 /= b2 /\ bseg[b1] /= NoSeg) => bseg[b1] /= bseg[b2]

\* No session thread outlives its worker process.
NoSessionWithoutWorker == wstate = "absent" => Active = {}

MailboxBounded == Len(mailbox) <= MailboxCap

Invariant ==
  /\ TypeOK /\ WorkerTouchesOnlyAttached /\ CapacityOK /\ NoDoubleServe
  /\ SingleOwner /\ NoSessionWithoutWorker /\ MailboxBounded

(* Liveness *)

\* Every dispatch ends: served, errored, or aborted.  No backend waits forever,
\* including one waiting out a full mailbox or a worker at capacity.
Settles == \A b \in Backends : (bstate[b] /= "idle") ~> (bstate[b] = "idle")

\* Every segment is eventually destroyed and every session reaped.
Quiescent == <>[](Active = {} /\ MailboxStaleOnly /\ \A id \in SegIds : ~Alive(id))

=============================================================================
