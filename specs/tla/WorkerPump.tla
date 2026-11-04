------------------------------ MODULE WorkerPump ------------------------------
(***************************************************************************)
(* The worker's main thread is the only thread that touches PostgreSQL     *)
(* (shm_mq, dsm, latch).  Session threads and DuckDB scheduler threads     *)
(* reach it through in-process queues.  This models one worker serving     *)
(* several sessions: result frames flowing session thread -> outbox ->     *)
(* response ring -> backend, the metadata RPC round trip (the session      *)
(* thread blocks until the backend's reply arrives on the request ring),   *)
(* backend abort, cancellation via Interrupt(), and the latch handshake    *)
(* that keeps the main loop from losing wakeups.                           *)
(*                                                                         *)
(* Every main-thread step is non-blocking: a full response ring disables   *)
(* that session's send and nothing else.  A backend that stops pulling     *)
(* therefore stalls only its own session.                                  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
  Sessions,          \* sessions served by one worker
  Chunks,            \* result chunks a session produces
  MaxRpcs,           \* metadata RPC round trips per session
  OutboxCap,         \* in-process outbox bound (session thread -> main thread)
  RespCap,           \* response ring capacity in frames (worker -> backend)
  ReqCap,            \* request ring capacity in frames (backend -> worker)
  FairBackends,      \* backends that keep pulling; the others may stall forever
  \* Bug switches.  TRUE is the implemented behaviour.
  ResetLatchFirst,   \* main resets its latch before scanning for work
  NonBlockingSend,   \* main never blocks on a full response ring
  FailMetaOnDetach   \* main fails a pending RPC once the backend detached

Frames == {"chunk", "metareq", "done"}
NoSession == "none"

VARIABLES
  tstate,    \* session thread: running | waitoutbox | waitmeta | done | exited
  left,      \* chunks still to produce
  rpcs,      \* RPC round trips still allowed
  outbox,    \* session thread -> main thread
  resp,      \* response ring (worker -> backend)
  req,       \* request ring (backend -> worker), carries RPC replies
  meta,      \* RPC state: none | pending (in outbox) | sent | replied | failed
  bstate,    \* backend: pull | execmeta | finished | detached
  cancel,    \* Interrupt() requested for the session
  latch,     \* the worker's latch
  phase,     \* main loop: wait | pump | blocked
  blockedOn  \* session a blocking send is parked on (NonBlockingSend = FALSE)

vars == <<tstate, left, rpcs, outbox, resp, req, meta, bstate, cancel, latch, phase, blockedOn>>

Count(q, f) == Cardinality({i \in 1..Len(q) : q[i] = f})

TypeOK ==
  /\ tstate \in [Sessions -> {"running", "waitoutbox", "waitmeta", "done", "exited"}]
  /\ left \in [Sessions -> 0..Chunks]
  /\ rpcs \in [Sessions -> 0..MaxRpcs]
  /\ outbox \in [Sessions -> Seq(Frames)]
  /\ resp \in [Sessions -> Seq(Frames)]
  /\ req \in [Sessions -> Seq({"metareply"})]
  /\ meta \in [Sessions -> {"none", "pending", "sent", "replied", "failed"}]
  /\ bstate \in [Sessions -> {"pull", "execmeta", "finished", "detached"}]
  /\ cancel \in [Sessions -> BOOLEAN]
  /\ latch \in BOOLEAN
  /\ phase \in {"wait", "pump", "dozing", "blocked"}
  /\ blockedOn \in Sessions \cup {NoSession}

Init ==
  /\ tstate = [s \in Sessions |-> "running"]
  /\ left = [s \in Sessions |-> Chunks]
  /\ rpcs = [s \in Sessions |-> MaxRpcs]
  /\ outbox = [s \in Sessions |-> <<>>]
  /\ resp = [s \in Sessions |-> <<>>]
  /\ req = [s \in Sessions |-> <<>>]
  /\ meta = [s \in Sessions |-> "none"]
  /\ bstate = [s \in Sessions |-> "pull"]
  /\ cancel = [s \in Sessions |-> FALSE]
  /\ latch = FALSE
  /\ phase = "wait"
  /\ blockedOn = NoSession

-----------------------------------------------------------------------------
(* Session thread *)

\* A result chunk is ready.  Once cancelled the thread drops frames instead
\* of waiting for an outbox nobody drains.
Produce(s) ==
  /\ tstate[s] = "running" /\ left[s] > 0
  /\ IF cancel[s]
       THEN UNCHANGED <<outbox, latch>>
       ELSE /\ Len(outbox[s]) < OutboxCap
            /\ outbox' = [outbox EXCEPT ![s] = Append(@, "chunk")]
            /\ latch' = TRUE
  /\ left' = [left EXCEPT ![s] = @ - 1]
  /\ UNCHANGED <<tstate, rpcs, resp, req, meta, bstate, cancel, phase, blockedOn>>

\* Outbox full: wait for main to drain it, or for cancel.
WaitOutbox(s) ==
  /\ tstate[s] = "running" /\ ~cancel[s] /\ Len(outbox[s]) = OutboxCap
  /\ tstate' = [tstate EXCEPT ![s] = "waitoutbox"]
  /\ UNCHANGED <<left, rpcs, outbox, resp, req, meta, bstate, cancel, latch, phase, blockedOn>>

OutboxRoom(s) ==
  /\ tstate[s] = "waitoutbox" /\ (Len(outbox[s]) < OutboxCap \/ cancel[s])
  /\ tstate' = [tstate EXCEPT ![s] = "running"]
  /\ UNCHANGED <<left, rpcs, outbox, resp, req, meta, bstate, cancel, latch, phase, blockedOn>>

\* A DuckDB thread of this session needs metadata: queue the request frame
\* and block until the reply (or a failure) comes back.
NeedMeta(s) ==
  /\ tstate[s] = "running" /\ ~cancel[s] /\ left[s] > 0 /\ rpcs[s] > 0
  /\ meta[s] = "none" /\ Len(outbox[s]) < OutboxCap
  /\ outbox' = [outbox EXCEPT ![s] = Append(@, "metareq")]
  /\ meta' = [meta EXCEPT ![s] = "pending"]
  /\ rpcs' = [rpcs EXCEPT ![s] = @ - 1]
  /\ tstate' = [tstate EXCEPT ![s] = "waitmeta"]
  /\ latch' = TRUE
  /\ UNCHANGED <<left, resp, req, bstate, cancel, phase, blockedOn>>

\* The reply arrived, or main failed the RPC: the thread unblocks.  A failed
\* RPC throws inside the query, which then unwinds; like every thread exit it
\* sets the latch so main reaps it.
MetaReturn(s) ==
  /\ tstate[s] = "waitmeta" /\ meta[s] \in {"replied", "failed"}
  /\ meta' = [meta EXCEPT ![s] = "none"]
  /\ tstate' = [tstate EXCEPT ![s] = IF meta[s] = "replied" THEN "running" ELSE "done"]
  /\ latch' = IF meta[s] = "replied" THEN latch ELSE TRUE
  /\ UNCHANGED <<left, rpcs, outbox, resp, req, bstate, cancel, phase, blockedOn>>

\* The query completed: queue the terminal frame.  Thread exit always sets
\* the latch, even when the frame is dropped.
Finish(s) ==
  /\ tstate[s] = "running" /\ left[s] = 0
  /\ IF cancel[s]
       THEN UNCHANGED outbox
       ELSE /\ Len(outbox[s]) < OutboxCap
            /\ outbox' = [outbox EXCEPT ![s] = Append(@, "done")]
  /\ latch' = TRUE
  /\ tstate' = [tstate EXCEPT ![s] = "done"]
  /\ UNCHANGED <<left, rpcs, resp, req, meta, bstate, cancel, phase, blockedOn>>

\* Interrupt() unwound the query.
Interrupted(s) ==
  /\ tstate[s] \in {"running", "waitoutbox"} /\ cancel[s]
  /\ tstate' = [tstate EXCEPT ![s] = "done"]
  /\ latch' = TRUE
  /\ UNCHANGED <<left, rpcs, outbox, resp, req, meta, bstate, cancel, phase, blockedOn>>

-----------------------------------------------------------------------------
(* Main thread *)

\* WaitLatch returned.  The implemented loop resets the latch before it
\* scans, so a wakeup that lands during the scan is seen on the next wait.
MainWake ==
  /\ phase = "wait" /\ latch
  /\ phase' = "pump"
  /\ latch' = IF ResetLatchFirst THEN FALSE ELSE latch
  /\ UNCHANGED <<tstate, left, rpcs, outbox, resp, req, meta, bstate, cancel, blockedOn>>

\* What a pump pass can do for a session.
CanSend(s) == bstate[s] /= "detached" /\ outbox[s] /= <<>> /\ Len(resp[s]) < RespCap
WouldBlock(s) == bstate[s] /= "detached" /\ outbox[s] /= <<>> /\ Len(resp[s]) >= RespCap
CanRecv(s) == req[s] /= <<>>
CanObserveDetach(s) == bstate[s] = "detached" /\ ~cancel[s]
CanReap(s) == tstate[s] = "done" /\ (outbox[s] = <<>> \/ bstate[s] = "detached")

HasWork ==
  \E s \in Sessions :
    CanSend(s) \/ (~NonBlockingSend /\ WouldBlock(s)) \/ CanRecv(s)
    \/ CanObserveDetach(s) \/ CanReap(s)

DoSend(s) ==
  /\ resp' = [resp EXCEPT ![s] = Append(@, Head(outbox[s]))]
  /\ outbox' = [outbox EXCEPT ![s] = Tail(@)]
  /\ meta' = IF Head(outbox[s]) = "metareq" THEN [meta EXCEPT ![s] = "sent"] ELSE meta

\* shm_mq_send(nowait): move the head of a session's outbox onto its ring.
Send(s) ==
  /\ phase = "pump" /\ CanSend(s)
  /\ DoSend(s)
  /\ UNCHANGED <<tstate, left, rpcs, req, bstate, cancel, latch, phase, blockedOn>>

\* Bug mode: a blocking send parks the whole main loop on one session's ring.
SendBlocks(s) ==
  /\ ~NonBlockingSend
  /\ phase = "pump" /\ WouldBlock(s)
  /\ phase' = "blocked" /\ blockedOn' = s
  /\ UNCHANGED <<tstate, left, rpcs, outbox, resp, req, meta, bstate, cancel, latch>>

BlockedSendDone ==
  /\ phase = "blocked"
  /\ LET s == blockedOn IN
       /\ \/ bstate[s] = "detached" /\ UNCHANGED <<resp, outbox, meta>>
          \/ bstate[s] /= "detached" /\ Len(resp[s]) < RespCap /\ DoSend(s)
  /\ phase' = "pump" /\ blockedOn' = NoSession
  /\ UNCHANGED <<tstate, left, rpcs, req, bstate, cancel, latch>>

\* shm_mq_receive(nowait): an RPC reply came in; hand it to the waiting thread.
Recv(s) ==
  /\ phase = "pump" /\ CanRecv(s)
  /\ req' = [req EXCEPT ![s] = Tail(@)]
  /\ meta' = [meta EXCEPT ![s] = IF meta[s] = "sent" THEN "replied" ELSE meta[s]]
  /\ UNCHANGED <<tstate, left, rpcs, outbox, resp, bstate, cancel, latch, phase, blockedOn>>

\* SHM_MQ_DETACHED on a session: Interrupt() its context, drop its outbox and
\* fail any RPC in flight so the thread cannot wait for a reply that will
\* never come.
ObserveDetach(s) ==
  /\ phase = "pump" /\ CanObserveDetach(s)
  /\ cancel' = [cancel EXCEPT ![s] = TRUE]
  /\ outbox' = [outbox EXCEPT ![s] = <<>>]
  /\ meta' = IF FailMetaOnDetach /\ meta[s] \in {"pending", "sent"}
               THEN [meta EXCEPT ![s] = "failed"] ELSE meta
  /\ UNCHANGED <<tstate, left, rpcs, resp, req, bstate, latch, phase, blockedOn>>

\* Join a finished thread once its frames are on the wire (or moot).
Reap(s) ==
  /\ phase = "pump" /\ CanReap(s)
  /\ tstate' = [tstate EXCEPT ![s] = "exited"]
  /\ UNCHANGED <<left, rpcs, outbox, resp, req, meta, bstate, cancel, latch, phase, blockedOn>>

\* The pass found nothing left to do.  Going back to WaitLatch is a separate
\* step: a wakeup can land in between, and the implemented order (reset in
\* MainWake, before the scan) keeps it visible.
MainScanEmpty ==
  /\ phase = "pump" /\ ~HasWork
  /\ phase' = "dozing"
  /\ UNCHANGED <<tstate, left, rpcs, outbox, resp, req, meta, bstate, cancel, latch, blockedOn>>

MainSleep ==
  /\ phase = "dozing"
  /\ phase' = "wait"
  /\ latch' = IF ResetLatchFirst THEN latch ELSE FALSE
  /\ UNCHANGED <<tstate, left, rpcs, outbox, resp, req, meta, bstate, cancel, blockedOn>>

-----------------------------------------------------------------------------
(* Backend *)

\* shm_mq_receive on the response ring; every receive wakes the sender.
Pull(s) ==
  /\ bstate[s] = "pull" /\ resp[s] /= <<>>
  /\ resp' = [resp EXCEPT ![s] = Tail(@)]
  /\ latch' = TRUE
  /\ bstate' = [bstate EXCEPT ![s] =
       CASE Head(resp[s]) = "chunk" -> "pull"
         [] Head(resp[s]) = "metareq" -> "execmeta"
         [] OTHER -> "finished"]
  /\ UNCHANGED <<tstate, left, rpcs, outbox, req, meta, cancel, phase, blockedOn>>

\* The metadata SQL ran locally; send the reply and wake the worker.
Reply(s) ==
  /\ bstate[s] = "execmeta" /\ Len(req[s]) < ReqCap
  /\ req' = [req EXCEPT ![s] = Append(@, "metareply")]
  /\ latch' = TRUE
  /\ bstate' = [bstate EXCEPT ![s] = "pull"]
  /\ UNCHANGED <<tstate, left, rpcs, outbox, resp, meta, cancel, phase, blockedOn>>

\* EndCustomScan: dsm_detach; shm_mq wakes the peer.
FinishDetach(s) ==
  /\ bstate[s] = "finished"
  /\ bstate' = [bstate EXCEPT ![s] = "detached"]
  /\ latch' = TRUE
  /\ UNCHANGED <<tstate, left, rpcs, outbox, resp, req, meta, cancel, phase, blockedOn>>

\* ERROR / cancel / FATAL while pulling or while servicing an RPC.
Abort(s) ==
  /\ bstate[s] \in {"pull", "execmeta"}
  /\ bstate' = [bstate EXCEPT ![s] = "detached"]
  /\ latch' = TRUE
  /\ UNCHANGED <<tstate, left, rpcs, outbox, resp, req, meta, cancel, phase, blockedOn>>

-----------------------------------------------------------------------------

AllDone == \A s \in Sessions : tstate[s] = "exited" /\ bstate[s] = "detached"
Terminating == AllDone /\ UNCHANGED vars

Next ==
  \/ \E s \in Sessions :
       \/ Produce(s) \/ WaitOutbox(s) \/ OutboxRoom(s) \/ NeedMeta(s) \/ MetaReturn(s)
       \/ Finish(s) \/ Interrupted(s)
       \/ Send(s) \/ SendBlocks(s) \/ Recv(s) \/ ObserveDetach(s) \/ Reap(s)
       \/ Pull(s) \/ Reply(s) \/ FinishDetach(s) \/ Abort(s)
  \/ MainWake \/ BlockedSendDone \/ MainScanEmpty \/ MainSleep
  \/ Terminating

\* Abort is never forced; backends outside FairBackends may stall forever.
Fairness ==
  /\ \A s \in Sessions :
       /\ WF_vars(Produce(s)) /\ WF_vars(WaitOutbox(s)) /\ WF_vars(OutboxRoom(s))
       /\ WF_vars(MetaReturn(s)) /\ WF_vars(Finish(s)) /\ WF_vars(Interrupted(s))
       /\ WF_vars(Send(s)) /\ WF_vars(SendBlocks(s)) /\ WF_vars(Recv(s))
       /\ WF_vars(ObserveDetach(s)) /\ WF_vars(Reap(s))
  /\ WF_vars(MainWake) /\ WF_vars(MainScanEmpty) /\ WF_vars(MainSleep) /\ WF_vars(BlockedSendDone)
  /\ \A s \in FairBackends :
       WF_vars(Pull(s)) /\ WF_vars(Reply(s)) /\ WF_vars(FinishDetach(s))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety *)

Bounded ==
  \A s \in Sessions :
    Len(outbox[s]) <= OutboxCap /\ Len(resp[s]) <= RespCap /\ Len(req[s]) <= ReqCap

\* At most one metadata round trip is in flight per session, so replies need
\* no request id.
OneMetaInFlight ==
  \A s \in Sessions :
    Count(outbox[s], "metareq") + Count(resp[s], "metareq")
      + (IF bstate[s] = "execmeta" THEN 1 ELSE 0) + Len(req[s]) <= 1

\* While the backend is attached, a reply on the request ring answers the
\* request this session sent.  A reply written just before a detach is stale
\* and is never delivered.
ReplyMatchesRequest ==
  \A s \in Sessions : (req[s] /= <<>> /\ bstate[s] /= "detached") => meta[s] = "sent"

\* Nothing follows the terminal frame on the wire.
TerminalIsLast ==
  \A s \in Sessions :
    \A i \in 1..Len(resp[s]) : resp[s][i] = "done" => i = Len(resp[s])

\* The main thread never parks on one session.
MainNeverBlocked == NonBlockingSend => phase /= "blocked"

\* Main only sleeps with its latch clear when there is nothing to do: every
\* producer sets the latch after queueing work, and the loop resets the latch
\* before it scans, so a wakeup can never be lost.
NoLostWakeup == (phase = "wait" /\ ~latch) => ~HasWork

(* Liveness *)

\* Every session whose backend keeps pulling is served to completion and
\* reaped, whatever the other backends do.
FairSessionsExit == \A s \in FairBackends : (tstate[s] /= "exited") ~> (tstate[s] = "exited")

FairBackendsSettle == \A s \in FairBackends : <>(bstate[s] = "detached")

=============================================================================
