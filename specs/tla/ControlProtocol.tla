----------------------------- MODULE ControlProtocol -----------------------------
(***************************************************************************)
(* A TLA+ model of the SESSION-POOL SESSION LIFECYCLE of the shared        *)
(* DuckDB duckdb worker (libpgduckdb/worker/duckdb_worker.cpp +            *)
(* libpgduckdb/worker/transport/session_pool.cpp).                         *)
(*                                                                         *)
(* What is modelled: how a backend gets a session onto the per-database    *)
(* worker and how the session slot is returned, under aborts, crashes, and *)
(* capacity pressure. The data plane (frames, chunks, demux lanes) is      *)
(* modelled separately in ScanInversion.tla; here a served session simply  *)
(* "finishes".                                                             *)
(*                                                                         *)
(* The protocol (DuckdbWorker::OpenSession -> worker main loop ->          *)
(* SessionThreadMain):                                                     *)
(*   - SessionPool::Acquire takes the first FREE slot (state -> IN_USE,    *)
(*     GENERATION bumped, attach refcount reset to 0) and the backend      *)
(*     immediately AttachEnds (refcount 1) and re-creates the channel      *)
(*     (SessionChannel::OpenSlot resets the header cancel flag). Nothing   *)
(*     fallible sits between the calls, and from that moment the slot is   *)
(*     tracked for cleanup (g_pending_conn_slot, then the open-stream      *)
(*     registry), so acquire+attach+track is modelled as ONE atomic step.  *)
(*     The SplitAcquire constant breaks that atomicity to show the bug     *)
(*     class the code avoids.                                              *)
(*   - CAPACITY WAITS, not fallback: when Acquire returns -1 (pool full)   *)
(*     the backend retries in a cancellable wait loop (interrupt check +   *)
(*     10ms latch wait) until a slot frees; it never falls back to         *)
(*     in-process execution for capacity reasons. Modelled by Acquire      *)
(*     simply not being enabled -- the backend takes no transition until a *)
(*     slot is FREE (weak fairness on the acquire action stands in for the *)
(*     retry loop). In-process execution happens only for statements the   *)
(*     dispatch gate excludes semantically, which is outside this model.   *)
(*   - The backend enqueues {slot index, slot generation} on the worker's  *)
(*     pending-session ring (EnqueuePendingSession) and pokes the worker   *)
(*     latch. A full ring or a missing worker also means WAIT-and-retry:   *)
(*     if the worker registry slot is gone the backend calls               *)
(*     EnsureWorkerForMyDatabase (which reclaims dead workers and          *)
(*     respawns), then re-attempts the enqueue; if the ring is just full   *)
(*     it waits for the worker to drain it. An abort during either wait is *)
(*     fine: the pool-full wait holds nothing yet, and the ring-full wait  *)
(*     holds a slot the cleanup callbacks release.                         *)
(*   - THE HANDSHAKE: after the enqueue lands the backend sends the        *)
(*     Snapshot and SQL frames and constructs the result stream            *)
(*     (SendHandshake here; then it is "served"). Independently, the       *)
(*     worker main loop drains the ring and spawns a session thread per    *)
(*     entry; the thread calls SessionPool::TryAttachEnd(idx, gen), which  *)
(*     attaches (refcount 2) only if the slot is still IN_USE at the       *)
(*     enqueued generation -- a STALE entry, whose backend released the    *)
(*     slot (or whose slot was re-acquired) after enqueueing, is skipped.  *)
(*     The ValidateGeneration constant turns that check off to reproduce   *)
(*     that pre-fix race. An attached thread then WAITS in the handshake   *)
(*     receive (SerializedRecvControl in RunOneSession): a nowait-recv     *)
(*     poll loop that observes EITHER a received frame OR the channel      *)
(*     cancel flag / worker drain (on which it returns Detached).          *)
(*     Modelled as the wwait counter and the HandshakeOk /                 *)
(*     HandshakeCancelled steps. After the query the thread DetachEnds.    *)
(*     Each side's DetachEnd decrements; the one that reaches 0 flips the  *)
(*     slot back to FREE.                                                  *)
(*   - Backend abort/exit: the xact-abort callbacks and before_shmem_exit  *)
(*     run ReleaseOpenSessions -> the BackendSession destructor cancels,   *)
(*     detaches, DetachEnds. Modelled as abort actions in each phase.      *)
(*     An abort while the session is still pending (enqueued, handshake    *)
(*     frames not yet sent) leaves a stale ring entry AND possibly a       *)
(*     session thread already attached and parked in the handshake         *)
(*     receive. The fixed cleanup path is TWO steps                        *)
(*     (ReleaseOpenSessions, g_pending_conn_slot branch): FIRST            *)
(*     SessionChannel::RequestCancelAt sets the channel cancel flag (the   *)
(*     parked thread's poll sees it and detaches), THEN DetachEnd releases *)
(*     the backend's own refcount. AbortSetsCancel = FALSE drops the       *)
(*     cancel step (the pre-fix code, which only DetachEnd-ed): a thread   *)
(*     already parked in the handshake then waits forever and its refcount *)
(*     pins the slot -- the wedged-thread slot leak.                       *)
(*     Subtransaction aborts release only streams created in that          *)
(*     subtransaction -- a refinement below this model's granularity (it   *)
(*     never releases a slot twice, so the accounting here is unaffected). *)
(*   - Worker death: the duckdb worker drains its session threads in a     *)
(*     before_shmem_exit callback, so ERROR/FATAL exits DetachEnd cleanly  *)
(*     -- those look like ordinary SessionFinish steps here. A HARD kill   *)
(*     (SIGKILL, segfault) skips the callback; in a real cluster the       *)
(*     postmaster then crash-restarts and reinitializes shared memory.     *)
(*     WorkerCrash models the counterfactual "hard kill with no shmem      *)
(*     reset" to show why the drain callback (and the crash-restart)       *)
(*     matter: without them, worker-attached refcounts strand their slots. *)
(*     EnsureWorkerForMyDatabase's pid probe reclaims the REGISTRY slot so *)
(*     a respawned worker starts with an empty pending ring                *)
(*     (WorkerRespawn); it does not touch session-pool refcounts.          *)
(*                                                                         *)
(* CONSTANTS pick the adversary:                                           *)
(*   Backends       - backend ids, one query each. Use one more backend    *)
(*                    than NSlots to exercise the pool-full wait.          *)
(*   NSlots         - session-pool slots.                                  *)
(*   PendingCap     - pending-ring capacity (1024 in code; small here to   *)
(*                    exercise the ring-full wait).                        *)
(*   AllowAbort     - backend may abort while WAITING for a slot, while    *)
(*                    ACQUIRED (incl. the ring-full wait), or while SERVED *)
(*                    (cleanup callback releases whatever it holds).       *)
(*   AllowEarlyAbort- backend may abort while PENDING (enqueued, handshake *)
(*                    frames not yet sent), leaving a stale ring entry and *)
(*                    possibly a parked session thread.                    *)
(*   AbortSetsCancel- TRUE: the pending-abort path sets the channel cancel *)
(*                    flag before DetachEnd (RequestCancelAt, the          *)
(*                    implemented fix), so a parked handshake thread bails.*)
(*                    FALSE: it only DetachEnds (pre-fix): expect FAIL     *)
(*                    (deadlock at the wedged-thread slot leak).           *)
(*   ValidateGeneration - TRUE: SessionThreadMain uses TryAttachEnd (the   *)
(*                    implemented fix). FALSE: attach unconditionally (the *)
(*                    pre-fix race): expect FAIL.                          *)
(*   AllowWorkerCrash - hard worker kill with no drain and no shmem reset  *)
(*                    (see above). Expect FAIL (deadlock at a leaked       *)
(*                    slot): the counterfactual the drain callback and     *)
(*                    postmaster crash-restart exist to prevent.           *)
(*   SplitAcquire   - model Acquire and the backend's AttachEnd as two     *)
(*                    steps with an untracked crash window between them.   *)
(*                    Expect FAIL (leaked slot) -- the bug class the real  *)
(*                    code prevents by making acquire+attach+track         *)
(*                    effectively atomic.                                  *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Backends,          \* backend ids, e.g. {b1, b2, b3}
    NSlots,            \* session-pool slots, e.g. 2
    PendingCap,        \* pending-session ring capacity, e.g. 1
    AllowAbort,        \* abort in IDLE-wait / ACQUIRED / SERVED phases
    AllowEarlyAbort,   \* abort in PENDING phase (stale ring entry + parked thread)
    AbortSetsCancel,   \* TRUE: pending-abort also sets the cancel flag (the fix)
    ValidateGeneration,\* TRUE: TryAttachEnd generation check (the fix)
    AllowWorkerCrash,  \* hard worker kill, no drain, no shmem reset
    SplitAcquire       \* break acquire+attach atomicity (the _bug variant)

Slots == 1 .. NSlots

\* Backend phases. "idle" doubles as the pool-full wait (the backend holds
\* nothing while Acquire keeps returning -1). "acquired" doubles as the
\* ring-full / worker-missing wait (the backend holds the slot). "pending"
\* is enqueued-but-handshake-not-sent; "served" is handshake sent + stream
\* constructed. "aborting" is mid-ReleaseOpenSessions on a pending abort
\* (cancel flag written, own DetachEnd not yet done). "acquired0" exists
\* only under SplitAcquire (slot IN_USE, refcount 0, untracked).
BPhases == {"idle","acquired0","acquired","pending","aborting","served","settled"}

VARIABLES
    \* --- the session pool (slot array in session_pool.cpp) ---
    sstate,   \* [Slots -> {"FREE","INUSE"}]      slot state
    sgen,     \* [Slots -> Nat]                   slot generation
    att,      \* [Slots -> Int]                   attach refcount
    \* --- the session channel header per slot ---
    cancel,   \* [Slots -> BOOLEAN]: the channel cancel flag (reset by OpenSlot)
    hs,       \* [Slots -> BOOLEAN]: handshake frames (Snapshot+SQL) are in the mq
    \* --- the worker registry slot for this database ---
    pending,  \* Seq of [slot, gen]: the pending-session ring (the code stores
              \* PendingSession {conn_slot, conn_generation})
    wup,      \* BOOLEAN: the duckdb worker process is alive
    wgen,     \* Nat: worker incarnation (bumped by respawn)
    crashes,  \* 0..1: crashes so far (bounds the state space)
    wwait,    \* [Slots -> Nat]: attached session threads parked in the handshake recv
    wheld,    \* [Slots -> Nat]: attached session threads past the handshake (running)
    \* --- per backend ---
    phase,    \* [Backends -> BPhases]
    bslot,    \* [Backends -> Slots \cup {0}]: the slot this backend claimed
    bwgen     \* [Backends -> Nat]: worker incarnation it enqueued under

vars == << sstate, sgen, att, cancel, hs, pending, wup, wgen, crashes,
           wwait, wheld, phase, bslot, bwgen >>

----------------------------------------------------------------------------
TypeOK ==
    /\ sstate \in [Slots -> {"FREE","INUSE"}]
    /\ sgen \in [Slots -> Nat]
    /\ att \in [Slots -> Int]
    /\ cancel \in [Slots -> BOOLEAN]
    /\ hs \in [Slots -> BOOLEAN]
    /\ \A i \in DOMAIN pending :
         /\ pending[i].slot \in Slots
         /\ pending[i].gen \in Nat
    /\ wup \in BOOLEAN
    /\ wgen \in Nat
    /\ crashes \in 0 .. 1
    /\ wwait \in [Slots -> Nat]
    /\ wheld \in [Slots -> Nat]
    /\ phase \in [Backends -> BPhases]
    /\ bslot \in [Backends -> Slots \cup {0}]
    /\ bwgen \in [Backends -> Nat]

Init ==
    /\ sstate = [s \in Slots |-> "FREE"]
    /\ sgen = [s \in Slots |-> 0]
    /\ att = [s \in Slots |-> 0]
    /\ cancel = [s \in Slots |-> FALSE]
    /\ hs = [s \in Slots |-> FALSE]
    /\ pending = << >>
    /\ wup = TRUE
    /\ wgen = 0
    /\ crashes = 0
    /\ wwait = [s \in Slots |-> 0]
    /\ wheld = [s \in Slots |-> 0]
    /\ phase = [b \in Backends |-> "idle"]
    /\ bslot = [b \in Backends |-> 0]
    /\ bwgen = [b \in Backends |-> 0]

\* First-fit, like SessionPool::Acquire's linear scan.
FreeSlots  == { s \in Slots : sstate[s] = "FREE" }
FirstFree  == CHOOSE s \in FreeSlots : \A t \in FreeSlots : s =< t

\* DetachEnd: decrement; the end that reaches 0 flips the slot to FREE.
\* (In C++ a fetch_sub whose result would be negative wraps -- NoUnderflow
\* below is the invariant standing in for that. Modelled as one atomic step;
\* in the code the decrement itself is outside the pool spinlock.)
Detach(s) ==
    /\ att' = [att EXCEPT ![s] = att[s] - 1]
    /\ sstate' = IF att[s] - 1 = 0 THEN [sstate EXCEPT ![s] = "FREE"] ELSE sstate

----------------------------------------------------------------------------
(* --- Backend: DuckdbWorker::OpenSession --- *)

\* Acquire + the backend's AttachEnd + channel OpenSlot (which re-inits the
\* header, resetting the cancel flag) + cleanup tracking, as one atomic step
\* (see the header note). Acquire bumps the slot generation and clobbers the
\* refcount (pg_atomic_write_u32(&attached, 0)); the AttachEnd then makes it 1.
\* When the pool is full this action is simply DISABLED: the backend sits in
\* OpenSession's cancellable retry loop (no fallback) until a slot frees --
\* the WF on backend progress below is the retry loop.
AcquireAttach(b) ==
    /\ ~SplitAcquire
    /\ phase[b] = "idle"
    /\ FreeSlots # {}
    /\ LET s == FirstFree IN
         /\ sstate' = [sstate EXCEPT ![s] = "INUSE"]
         /\ sgen' = [sgen EXCEPT ![s] = sgen[s] + 1]
         /\ att' = [att EXCEPT ![s] = 1]
         /\ cancel' = [cancel EXCEPT ![s] = FALSE]
         /\ hs' = [hs EXCEPT ![s] = FALSE]
         /\ bslot' = [bslot EXCEPT ![b] = s]
    /\ phase' = [phase EXCEPT ![b] = "acquired"]
    /\ UNCHANGED << pending, wup, wgen, crashes, wwait, wheld, bwgen >>

\* --- SplitAcquire (_bug) variant: the two halves as separate steps, with an
\* --- untracked crash window between them. If the backend dies in that window
\* --- nothing ever DetachEnds the slot: it stays INUSE/refcount-0 forever.
AcquireOnly(b) ==
    /\ SplitAcquire
    /\ phase[b] = "idle"
    /\ FreeSlots # {}
    /\ LET s == FirstFree IN
         /\ sstate' = [sstate EXCEPT ![s] = "INUSE"]
         /\ sgen' = [sgen EXCEPT ![s] = sgen[s] + 1]
         /\ att' = [att EXCEPT ![s] = 0]      \* Acquire resets the refcount
         /\ cancel' = [cancel EXCEPT ![s] = FALSE]
         /\ hs' = [hs EXCEPT ![s] = FALSE]
         /\ bslot' = [bslot EXCEPT ![b] = s]
    /\ phase' = [phase EXCEPT ![b] = "acquired0"]
    /\ UNCHANGED << pending, wup, wgen, crashes, wwait, wheld, bwgen >>

BackendAttach(b) ==
    /\ phase[b] = "acquired0"
    /\ att' = [att EXCEPT ![bslot[b]] = att[bslot[b]] + 1]
    /\ phase' = [phase EXCEPT ![b] = "acquired"]
    /\ UNCHANGED << sstate, sgen, cancel, hs, pending, wup, wgen, crashes,
                    wwait, wheld, bslot, bwgen >>

\* Backend dies between acquire and attach: the cleanup callback knows nothing
\* about the slot (g_pending_conn_slot is only set after AttachEnd), so the
\* slot is orphaned. Adversarial; only reachable under SplitAcquire.
CrashBetween(b) ==
    /\ phase[b] = "acquired0"
    /\ phase' = [phase EXCEPT ![b] = "settled"]
    /\ UNCHANGED << sstate, sgen, att, cancel, hs, pending, wup, wgen, crashes,
                    wwait, wheld, bslot, bwgen >>

\* EnqueuePendingSession succeeded: the session is pending on the worker,
\* tagged with the slot's generation at enqueue time. When the ring is full or
\* the worker is down this is DISABLED: the backend waits in OpenSession's
\* enqueue retry loop, holding its slot (a dead worker is respawned by the
\* same loop -- WorkerRespawn below).
Enqueue(b) ==
    /\ phase[b] = "acquired"
    /\ wup
    /\ Len(pending) < PendingCap
    /\ pending' = Append(pending, [slot |-> bslot[b], gen |-> sgen[bslot[b]]])
    /\ bwgen' = [bwgen EXCEPT ![b] = wgen]
    /\ phase' = [phase EXCEPT ![b] = "pending"]
    /\ UNCHANGED << sstate, sgen, att, cancel, hs, wup, wgen, crashes,
                    wwait, wheld, bslot >>

\* The backend ships the Snapshot and SQL frames and constructs the result
\* stream (BackendSession); from here the open-stream registry owns the slot.
\* Independent of the worker's ring drain: the frames just sit in the shm_mq
\* until the session thread's handshake receive consumes them. (An abort
\* between the two frame sends is below this model's granularity: the parked
\* thread's poll observes frame-or-cancel either way.)
SendHandshake(b) ==
    /\ phase[b] = "pending"
    /\ hs' = [hs EXCEPT ![bslot[b]] = TRUE]
    /\ phase' = [phase EXCEPT ![b] = "served"]
    /\ UNCHANGED << sstate, sgen, att, cancel, pending, wup, wgen, crashes,
                    wwait, wheld, bslot, bwgen >>

----------------------------------------------------------------------------
(* --- Worker: main loop drain + SessionThreadMain --- *)

\* Drain one pending entry and spawn its session thread. The thread calls
\* TryAttachEnd(slot, gen): attach only if the slot is still IN_USE at the
\* enqueued generation; a stale entry (its backend released the slot, or the
\* slot was re-acquired -- Acquire bumped the generation) is skipped and the
\* thread exits without touching the channel. ValidateGeneration = FALSE
\* models the pre-fix unconditional AttachEnd. An attached thread parks in
\* the handshake receive (wwait) until HandshakeOk / HandshakeCancelled.
WorkerServe ==
    /\ wup
    /\ pending # << >>
    /\ LET e == Head(pending)
           matched == sstate[e.slot] = "INUSE" /\ sgen[e.slot] = e.gen
       IN
         /\ pending' = Tail(pending)
         /\ IF ValidateGeneration /\ ~matched
              THEN \* TryAttachEnd rejects the stale entry: skip.
                   UNCHANGED << att, wwait >>
              ELSE /\ wwait' = [wwait EXCEPT ![e.slot] = wwait[e.slot] + 1]
                   /\ att' = [att EXCEPT ![e.slot] = att[e.slot] + 1]
    /\ UNCHANGED << sstate, sgen, cancel, hs, wup, wgen, crashes, wheld,
                    phase, bslot, bwgen >>

\* The handshake receive (SerializedRecvControl polled in RunOneSession) sees
\* the Snapshot frame: the thread proceeds to run the session.
HandshakeOk ==
    /\ wup
    /\ \E s \in Slots :
         /\ wwait[s] > 0
         /\ hs[s]
         /\ wwait' = [wwait EXCEPT ![s] = wwait[s] - 1]
         /\ wheld' = [wheld EXCEPT ![s] = wheld[s] + 1]
    /\ UNCHANGED << sstate, sgen, att, cancel, hs, pending, wup, wgen, crashes,
                    phase, bslot, bwgen >>

\* The handshake receive sees the channel cancel flag instead
\* (SerializedRecvControl returns Detached): the thread detaches without ever
\* running -- this is what frees the slot when the backend aborted mid-
\* handshake. Only reachable when the abort path SET the flag
\* (AbortSetsCancel); without it the parked thread has no exit.
HandshakeCancelled ==
    /\ wup
    /\ \E s \in Slots :
         /\ wwait[s] > 0
         /\ cancel[s]
         /\ wwait' = [wwait EXCEPT ![s] = wwait[s] - 1]
         /\ Detach(s)
    /\ UNCHANGED << sgen, cancel, hs, pending, wup, wgen, crashes, wheld,
                    phase, bslot, bwgen >>

\* A running session thread finishes (query complete, error, cancel, or peer
\* detach) and DetachEnds its end.
SessionFinish ==
    /\ wup
    /\ \E s \in Slots :
         /\ wheld[s] > 0
         /\ wheld' = [wheld EXCEPT ![s] = wheld[s] - 1]
         /\ Detach(s)
    /\ UNCHANGED << sgen, cancel, hs, pending, wup, wgen, crashes, wwait,
                    phase, bslot, bwgen >>

\* Counterfactual hard kill (see header): session threads vanish WITHOUT
\* DetachEnd -- their refcounts stay behind -- and no shmem reset follows.
\* (An ERROR/FATAL exit instead runs the worker's before_shmem_exit drain:
\* every session thread is joined and DetachEnds, i.e. ordinary SessionFinish
\* steps before the process goes away.)
WorkerCrash ==
    /\ AllowWorkerCrash
    /\ crashes < 1
    /\ wup
    /\ wup' = FALSE
    /\ crashes' = crashes + 1
    /\ wwait' = [s \in Slots |-> 0]
    /\ wheld' = [s \in Slots |-> 0]
    /\ UNCHANGED << sstate, sgen, att, cancel, hs, pending, wgen, phase, bslot, bwgen >>

\* EnsureWorkerForMyDatabase's pid probe reclaims the dead worker's REGISTRY
\* slot and respawns it: the new worker starts with an empty pending ring
\* (pending_head = pending_tail = 0). Session-pool refcounts are untouched.
\* Any dispatching backend drives this -- OpenSession calls EnsureWorker at
\* the top, and the enqueue retry loop calls it whenever the registry slot is
\* gone -- so it gets weak fairness below.
WorkerRespawn ==
    /\ ~wup
    /\ wup' = TRUE
    /\ wgen' = wgen + 1
    /\ pending' = << >>
    /\ UNCHANGED << sstate, sgen, att, cancel, hs, crashes, wwait, wheld,
                    phase, bslot, bwgen >>

----------------------------------------------------------------------------
(* --- Backend: session end and aborts --- *)

\* Normal end (BackendSession destructor after Complete/Error) or an abort
\* while streaming: either way ReleaseOpenSessions detaches the backend's end.
\* (The destructor also sets the cancel flag when the query had not finished;
\* a thread still parked in the handshake can equally proceed via the already-
\* sent frames -- hs is TRUE in "served" -- so the flag write is omitted here
\* to keep the state space small.)
BackendFinish(b) ==
    /\ phase[b] = "served"
    /\ Detach(bslot[b])
    /\ phase' = [phase EXCEPT ![b] = "settled"]
    /\ UNCHANGED << sgen, cancel, hs, pending, wup, wgen, crashes, wwait, wheld,
                    bslot, bwgen >>

\* Abort during the pool-full wait (cancel / statement_timeout inside the
\* Acquire retry loop): nothing is held yet, nothing to release.
AbortIdle(b) ==
    /\ AllowAbort
    /\ phase[b] = "idle"
    /\ phase' = [phase EXCEPT ![b] = "settled"]
    /\ UNCHANGED << sstate, sgen, att, cancel, hs, pending, wup, wgen, crashes,
                    wwait, wheld, bslot, bwgen >>

\* Abort after acquiring but before the enqueue lands -- including a cancel
\* inside the enqueue retry loop while the ring is full or the worker is
\* down: the g_pending_conn_slot branch of ReleaseOpenSessions covers the
\* slot -- RequestCancelAt (if AbortSetsCancel) then DetachEnd. No ring entry
\* and no session thread exist yet, so nothing can observe the ordering:
\* modelled as one atomic step.
AbortAcquired(b) ==
    /\ AllowAbort
    /\ phase[b] = "acquired"
    /\ cancel' = IF AbortSetsCancel THEN [cancel EXCEPT ![bslot[b]] = TRUE] ELSE cancel
    /\ Detach(bslot[b])
    /\ phase' = [phase EXCEPT ![b] = "settled"]
    /\ UNCHANGED << sgen, hs, pending, wup, wgen, crashes, wwait, wheld,
                    bslot, bwgen >>

\* Abort (or backend FATAL exit) while the session is PENDING: the ring entry
\* stays (stale; the generation check must reject it) and a session thread
\* may ALREADY be attached and parked in the handshake receive. The cleanup
\* path is two steps, modelled separately because the parked thread races
\* with them: FIRST the cancel-flag write (RequestCancelAt -- skipped when
\* AbortSetsCancel = FALSE, the pre-fix code)...
EarlyAbortCancel(b) ==
    /\ AllowEarlyAbort
    /\ phase[b] = "pending"
    /\ cancel' = IF AbortSetsCancel THEN [cancel EXCEPT ![bslot[b]] = TRUE] ELSE cancel
    /\ phase' = [phase EXCEPT ![b] = "aborting"]
    /\ UNCHANGED << sstate, sgen, att, hs, pending, wup, wgen, crashes,
                    wwait, wheld, bslot, bwgen >>

\* ...THEN the backend's own DetachEnd. Straight-line code inside the cleanup
\* callback, so it is a fair progress step once "aborting".
EarlyAbortDetach(b) ==
    /\ phase[b] = "aborting"
    /\ Detach(bslot[b])
    /\ phase' = [phase EXCEPT ![b] = "settled"]
    /\ UNCHANGED << sgen, cancel, hs, pending, wup, wgen, crashes, wwait, wheld,
                    bslot, bwgen >>

\* The backend's fetch loop probes the worker pid (PidIsAlive in Fetch) or its
\* enqueue was lost to a respawn: give up, destructor DetachEnds. Models the
\* "duckdb worker terminated unexpectedly" error path.
DetectDeath(b) ==
    /\ phase[b] \in {"pending","served"}
    /\ (~wup \/ bwgen[b] # wgen)
    /\ Detach(bslot[b])
    /\ phase' = [phase EXCEPT ![b] = "settled"]
    /\ UNCHANGED << sgen, cancel, hs, pending, wup, wgen, crashes, wwait, wheld,
                    bslot, bwgen >>

----------------------------------------------------------------------------
(* --- Terminal / stutter --- *)

AllSettled == \A b \in Backends : phase[b] = "settled"

Quiescent ==
    /\ AllSettled
    /\ \A s \in Slots : sstate[s] = "FREE" /\ att[s] = 0 /\ wwait[s] = 0 /\ wheld[s] = 0
    /\ pending = << >>

Terminating == Quiescent /\ UNCHANGED vars

BackendProgress(b) ==
    \/ AcquireAttach(b)
    \/ AcquireOnly(b) \/ BackendAttach(b)
    \/ Enqueue(b)
    \/ SendHandshake(b)
    \/ EarlyAbortDetach(b)
    \/ BackendFinish(b) \/ DetectDeath(b)

WorkerProgress == WorkerServe \/ HandshakeOk \/ HandshakeCancelled \/ SessionFinish

Next ==
    \/ \E b \in Backends : BackendProgress(b)
    \/ WorkerProgress
    \/ WorkerRespawn
    \/ \E b \in Backends : AbortIdle(b) \/ AbortAcquired(b) \/ EarlyAbortCancel(b)
                           \/ CrashBetween(b)
    \/ WorkerCrash
    \/ Terminating

\* Fairness on progress only; aborts and crashes are adversarial (never
\* forced), but once an abort STARTED (EarlyAbortCancel) its DetachEnd is
\* straight-line cleanup code and therefore fair. WorkerRespawn is fair
\* because dispatching backends actively call EnsureWorkerForMyDatabase in
\* their retry loops whenever the worker is down. The capacity waits need no
\* extra machinery: a waiting backend's acquire / enqueue action becomes
\* continuously enabled once the finitely many other sessions complete, and
\* WF then forces it -- the model's retry loop. The handshake poll (a parked
\* thread checking frame-or-cancel every iteration) is WF on HandshakeOk /
\* HandshakeCancelled inside WorkerProgress.
Fairness ==
    /\ \A b \in Backends : WF_vars(BackendProgress(b))
    /\ WF_vars(WorkerProgress)
    /\ WF_vars(WorkerRespawn)

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
(* --- Safety --- *)

\* The refcount never underflows (a DetachEnd without a matching AttachEnd
\* would wrap the C++ uint32 and strand the slot forever).
NoUnderflow == \A s \in Slots : att[s] >= 0

\* At most two ends ever attach: the backend's and the worker session's.
RefcountBounded == \A s \in Slots : att[s] =< 2

\* No double-free / no stale attach: a FREE slot has no attached ends. This is
\* what breaks when the worker attaches a stale pending entry for a slot its
\* enqueuer already released (ValidateGeneration = FALSE).
FreeImpliesUnattached ==
    \A s \in Slots : sstate[s] = "FREE" => (att[s] = 0 /\ wwait[s] = 0 /\ wheld[s] = 0)

\* One backend per slot: two live backends never hold the same slot.
Holding(b) == phase[b] \in {"acquired0","acquired","pending","aborting","served"}
SingleBackendPerSlot ==
    \A b1, b2 \in Backends :
        (Holding(b1) /\ Holding(b2) /\ bslot[b1] = bslot[b2]) => b1 = b2

----------------------------------------------------------------------------
(* --- Liveness --- *)

\* No slot leak: the pool always eventually returns (and stays) fully FREE.
\* This is what breaks when a pending abort skips the cancel-flag write
\* (AbortSetsCancel = FALSE): the parked handshake thread never detaches.
NoSlotLeak == <>[](\A s \in Slots : sstate[s] = "FREE")

\* No lost session, no starved wait: every backend -- including one that had
\* to wait for pool or ring capacity -- is eventually served to completion or
\* settles cleanly (abort / worker-death error).
SessionSettles == \A b \in Backends : <>(phase[b] = "settled")

Liveness == <>Quiescent
=============================================================================
