----------------------------- MODULE SharedArena -----------------------------
(***************************************************************************)
(* One page arena shared by every session of a worker.  A session thread   *)
(* takes a page to write a result chunk, the backend consumes the chunk    *)
(* and frees the page.  Every admitted session owns one reserved page; the *)
(* rest of the arena is a shared overflow, so the arena                    *)
(* arena bounds memory and throttles sessions that outrun their backends   *)
(* without letting one stalled backend starve the others.                  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
  Sessions,          \* sessions served by one worker
  ReservedPages,     \* pages reserved for each session
  SharedPages,       \* overflow pages beyond the reserved ones
  Chunks,            \* chunks each session produces
  FairBackends,      \* backends that keep consuming; the others may stall forever
  \* Bug switches.  TRUE is the proposed behaviour.
  ReservePerSession, \* a session's reserved pages are never lent to another session
  FreeBeforeWait     \* the backend frees a consumed page before waiting for the next chunk

VARIABLES
  tstate,     \* session thread: running | waitpage | done
  left,       \* chunks still to produce
  inflight,   \* chunks written and not yet consumed
  privFree,   \* free reserved pages of the session (0 without reservation)
  privHeld,   \* reserved pages the session holds
  pool,       \* free overflow pages
  sharedHeld, \* overflow pages the session holds
  bstate,     \* backend: pull | consuming | finished | detached
  bheld       \* the backend sits on a consumed page it has not freed yet

vars == <<tstate, left, inflight, privFree, privHeld, pool, sharedHeld, bstate, bheld>>

TotalPages == Cardinality(Sessions) * ReservedPages + SharedPages

TypeOK ==
  /\ tstate \in [Sessions -> {"running", "waitpage", "done"}]
  /\ left \in [Sessions -> 0..Chunks]
  /\ inflight \in [Sessions -> 0..Chunks]
  /\ privFree \in [Sessions -> 0..ReservedPages]
  /\ privHeld \in [Sessions -> 0..ReservedPages]
  /\ pool \in 0..TotalPages
  /\ sharedHeld \in [Sessions -> 0..TotalPages]
  /\ bstate \in [Sessions -> {"pull", "consuming", "finished", "detached"}]
  /\ bheld \in [Sessions -> BOOLEAN]

Init ==
  /\ tstate = [s \in Sessions |-> "running"]
  /\ left = [s \in Sessions |-> Chunks]
  /\ inflight = [s \in Sessions |-> 0]
  /\ privFree = [s \in Sessions |-> IF ReservePerSession THEN ReservedPages ELSE 0]
  /\ privHeld = [s \in Sessions |-> 0]
  /\ pool = IF ReservePerSession THEN SharedPages ELSE TotalPages
  /\ sharedHeld = [s \in Sessions |-> 0]
  /\ bstate = [s \in Sessions |-> "pull"]
  /\ bheld = [s \in Sessions |-> FALSE]

CanTake(s) == privFree[s] > 0 \/ pool > 0

-----------------------------------------------------------------------------
(* Session thread *)

\* Take a page (a reserved one first), write the chunk, queue its reference.
Produce(s) ==
  /\ tstate[s] = "running" /\ left[s] > 0 /\ CanTake(s)
  /\ IF privFree[s] > 0
       THEN /\ privFree' = [privFree EXCEPT ![s] = @ - 1]
            /\ privHeld' = [privHeld EXCEPT ![s] = @ + 1]
            /\ UNCHANGED <<pool, sharedHeld>>
       ELSE /\ pool' = pool - 1
            /\ sharedHeld' = [sharedHeld EXCEPT ![s] = @ + 1]
            /\ UNCHANGED <<privFree, privHeld>>
  /\ left' = [left EXCEPT ![s] = @ - 1]
  /\ inflight' = [inflight EXCEPT ![s] = @ + 1]
  /\ UNCHANGED <<tstate, bstate, bheld>>

Block(s) ==
  /\ tstate[s] = "running" /\ left[s] > 0 /\ ~CanTake(s)
  /\ tstate' = [tstate EXCEPT ![s] = "waitpage"]
  /\ UNCHANGED <<left, inflight, privFree, privHeld, pool, sharedHeld, bstate, bheld>>

Unblock(s) ==
  /\ tstate[s] = "waitpage" /\ CanTake(s)
  /\ tstate' = [tstate EXCEPT ![s] = "running"]
  /\ UNCHANGED <<left, inflight, privFree, privHeld, pool, sharedHeld, bstate, bheld>>

Finish(s) ==
  /\ tstate[s] = "running" /\ left[s] = 0
  /\ tstate' = [tstate EXCEPT ![s] = "done"]
  /\ UNCHANGED <<left, inflight, privFree, privHeld, pool, sharedHeld, bstate, bheld>>

-----------------------------------------------------------------------------
(* Backend *)

\* Give one page back: overflow first, so the reserved page stays with the session.
ReturnPage(s) ==
  IF sharedHeld[s] > 0
    THEN /\ pool' = pool + 1
         /\ sharedHeld' = [sharedHeld EXCEPT ![s] = @ - 1]
         /\ UNCHANGED <<privFree, privHeld>>
    ELSE /\ privFree' = [privFree EXCEPT ![s] = @ + 1]
         /\ privHeld' = [privHeld EXCEPT ![s] = @ - 1]
         /\ UNCHANGED <<pool, sharedHeld>>

\* The next chunk arrived.  With FreeBeforeWait = FALSE the page of the
\* previous chunk is only given back now.
Pull(s) ==
  /\ bstate[s] = "pull" /\ inflight[s] > 0
  /\ inflight' = [inflight EXCEPT ![s] = @ - 1]
  /\ bstate' = [bstate EXCEPT ![s] = "consuming"]
  /\ IF bheld[s] THEN ReturnPage(s) ELSE UNCHANGED <<privFree, privHeld, pool, sharedHeld>>
  /\ bheld' = [bheld EXCEPT ![s] = TRUE]
  /\ UNCHANGED <<tstate, left>>

\* The chunk is converted; with FreeBeforeWait the page goes back right away.
Done(s) ==
  /\ bstate[s] = "consuming"
  /\ bstate' = [bstate EXCEPT ![s] = "pull"]
  /\ IF FreeBeforeWait
       THEN ReturnPage(s) /\ bheld' = [bheld EXCEPT ![s] = FALSE]
       ELSE UNCHANGED <<privFree, privHeld, pool, sharedHeld, bheld>>
  /\ UNCHANGED <<tstate, left, inflight>>

BackendFinish(s) ==
  /\ bstate[s] = "pull" /\ tstate[s] = "done" /\ inflight[s] = 0
  /\ IF bheld[s] THEN ReturnPage(s) ELSE UNCHANGED <<privFree, privHeld, pool, sharedHeld>>
  /\ bheld' = [bheld EXCEPT ![s] = FALSE]
  /\ bstate' = [bstate EXCEPT ![s] = "detached"]
  /\ UNCHANGED <<tstate, left, inflight>>

\* Abort: the worker reclaims every page the session held.
Abort(s) ==
  /\ bstate[s] \in {"pull", "consuming"}
  /\ pool' = pool + sharedHeld[s]
  /\ sharedHeld' = [sharedHeld EXCEPT ![s] = 0]
  /\ privFree' = [privFree EXCEPT ![s] = IF ReservePerSession THEN ReservedPages ELSE 0]
  /\ privHeld' = [privHeld EXCEPT ![s] = 0]
  /\ inflight' = [inflight EXCEPT ![s] = 0]
  /\ bheld' = [bheld EXCEPT ![s] = FALSE]
  /\ bstate' = [bstate EXCEPT ![s] = "detached"]
  /\ tstate' = [tstate EXCEPT ![s] = "done"]
  /\ left' = [left EXCEPT ![s] = 0]

-----------------------------------------------------------------------------

AllDone == \A s \in Sessions : tstate[s] = "done" /\ bstate[s] = "detached"
Terminating == AllDone /\ UNCHANGED vars

Next ==
  \/ \E s \in Sessions :
       \/ Produce(s) \/ Block(s) \/ Unblock(s) \/ Finish(s)
       \/ Pull(s) \/ Done(s) \/ BackendFinish(s) \/ Abort(s)
  \/ Terminating

Fairness ==
  /\ \A s \in Sessions :
       WF_vars(Produce(s)) /\ WF_vars(Block(s)) /\ WF_vars(Unblock(s)) /\ WF_vars(Finish(s))
  /\ \A s \in FairBackends :
       WF_vars(Pull(s)) /\ WF_vars(Done(s)) /\ WF_vars(BackendFinish(s))

Spec == Init /\ [][Next]_vars /\ Fairness

(* Safety *)

Held(s) == privHeld[s] + sharedHeld[s]

\* Every page is free or held by exactly one session.
RECURSIVE Sum(_, _)
Sum(f, set) == IF set = {} THEN 0 ELSE LET x == CHOOSE y \in set : TRUE IN f[x] + Sum(f, set \ {x})

PageConservation ==
  pool + Sum(privFree, Sessions) + Sum(privHeld, Sessions) + Sum(sharedHeld, Sessions) = TotalPages


(* Liveness *)

\* A session whose backend keeps consuming always completes, whatever the
\* other backends do and however many pages they sit on.
FairSessionsComplete ==
  \A s \in FairBackends : <>(tstate[s] = "done" /\ bstate[s] = "detached")

=============================================================================
