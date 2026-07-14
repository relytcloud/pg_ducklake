----------------------------- MODULE ScanInversion -----------------------------
(***************************************************************************)
(* A TLA+ model of the SCAN-INVERSION read-ahead sub-protocol of the       *)
(* shared DuckDB duckdb worker:                                            *)
(*   InversionScanStream (libpgduckdb/worker/scan_producer.cpp, worker/    *)
(*   consumer side) <-> BackendSession::ServiceScanFetch            *)
(*   (libpgduckdb/worker/duckdb_worker.cpp, backend/producer side) over the   *)
(*   session channel's two shm_mq FIFOs, with the worker-side demux of      *)
(*   SessionChannel::RoutedRecv (libpgduckdb/worker/transport/              *)
(*   session_channel.cpp).                                                  *)
(*                                                                          *)
(* Direction of the two queues (named from the duckdb worker's view):       *)
(*   result  : worker --ScanFetch/MetaQuery--> backend  (the result queue)  *)
(*   control : backend --ScanChunk*/ScanDone/ScanError/MetaResult--> worker *)
(*                                                                          *)
(* Every scan reply now carries a uint32 scan_id prefix (SendScanReply in   *)
(* duckdb_worker.cpp). The worker demultiplexes the control FIFO into per-    *)
(* scan LANES plus one META lane (RoutedRecv in session_channel.cpp): a     *)
(* receiving thread holds the demux lock across recv+route, so a lane's     *)
(* frame order matches wire order. This lets several scans (a join's two    *)
(* inputs on different DuckDB threads) and metadata round-trips share ONE   *)
(* channel CONCURRENTLY -- the property this model checks.                  *)
(*                                                                          *)
(* What is modelled, per scan_id:                                           *)
(*   - Worker Next() (InversionScanStream::Next): top up `outstanding` to  *)
(*     WINDOW ScanFetch sends (first carries count_only+SQL), then consume  *)
(*     ONE reply from THIS scan's lane (RecvScanReply), outstanding--. On   *)
(*     ScanChunkArrow a pool page rides along (acquired by the backend,     *)
(*     released when the worker drops the chunk). On ScanDone/ScanError set *)
(*     done_ and DrainOutstanding(). Early teardown (LIMIT/cancel) -> the   *)
(*     destructor DrainOutstanding() then CloseScanLane. The drain can BAIL *)
(*     with replies still outstanding (SerializedRecvControl/RoutedRecv     *)
(*     return Detached on backend cancel or worker drain), so CloseScanLane *)
(*     must cope with replies that arrive AFTER the close: it tombstones    *)
(*     the scan_id (closed_scans) and frees the pages of frames already     *)
(*     routed to the lane; RoutedRecv then DROPS a late reply for a         *)
(*     tombstoned scan (freeing its page) instead of re-creating ("resur-   *)
(*     recting") the lane and pinning the page forever. (~SessionChannel    *)
(*     also frees pages of frames still buffered in live lanes -- channel   *)
(*     teardown is below this model's granularity; the close covers it.)    *)
(*   - RouteFrame: RoutedRecv's route step -- pop the wire head, strip the  *)
(*     scan_id prefix, append to that scan's lane (or the meta lane).       *)
(*   - Backend ServiceScanFetch: per-scan_id BackendScanState in            *)
(*     open_scans_; on first fetch InitBackendScan; produce one chunk       *)
(*     (Arrow page or inline) or ScanDone at EOF, or ScanError; a scan that *)
(*     has finished/errored is KEPT (not erased) and replies the SAME       *)
(*     terminal to extra windowed fetches (the "kept, not re-opened"        *)
(*     invariant).                                                          *)
(*   - Metadata round-trip (optional): WorkerMetadataQuery/MetadataRoundTrip *)
(*     in session_protocol.cpp -- send one MetaQuery on the result queue    *)
(*     (serialized per channel by MetaRequestMutex, so at most one is in    *)
(*     flight), receive the MetaResult off the meta lane. Models a DuckLake *)
(*     GetFilesForTable issued from a scheduler thread mid-scan.            *)
(*                                                                          *)
(* Pages are modelled abstractly as a conserved resource: every Arrow page  *)
(* the backend hands out must eventually be released -- by the worker       *)
(* importing+dropping the chunk, by DrainOutstanding freeing a drained      *)
(* Arrow frame, or by CloseScanLane freeing a routed-but-unconsumed frame.  *)
(*                                                                          *)
(* CONSTANTS pick the adversary:                                            *)
(*   NScans       = scan_ids sharing the ONE channel (2 = a join, driven    *)
(*                  CONCURRENTLY -- no sequencing assumption).              *)
(*   W            = read-ahead window (the C++ WINDOW = 8; we use 2).       *)
(*   Chunks1/2, Error1/2 = per-scan chunk counts / error injection points.  *)
(*   NumPages     = pool pages available for the Arrow fast path.           *)
(*   AllowArrow   = TRUE: chunks may ride an Arrow page (resource).         *)
(*   AllowTeardown= TRUE: a scan may tear down early (destructor drain).    *)
(*   AllowMeta    = TRUE: one metadata round-trip may interleave.           *)
(*   RouteByScanId= TRUE (the real code): replies are routed to lanes by    *)
(*                  their scan_id prefix. FALSE (the pre-demux bug class):  *)
(*                  a receiving scan consumes the wire head no matter whom  *)
(*                  it was for -- concurrent scans then mis-route.          *)
(*   DropLateReplies = TRUE (the real code): a reply routed after           *)
(*                  CloseScanLane is dropped and its page freed (the        *)
(*                  closed_scans tombstone). FALSE (the pre-fix bug): the   *)
(*                  late reply resurrects the closed lane, and the page is  *)
(*                  pinned forever -- expect FAIL.                          *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    NScans,           \* number of scan_ids sharing the channel
    W,                \* read-ahead window (WINDOW), e.g. 2
    Chunks1,          \* data chunks scan 1 produces before ScanDone
    Chunks2,          \* data chunks scan 2 produces (ignored if NScans = 1)
    Error1,           \* chunk index at which scan 1 errors (0 = never)
    Error2,           \* chunk index at which scan 2 errors (0 = never)
    NumPages,         \* Arrow pool pages available
    AllowArrow,       \* TRUE: chunks may use an Arrow page (resource)
    AllowTeardown,    \* TRUE: a scan may tear down early
    AllowMeta,        \* TRUE: a metadata round-trip may share the channel
    RouteByScanId,    \* TRUE: the demux (real code); FALSE: consume wire head
    DropLateReplies   \* TRUE: tombstone closed scans (the fix); FALSE: resurrect

\* scan_ids are 1..NScans; per-scan chunk counts and error points are selected
\* from the flat scalar constants above (cfg files cannot hold function literals).
Scans      == 1 .. NScans
ChunksOf   == [s \in Scans |-> IF s = 1 THEN Chunks1 ELSE Chunks2]
ErrorsAt   == [s \in Scans |-> IF s = 1 THEN Error1  ELSE Error2]

(* Reply frames on the control FIFO. `sid` is the scan_id prefix the wire
   frame now CARRIES (0 = a metadata reply, which has no prefix but is routed
   to the meta lane by its tag). `page` = does this frame own an Arrow page. *)
ChunkF(sid, ix, pg)  == [kind |-> "chunk", sid |-> sid, ix |-> ix, page |-> pg]
DoneF(sid)           == [kind |-> "done",  sid |-> sid, ix |-> 0,  page |-> FALSE]
ErrF(sid)            == [kind |-> "error", sid |-> sid, ix |-> 0,  page |-> FALSE]
MetaF                == [kind |-> "meta",  sid |-> 0,   ix |-> 0,  page |-> FALSE]

(* Request frames on the result FIFO. *)
FetchF(sid) == [kind |-> "fetch", sid |-> sid]
MetaReqF    == [kind |-> "metaq", sid |-> 0]

VARIABLES
    result,     \* Seq of requests: worker --> backend (the result queue)
    control,    \* Seq of reply frames: backend --> worker (the control queue)
    lanes,      \* [Scans -> Seq of replies]: the per-scan demux lanes
    metaLane,   \* Seq of replies: the metadata demux lane
    bstate,     \* [Scans -> {"unopened","open","done","errored"}]: BackendScanState
    bnext,      \* [Scans -> Nat]: next data-chunk index the backend will produce
    started,    \* [Scans -> BOOLEAN]: worker has sent the first (SQL-carrying) fetch
    outstanding,\* [Scans -> Nat]: worker's outstanding ScanFetch count
    wstate,     \* [Scans -> {"run","done","torndown","closed"}]: worker stream
                \* state; "closed" = CloseScanLane ran (the tombstone)
    recvd,      \* [Scans -> Seq of Nat]: data-chunk indices the worker has consumed
    pagesOut,   \* number of Arrow pages handed out but not yet released
    mstate      \* {"idle","sent","done"}: the (single) metadata round-trip

vars == << result, control, lanes, metaLane, bstate, bnext, started,
           outstanding, wstate, recvd, pagesOut, mstate >>

----------------------------------------------------------------------------
ErrorsHere(sid)   == ErrorsAt[sid] # 0 /\ bnext[sid] = ErrorsAt[sid]
AtEOF(sid)        == bnext[sid] >= ChunksOf[sid]

\* A page may be attached only if the Arrow path is on and a page is free.
CanPage           == AllowArrow /\ pagesOut < NumPages

----------------------------------------------------------------------------
TypeOK ==
    /\ \A i \in DOMAIN result :
         /\ result[i].kind \in {"fetch","metaq"}
         /\ result[i].sid \in Scans \cup {0}
    /\ \A i \in DOMAIN control :
         /\ control[i].kind \in {"chunk","done","error","meta"}
         /\ control[i].sid \in Scans \cup {0}
         /\ control[i].ix \in Nat
         /\ control[i].page \in BOOLEAN
    /\ \A s \in Scans : \A i \in DOMAIN lanes[s] : lanes[s][i].kind \in {"chunk","done","error"}
    /\ \A i \in DOMAIN metaLane : metaLane[i].kind = "meta"
    /\ bstate \in [Scans -> {"unopened","open","done","errored"}]
    /\ bnext \in [Scans -> Nat]
    /\ started \in [Scans -> BOOLEAN]
    /\ outstanding \in [Scans -> Nat]
    /\ wstate \in [Scans -> {"run","done","torndown","closed"}]
    /\ \A s \in Scans : \A i \in DOMAIN recvd[s] : recvd[s][i] \in Nat
    /\ pagesOut \in 0 .. NumPages
    /\ mstate \in {"idle","sent","done"}

Init ==
    /\ result = << >>
    /\ control = << >>
    /\ lanes = [s \in Scans |-> << >>]
    /\ metaLane = << >>
    /\ bstate = [s \in Scans |-> "unopened"]
    /\ bnext = [s \in Scans |-> 0]
    /\ started = [s \in Scans |-> FALSE]
    /\ outstanding = [s \in Scans |-> 0]
    /\ wstate = [s \in Scans |-> "run"]
    /\ recvd = [s \in Scans |-> << >>]
    /\ pagesOut = 0
    /\ mstate = "idle"

----------------------------------------------------------------------------
(* --- Worker side: InversionScanStream::Next (scan_producer.cpp) --- *)

\* Top-up loop in Next(): `while (outstanding_ < WINDOW) SerializedSendResult
\* (ScanFetch)`. The first fetch flips started_ (carries count_only + SQL).
\* One send per step. NO cross-scan gate: scans interleave freely -- the demux
\* makes that safe.
WorkerSendFetch(s) ==
    /\ wstate[s] = "run"
    /\ outstanding[s] < W
    /\ result' = Append(result, FetchF(s))
    /\ outstanding' = [outstanding EXCEPT ![s] = outstanding[s] + 1]
    /\ started' = [started EXCEPT ![s] = TRUE]
    /\ UNCHANGED << control, lanes, metaLane, bstate, bnext, wstate, recvd, pagesOut, mstate >>

\* RoutedRecv's ROUTE step (session_channel.cpp): pop the wire head under the
\* demux lock and append it to the lane its scan_id prefix selects (meta
\* replies go to the meta lane). Holding the lock across recv+route means a
\* lane's order equals wire order -- modelled by routing only the wire HEAD.
\* A LATE reply -- routed after CloseScanLane tombstoned its scan_id -- is
\* DROPPED and its Arrow page freed (the closed_scans check, the fix);
\* DropLateReplies = FALSE reverts to the pre-fix resurrection: the frame is
\* appended to the closed lane, where nothing will ever consume it.
RouteFrame ==
    /\ RouteByScanId
    /\ control # << >>
    /\ LET f == Head(control) IN
         /\ control' = Tail(control)
         /\ IF f.kind = "meta"
              THEN /\ metaLane' = Append(metaLane, f)
                   /\ lanes' = lanes
                   /\ pagesOut' = pagesOut
              ELSE IF wstate[f.sid] = "closed" /\ DropLateReplies
              THEN /\ lanes' = lanes
                   /\ metaLane' = metaLane
                   /\ pagesOut' = pagesOut - (IF f.page THEN 1 ELSE 0)
              ELSE /\ lanes' = [lanes EXCEPT ![f.sid] = Append(lanes[f.sid], f)]
                   /\ metaLane' = metaLane
                   /\ pagesOut' = pagesOut
    /\ UNCHANGED << result, bstate, bnext, started, outstanding, wstate, recvd, mstate >>

\* RecvScanReply in Next(): consume the head of THIS scan's lane.
WorkerRecvLane(s) ==
    /\ RouteByScanId
    /\ wstate[s] = "run"
    /\ outstanding[s] > 0
    /\ lanes[s] # << >>
    /\ LET f == Head(lanes[s]) IN
         /\ lanes' = [lanes EXCEPT ![s] = Tail(lanes[s])]
         /\ outstanding' = [outstanding EXCEPT ![s] = outstanding[s] - 1]
         /\ IF f.kind = "chunk"
              THEN \* import the chunk; if it carried a page it is released on drop
                   /\ recvd' = [recvd EXCEPT ![s] = Append(recvd[s], f.ix)]
                   /\ pagesOut' = pagesOut - (IF f.page THEN 1 ELSE 0)
                   /\ UNCHANGED << wstate >>
              ELSE \* ScanDone / ScanError: set done_, then DrainOutstanding()
                   /\ wstate' = [wstate EXCEPT ![s] = "done"]
                   /\ recvd' = recvd
                   /\ pagesOut' = pagesOut
    /\ UNCHANGED << result, control, metaLane, bstate, bnext, started, mstate >>

\* DrainOutstanding(): consume one leftover reply off this scan's lane,
\* freeing its Arrow page. Reached after done_ (terminal seen) or teardown.
WorkerDrainLane(s) ==
    /\ RouteByScanId
    /\ wstate[s] \in {"done","torndown"}
    /\ outstanding[s] > 0
    /\ lanes[s] # << >>
    /\ LET f == Head(lanes[s]) IN
         /\ lanes' = [lanes EXCEPT ![s] = Tail(lanes[s])]
         /\ outstanding' = [outstanding EXCEPT ![s] = outstanding[s] - 1]
         /\ pagesOut' = pagesOut - (IF f.page THEN 1 ELSE 0)
    /\ UNCHANGED << result, control, metaLane, bstate, bnext, started, wstate, recvd, mstate >>

\* CloseScanLane in ~InversionScanStream: tombstone the scan_id (closed_scans),
\* drop the lane, and free the Arrow pages of any frame routed there but never
\* consumed. Enabled with outstanding > 0 because the destructor's drain can
\* BAIL (Detached on backend cancel / worker drain) and close anyway -- the
\* replies for those fetches then arrive LATE, after the tombstone, and
\* RouteFrame must drop them. The stream is gone afterwards: no more drain.
WorkerCloseLane(s) ==
    /\ RouteByScanId
    /\ wstate[s] \in {"done","torndown"}
    /\ wstate' = [wstate EXCEPT ![s] = "closed"]
    /\ pagesOut' = pagesOut -
         Cardinality({ i \in DOMAIN lanes[s] : lanes[s][i].page })
    /\ lanes' = [lanes EXCEPT ![s] = << >>]
    /\ UNCHANGED << result, control, metaLane, bstate, bnext, started, outstanding, recvd, mstate >>

\* --- Pre-demux bug class (RouteByScanId = FALSE): the worker consumes the ---
\* --- wire HEAD and attributes it to ITSELF, whoever it was intended for.  ---
WorkerRecvWire(s) ==
    /\ ~RouteByScanId
    /\ wstate[s] = "run"
    /\ outstanding[s] > 0
    /\ control # << >>
    /\ LET f == Head(control) IN
         /\ control' = Tail(control)
         /\ outstanding' = [outstanding EXCEPT ![s] = outstanding[s] - 1]
         /\ IF f.kind = "chunk"
              THEN /\ recvd' = [recvd EXCEPT ![s] = Append(recvd[s], f.ix)]
                   /\ pagesOut' = pagesOut - (IF f.page THEN 1 ELSE 0)
                   /\ UNCHANGED << wstate >>
              ELSE /\ wstate' = [wstate EXCEPT ![s] = "done"]
                   /\ recvd' = recvd
                   /\ pagesOut' = pagesOut
    /\ UNCHANGED << result, lanes, metaLane, bstate, bnext, started, mstate >>

WorkerDrainWire(s) ==
    /\ ~RouteByScanId
    /\ wstate[s] \in {"done","torndown"}
    /\ outstanding[s] > 0
    /\ control # << >>
    /\ LET f == Head(control) IN
         /\ control' = Tail(control)
         /\ outstanding' = [outstanding EXCEPT ![s] = outstanding[s] - 1]
         /\ pagesOut' = pagesOut - (IF f.page THEN 1 ELSE 0)
    /\ UNCHANGED << result, lanes, metaLane, bstate, bnext, started, wstate, recvd, mstate >>

\* ~InversionScanStream(): early teardown (LIMIT/cancel) while !done_.
WorkerTeardown(s) ==
    /\ AllowTeardown
    /\ wstate[s] = "run"
    /\ wstate' = [wstate EXCEPT ![s] = "torndown"]
    /\ UNCHANGED << result, control, lanes, metaLane, bstate, bnext, started, outstanding, recvd, pagesOut, mstate >>

----------------------------------------------------------------------------
(* --- Metadata round-trip (MetadataRoundTrip in session_protocol.cpp) --- *)
(* One at a time per channel (MetaRequestMutex); may fire mid-scan from a    *)
(* DuckDB scheduler thread (e.g. DuckLake GetFilesForTable).                 *)

MetaSend ==
    /\ AllowMeta
    /\ mstate = "idle"
    /\ result' = Append(result, MetaReqF)
    /\ mstate' = "sent"
    /\ UNCHANGED << control, lanes, metaLane, bstate, bnext, started, outstanding, wstate, recvd, pagesOut >>

\* RecvMetaReply: consume the head of the meta lane.
MetaRecv ==
    /\ mstate = "sent"
    /\ metaLane # << >>
    /\ metaLane' = Tail(metaLane)
    /\ mstate' = "done"
    /\ UNCHANGED << result, control, lanes, bstate, bnext, started, outstanding, wstate, recvd, pagesOut >>

----------------------------------------------------------------------------
(* --- Backend side: BackendSession::Fetch loop (duckdb_worker.cpp) --- *)
(* The Fetch loop dequeues frames from the result FIFO IN ORDER; a ScanFetch  *)
(* goes to ServiceScanFetch (one SendScanReply per fetch, scan_id-prefixed),  *)
(* a MetaQuery to the extension's serve_frame (one MetaResult).               *)

BackendService ==
    /\ result # << >>
    /\ LET f == Head(result) IN
       IF f.kind = "metaq"
       THEN \* ServeDuckLakeFrame -> SendMetadataReply
            /\ result' = Tail(result)
            /\ control' = Append(control, MetaF)
            /\ UNCHANGED << bstate, bnext, pagesOut >>
       ELSE LET sid == f.sid IN
            /\ result' = Tail(result)
            /\ CASE
                 \* Kept-and-errored: reply the SAME ScanError (no re-run).
                 bstate[sid] = "errored" ->
                   /\ control' = Append(control, ErrF(sid))
                   /\ UNCHANGED << bstate, bnext, pagesOut >>
                 \* Kept-and-done: reply the SAME ScanDone (no re-open).
                 [] bstate[sid] = "done" ->
                   /\ control' = Append(control, DoneF(sid))
                   /\ UNCHANGED << bstate, bnext, pagesOut >>
                 \* Unopened/open: open on first fetch, produce chunk/terminal.
                 [] OTHER ->
                   /\ IF ErrorsHere(sid)
                        THEN /\ bstate' = [bstate EXCEPT ![sid] = "errored"]
                             /\ control' = Append(control, ErrF(sid))
                             /\ UNCHANGED << bnext, pagesOut >>
                        ELSE IF AtEOF(sid)
                          THEN /\ bstate' = [bstate EXCEPT ![sid] = "done"]
                               /\ control' = Append(control, DoneF(sid))
                               /\ UNCHANGED << bnext, pagesOut >>
                          ELSE /\ LET usePage == CanPage IN
                                    /\ control' = Append(control, ChunkF(sid, bnext[sid], usePage))
                                    /\ pagesOut' = pagesOut + (IF usePage THEN 1 ELSE 0)
                               /\ bstate' = [bstate EXCEPT ![sid] = "open"]
                               /\ bnext' = [bnext EXCEPT ![sid] = bnext[sid] + 1]
    /\ UNCHANGED << lanes, metaLane, started, outstanding, wstate, recvd, mstate >>

----------------------------------------------------------------------------
(* --- Terminal / stutter --- *)

\* A scan is retired once its lane is closed (the destructor always runs). In
\* the no-demux bug variant there are no lanes to close: terminal wstate is
\* enough. `outstanding` is NOT required to be 0: a bailed drain leaves the
\* counter behind; the tombstone (not the counter) retires those replies.
Retired(s) == IF RouteByScanId THEN wstate[s] = "closed"
                               ELSE wstate[s] \in {"done","torndown"}

AllRetired ==
    /\ \A s \in Scans : Retired(s)
    /\ \A s \in Scans : lanes[s] = << >>
    /\ result = << >>
    /\ control = << >>
    /\ metaLane = << >>
    /\ mstate # "sent"

\* Clean terminal: all scans retired and every Arrow page reclaimed.
Quiescent ==
    /\ AllRetired
    /\ pagesOut = 0

Terminating == Quiescent /\ UNCHANGED vars

ProgressNext ==
    \/ \E s \in Scans : WorkerSendFetch(s) \/ WorkerRecvLane(s) \/ WorkerDrainLane(s)
                        \/ WorkerCloseLane(s) \/ WorkerRecvWire(s) \/ WorkerDrainWire(s)
    \/ RouteFrame
    \/ MetaRecv
    \/ BackendService

Next ==
    \/ ProgressNext
    \/ \E s \in Scans : WorkerTeardown(s)   \* adversarial: not forced
    \/ MetaSend                             \* adversarial: not forced
    \/ Terminating

\* Fairness on progress actions only. WorkerTeardown and MetaSend are
\* adversarial (never forced): the protocol must terminate either way.
Fairness ==
    /\ \A s \in Scans :
         /\ WF_vars(WorkerSendFetch(s))
         /\ WF_vars(WorkerRecvLane(s))
         /\ WF_vars(WorkerDrainLane(s))
         /\ WF_vars(WorkerCloseLane(s))
         /\ WF_vars(WorkerRecvWire(s))
         /\ WF_vars(WorkerDrainWire(s))
    /\ WF_vars(RouteFrame)
    /\ WF_vars(MetaRecv)
    /\ WF_vars(BackendService)

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
(* --- Safety --- *)

\* WindowBounded: at most W ScanFetches outstanding per scan.
WindowBounded == \A s \in Scans : outstanding[s] =< W

\* RequestsBounded: the result FIFO holds at most W per scan + 1 meta request.
RequestsBounded == Len(result) =< W * Cardinality(Scans) + 1

\* PageConservation: pages handed out but not released stay within the pool.
PageConservation == pagesOut \in 0 .. NumPages

\* FIFOMatching: the data chunks a scan has consumed are exactly the prefix
\* 0,1,2,... of its produced sequence -- no reorder, no loss, no duplication,
\* no foreign chunk. This is what the scan_id demux guarantees for CONCURRENT
\* scans, and what breaks when RouteByScanId = FALSE.
PrefixOk(s) == \A i \in DOMAIN recvd[s] : recvd[s][i] = i - 1
FIFOMatching == \A s \in Scans : PrefixOk(s)

\* MetaNotOnScanLane: routing never puts a metadata reply on a scan lane or a
\* scan reply on the meta lane (checked structurally via TypeOK's lane types).
MetaLaneClean == \A i \in DOMAIN metaLane : metaLane[i].kind = "meta"

\* ClosedLaneEmpty: a closed lane stays empty -- a late reply must never
\* resurrect it (its page would be pinned forever: nothing consumes a closed
\* lane). This is what the closed_scans tombstone in RoutedRecv guarantees
\* and what DropLateReplies = FALSE breaks.
ClosedLaneEmpty == \A s \in Scans : (wstate[s] = "closed") => lanes[s] = << >>

----------------------------------------------------------------------------
(* --- Terminal-stability safety (TerminalStable) --- *)
\* Once the backend marks a scan done/errored it never produces a fresh data
\* chunk for it again ("kept, not re-opened", ServiceScanFetch).
TerminalStableInv ==
    \A s \in Scans :
        /\ (bstate[s] = "done")    => (bnext[s] >= ChunksOf[s] \/ bnext[s] = 0)
        /\ (bstate[s] = "errored") => (ErrorsAt[s] = 0 \/ bnext[s] = ErrorsAt[s])

TerminalStable ==
    [][ \A s \in Scans :
          (bstate[s] \in {"done","errored"}) =>
            (bnext'[s] = bnext[s] /\ bstate'[s] = bstate[s]) ]_vars

----------------------------------------------------------------------------
(* --- Liveness --- *)

\* Termination: the protocol always reaches the clean terminal -- every scan
\* retired (lane closed), both FIFOs drained, lanes empty, every page
\* reclaimed, no metadata round-trip stuck in flight. Fails when a late reply
\* resurrects a closed lane (its page is never freed).
Termination == <>Quiescent
=============================================================================
