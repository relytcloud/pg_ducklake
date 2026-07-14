#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace pgddb {

// Wire frame kinds. The control queue carries Sql/Snapshot (backend -> worker) and the
// backend's answers to the worker's requests; the result queue carries Chunk/Complete/
// Error and the worker's requests back to the backend (MetaQuery, ScanFetch).
enum class FrameTag : uint8_t {
	Sql = 1,
	Chunk = 3,
	Complete = 4,
	Error = 5,
	Snapshot = 6,           // control: the requesting backend's serialized MVCC snapshot
	MetaQuery = 7,          // result-queue: worker asks the backend to run a metadata SQL
	MetaResult = 8,         // control: backend's serialized result for a MetaQuery
	MetaError = 9,          // control: backend's error message for a MetaQuery
	ScanFetch = 10,         // result-queue: worker asks the backend for the next chunk of a heap scan
	ScanChunk = 11,         // control: backend's next serialized DataChunk for a heap scan (inline bytes)
	ScanDone = 12,          // control: heap scan exhausted
	ScanError = 13,         // control: backend error while producing a heap scan
	ScanChunkArrow = 15,    // control: next chunk is an Arrow batch in a global pool page; payload is the descriptor
	DescribeRel = 16,       // result-queue: worker asks the backend to describe schema.table
	DescribeRelResult = 17, // control: backend's serialized RelationDesc
	DescribeRelError = 18,  // control: backend error (e.g. relation not found)
	MetaExec = 19,          // result-queue: worker asks the backend to run a metadata write (subtransaction)
};

enum class FrameResult { Ok, WouldBlock, Detached };

// The shared-memory transport for one duckdb query: a small header (a cooperative
// cancel flag) plus two shm_mq rings -- a control queue (backend -> worker) and a
// result queue (worker -> backend) -- laid out in a fixed session-slot region (see
// SessionPool). The backend opens the slot (creates the rings) and the worker
// attaches to the same region. The rings are recreated per query, so a slot is reused
// across queries without allocating shared memory. PostgreSQL/IPC types stay in the impl.
class SessionChannel {
public:
	~SessionChannel();

	// Bytes a slot's channel region needs (header + the two rings).
	static std::size_t ChannelRegionBytes();

	// Backend side: (re)create the rings in the slot region `base` and bind this
	// process as control-sender / result-receiver. Returns nullptr on failure.
	static std::unique_ptr<SessionChannel> OpenSlot(char *base);
	// Worker side: attach to the rings already created at `base`, as control-receiver /
	// result-sender. Returns nullptr on failure.
	static std::unique_ptr<SessionChannel> AttachSlot(char *base);

	// Control queue: backend sends, worker receives. Send blocks (backpressure)
	// until space is available or the peer detaches.
	bool SendControl(FrameTag tag, const char *data, std::size_t len);
	FrameResult RecvControl(FrameTag *tag, const char **data, std::size_t *len, bool nowait);

	// Result queue: worker sends, backend receives.
	FrameResult RecvResult(FrameTag *tag, const char **data, std::size_t *len, bool nowait);

	// Multi-threaded duckdb-worker transport: serialize each poke under the global
	// process lock, dropping it between non-blocking attempts so a session thread never
	// blocks holding it (MyLatch stays owned by the worker main loop). Return false /
	// Detached on peer detach or cancel.
	bool SerializedSendResult(FrameTag tag, const char *data, std::size_t len);
	// Raw serialized receive; only safe for the session handshake (Snapshot + Sql),
	// before any concurrent requesters exist -- afterwards use the routed receives.
	FrameResult SerializedRecvControl(FrameTag *tag, const char **data, std::size_t *len);

	// Routed worker-side receives: several threads of one session (scan streams on
	// DuckDB scheduler threads, metadata RPCs) share the control queue, so incoming
	// frames are demultiplexed into lanes -- per-scan lanes keyed by the scan_id prefix
	// of scan replies, and one metadata lane (Meta*/DescribeRel* replies). Each call
	// returns the next frame of its lane (payload copied out), polling the queue and
	// routing stray frames to their lanes; Detached on peer detach, cancel, or drain.
	FrameResult RecvMetaReply(FrameTag *tag, std::string *payload);
	FrameResult RecvScanReply(uint32_t scan_id, FrameTag *tag, std::string *payload);
	// Serialize whole metadata round-trips: replies carry no request id, so only one
	// metadata request may be in flight per channel.
	std::mutex &MetaRequestMutex();
	// Drop a scan's lane, handing any still-buffered frames to `on_frame` (so the
	// caller can free Arrow pages a drained frame owns).
	void CloseScanLane(uint32_t scan_id, const std::function<void(FrameTag, const std::string &)> &on_frame);

	// Cooperative cancellation: backend sets, worker polls.
	void RequestCancel();
	bool IsCancelRequested() const;
	// Set the cancel flag directly in a slot region (no channel object needed): the
	// backend's abort path uses it when a longjmp skipped the channel destructor.
	static void RequestCancelAt(char *base);

private:
	struct State;
	std::unique_ptr<State> state_;
	SessionChannel();
	FrameResult RoutedRecv(bool meta, uint32_t scan_id, FrameTag *tag, std::string *payload);
};

// The session the calling worker thread is executing, or nullptr (thread-local; each
// session runs on its own thread). Set around each query so PG-touching code that runs
// on the session thread (binding, the DuckLake metadata manager) can route its queries
// back to the requesting backend instead of doing local SPI. Scheduler threads resolve
// their session via EffectiveWorkerSession (duckdb_worker.hpp) instead.
void SetCurrentWorkerSession(SessionChannel *channel);
SessionChannel *CurrentWorkerSession();

// Signal the multi-threaded worker's session-thread transport polls to bail (shutdown).
void SetWorkerDraining(bool draining);
bool IsWorkerDraining();

} // namespace pgddb
