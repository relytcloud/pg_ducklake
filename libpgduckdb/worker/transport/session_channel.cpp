#include "pgddb/worker/transport/session_channel.hpp"

#include <atomic>
#include <cstring>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "pgddb/pgddb_process_lock.hpp"
#include "pgddb/worker/transport/arrow_codec.hpp"
#include "pgddb/worker/transport/page_pool.hpp"

extern "C" {
#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"
}

namespace pgddb {

namespace {

std::atomic<bool> g_worker_draining {false};

constexpr Size CONTROL_QUEUE_SIZE = 64 * 1024;
constexpr Size RESULT_QUEUE_SIZE = 256 * 1024;

struct SessionChannelHeader {
	pg_atomic_uint32 cancel;
};

inline Size
HeaderSize() {
	return MAXALIGN(sizeof(SessionChannelHeader));
}

shm_mq_result
SendFrame(shm_mq_handle *mqh, FrameTag tag, const char *data, std::size_t len, bool nowait) {
	uint8 tagbyte = (uint8)tag;
	shm_mq_iovec iov[2];
	iov[0].data = (const char *)&tagbyte;
	iov[0].len = 1;
	iov[1].data = data;
	iov[1].len = len;
	int iovcnt = (len > 0) ? 2 : 1;
	return shm_mq_sendv(mqh, iov, iovcnt, nowait, true);
}

FrameResult
RecvFrame(shm_mq_handle *mqh, FrameTag *tag, const char **data, std::size_t *len, bool nowait) {
	Size nbytes = 0;
	void *d = nullptr;
	shm_mq_result r = shm_mq_receive(mqh, &nbytes, &d, nowait);
	if (r == SHM_MQ_SUCCESS) {
		*tag = (FrameTag)(*(uint8 *)d);
		*data = (const char *)d + 1;
		*len = (std::size_t)(nbytes - 1);
		return FrameResult::Ok;
	}
	if (r == SHM_MQ_WOULD_BLOCK)
		return FrameResult::WouldBlock;
	return FrameResult::Detached;
}

} // namespace

struct RoutedFrame {
	FrameTag tag;
	std::string payload;
};

struct SessionChannel::State {
	SessionChannelHeader *hdr = nullptr;
	shm_mq_handle *control = nullptr;
	shm_mq_handle *result = nullptr;
	// Worker-side demux of the control queue: per-scan lanes (keyed by the scan_id
	// prefix of scan replies) + one metadata lane. Guarded by demux_lock, which is held
	// across recv+route so a lane's frame order matches the wire order.
	std::mutex demux_lock = {};
	std::unordered_map<uint32_t, std::deque<RoutedFrame>> scan_lanes = {};
	// Scans whose lane was closed: late replies are dropped (their Arrow page freed)
	// instead of resurrecting the lane and pinning pages forever.
	std::unordered_set<uint32_t> closed_scans = {};
	std::deque<RoutedFrame> meta_lane = {};
	std::mutex meta_request_lock = {};
};

namespace {

// Free the pool page a buffered-but-never-consumed ScanChunkArrow frame owns.
void
ReleaseArrowFramePage(FrameTag tag, const char *payload, std::size_t len) {
	if (tag != FrameTag::ScanChunkArrow || len < sizeof(ArrowBatchHeader) || !PagePool().Available())
		return;
	ArrowBatchHeader hdr;
	std::memcpy(&hdr, payload, sizeof(hdr));
	PagePool().Release(PagePool().Slot((int)hdr.page_index));
}

} // namespace

SessionChannel::SessionChannel() : state_(std::make_unique<State>()) {
}

SessionChannel::~SessionChannel() {
	// Frames still buffered in scan lanes may own pool pages; free them.
	for (auto &lane : state_->scan_lanes) {
		for (auto &frame : lane.second)
			ReleaseArrowFramePage(frame.tag, frame.payload.data(), frame.payload.size());
	}
	if (state_->control)
		shm_mq_detach(state_->control);
	if (state_->result)
		shm_mq_detach(state_->result);
}

std::size_t
SessionChannel::ChannelRegionBytes() {
	return HeaderSize() + CONTROL_QUEUE_SIZE + RESULT_QUEUE_SIZE;
}

std::unique_ptr<SessionChannel>
SessionChannel::OpenSlot(char *base) {
	auto *hdr = (SessionChannelHeader *)base;
	pg_atomic_init_u32(&hdr->cancel, 0);

	char *control_addr = base + HeaderSize();
	char *result_addr = control_addr + CONTROL_QUEUE_SIZE;
	// Recreate the rings in place so the slot is reusable across queries (no dsm).
	shm_mq *control_mq = shm_mq_create(control_addr, CONTROL_QUEUE_SIZE);
	shm_mq *result_mq = shm_mq_create(result_addr, RESULT_QUEUE_SIZE);
	shm_mq_set_sender(control_mq, MyProc);
	shm_mq_set_receiver(result_mq, MyProc);

	std::unique_ptr<SessionChannel> ch(new SessionChannel());
	ch->state_->hdr = hdr;
	ch->state_->control = shm_mq_attach(control_mq, NULL, NULL);
	ch->state_->result = shm_mq_attach(result_mq, NULL, NULL);
	return ch;
}

std::unique_ptr<SessionChannel>
SessionChannel::AttachSlot(char *base) {
	auto *hdr = (SessionChannelHeader *)base;
	char *control_addr = base + HeaderSize();
	char *result_addr = control_addr + CONTROL_QUEUE_SIZE;
	shm_mq *control_mq = (shm_mq *)control_addr;
	shm_mq *result_mq = (shm_mq *)result_addr;
	shm_mq_set_receiver(control_mq, MyProc);
	shm_mq_set_sender(result_mq, MyProc);

	std::unique_ptr<SessionChannel> ch(new SessionChannel());
	ch->state_->hdr = hdr;
	ch->state_->control = shm_mq_attach(control_mq, NULL, NULL);
	ch->state_->result = shm_mq_attach(result_mq, NULL, NULL);
	return ch;
}

bool
SessionChannel::SendControl(FrameTag tag, const char *data, std::size_t len) {
	return SendFrame(state_->control, tag, data, len, false) == SHM_MQ_SUCCESS;
}

FrameResult
SessionChannel::RecvControl(FrameTag *tag, const char **data, std::size_t *len, bool nowait) {
	return RecvFrame(state_->control, tag, data, len, nowait);
}

FrameResult
SessionChannel::RecvResult(FrameTag *tag, const char **data, std::size_t *len, bool nowait) {
	return RecvFrame(state_->result, tag, data, len, nowait);
}

void
SessionChannel::RequestCancel() {
	pg_atomic_write_u32(&state_->hdr->cancel, 1);
}

bool
SessionChannel::IsCancelRequested() const {
	return pg_atomic_read_u32(&state_->hdr->cancel) != 0;
}

void
SessionChannel::RequestCancelAt(char *base) {
	auto *hdr = (SessionChannelHeader *)base;
	pg_atomic_write_u32(&hdr->cancel, 1);
}

namespace {

bool
IsScanReply(FrameTag tag) {
	return tag == FrameTag::ScanChunk || tag == FrameTag::ScanChunkArrow || tag == FrameTag::ScanDone ||
	       tag == FrameTag::ScanError;
}

bool
IsMetaReply(FrameTag tag) {
	return tag == FrameTag::MetaResult || tag == FrameTag::MetaError || tag == FrameTag::DescribeRelResult ||
	       tag == FrameTag::DescribeRelError;
}

} // namespace

/* Pull the next frame of one lane. Holds demux_lock across the raw receive and the
 * routing so a lane's frames keep wire order even with several concurrent pollers;
 * the process lock nests inside (demux -> process, never the reverse). Scan replies
 * carry a [uint32 scan_id] prefix that is stripped while routing. */
FrameResult
SessionChannel::RoutedRecv(bool meta, uint32_t scan_id, FrameTag *tag, std::string *payload) {
	for (;;) {
		{
			std::lock_guard<std::mutex> demux(state_->demux_lock);
			auto &lane = meta ? state_->meta_lane : state_->scan_lanes[scan_id];
			if (!lane.empty()) {
				*tag = lane.front().tag;
				*payload = std::move(lane.front().payload);
				lane.pop_front();
				return FrameResult::Ok;
			}
			FrameTag rtag = FrameTag::Error;
			const char *data = nullptr;
			std::size_t len = 0;
			FrameResult r;
			{
				std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
				r = RecvFrame(state_->control, &rtag, &data, &len, /*nowait=*/true);
			}
			if (r == FrameResult::Detached)
				return FrameResult::Detached;
			if (r == FrameResult::Ok) {
				if (IsScanReply(rtag) && len >= sizeof(uint32_t)) {
					uint32_t id = 0;
					std::memcpy(&id, data, sizeof(id));
					if (!meta && id == scan_id) {
						*tag = rtag;
						payload->assign(data + sizeof(id), len - sizeof(id));
						return FrameResult::Ok;
					}
					if (state_->closed_scans.count(id)) {
						// Late reply for a closed lane: drop it (freeing its page)
						// instead of re-creating the lane.
						ReleaseArrowFramePage(rtag, data + sizeof(id), len - sizeof(id));
						continue;
					}
					state_->scan_lanes[id].push_back(
					    RoutedFrame {rtag, std::string(data + sizeof(id), len - sizeof(id))});
					continue;
				}
				if (IsMetaReply(rtag)) {
					if (meta) {
						*tag = rtag;
						payload->assign(data, len);
						return FrameResult::Ok;
					}
					state_->meta_lane.push_back(RoutedFrame {rtag, std::string(data, len)});
					continue;
				}
				continue; /* unexpected frame kind: drop */
			}
		}
		if (IsCancelRequested() || g_worker_draining.load(std::memory_order_relaxed))
			return FrameResult::Detached;
		pg_usleep(200);
	}
}

FrameResult
SessionChannel::RecvMetaReply(FrameTag *tag, std::string *payload) {
	return RoutedRecv(true, 0, tag, payload);
}

FrameResult
SessionChannel::RecvScanReply(uint32_t scan_id, FrameTag *tag, std::string *payload) {
	return RoutedRecv(false, scan_id, tag, payload);
}

std::mutex &
SessionChannel::MetaRequestMutex() {
	return state_->meta_request_lock;
}

void
SessionChannel::CloseScanLane(uint32_t scan_id, const std::function<void(FrameTag, const std::string &)> &on_frame) {
	std::lock_guard<std::mutex> demux(state_->demux_lock);
	state_->closed_scans.insert(scan_id);
	auto it = state_->scan_lanes.find(scan_id);
	if (it == state_->scan_lanes.end())
		return;
	for (auto &frame : it->second)
		on_frame(frame.tag, frame.payload);
	state_->scan_lanes.erase(it);
}

/* Thread-local: each duckdb-worker session runs on its own thread (Phase 4), so the
 * "current session" is per-thread, not per-process. */
static thread_local SessionChannel *g_current_worker_session = nullptr;

void
SetCurrentWorkerSession(SessionChannel *channel) {
	g_current_worker_session = channel;
}

SessionChannel *
CurrentWorkerSession() {
	return g_current_worker_session;
}

/* Set when the worker is shutting down so session-thread polls bail promptly. */
void
SetWorkerDraining(bool draining) {
	g_worker_draining.store(draining, std::memory_order_relaxed);
}

bool
IsWorkerDraining() {
	return g_worker_draining.load(std::memory_order_relaxed);
}

/* Worker-side transport for the multi-threaded duckdb worker. The shm_mq rings, MyLatch,
 * and palloc are not thread-safe, so every poke is taken under the global process lock and
 * the lock is dropped between attempts (a short sleep) so a thread never blocks holding it.
 * This keeps MyLatch owned solely by the worker's main loop and serializes all PG access.
 * Returns false on detach, cancel, or worker drain. */
bool
SessionChannel::SerializedSendResult(FrameTag tag, const char *data, std::size_t len) {
	for (;;) {
		shm_mq_result r;
		{
			std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
			r = SendFrame(state_->result, tag, data, len, /*nowait=*/true);
		}
		if (r == SHM_MQ_SUCCESS)
			return true;
		if (r == SHM_MQ_DETACHED)
			return false;
		if (IsCancelRequested() || g_worker_draining.load(std::memory_order_relaxed))
			return false;
		pg_usleep(200);
	}
}

FrameResult
SessionChannel::SerializedRecvControl(FrameTag *tag, const char **data, std::size_t *len) {
	for (;;) {
		FrameResult r;
		{
			std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
			r = RecvFrame(state_->control, tag, data, len, /*nowait=*/true);
		}
		if (r != FrameResult::WouldBlock)
			return r;
		if (IsCancelRequested() || g_worker_draining.load(std::memory_order_relaxed))
			return FrameResult::Detached;
		pg_usleep(200);
	}
}

} // namespace pgddb
