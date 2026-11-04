#include "pgddb/worker/duckdb_worker.hpp"
#include "pgddb/worker/worker_protocol.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <csignal>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "pgddb/pgddb_types.hpp"
#include "pgddb/utility/signal_guard.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/prepared_statement.hpp"

extern "C" {
#include "postgres.h"

#include "access/xact.h"
#include "miscadmin.h"
#include "nodes/params.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#if PG_VERSION_NUM >= 180000
#include "storage/waiteventset.h"
#endif
}

namespace pgddb {

DispatchHook pgddb_dispatch_hook = nullptr;

// Lets the file-local worker and backend code reach the extension's virtuals.
struct WorkerInternals {
	static const DuckdbWorker::Settings &
	SettingsOf(DuckdbWorker &w) {
		return w.settings_;
	}
	static duckdb::DuckDB &
	Prime(DuckdbWorker &w) {
		return w.Prime();
	}
	static void
	BeforeSession(DuckdbWorker &w) {
		w.BeforeSession();
	}
	static void
	ConfigureSession(DuckdbWorker &w, duckdb::Connection &con, const QueryFrame &frame) {
		w.ConfigureSession(con, frame);
	}
	static std::string
	ServeRequest(DuckdbWorker &w, const std::string &request) {
		return w.ServeRequest(request);
	}
};

namespace {

constexpr int MAX_WORKERS = 16;  // databases with a live worker
constexpr int MAILBOX_CAP = 256; // posted sessions a worker has not admitted yet
constexpr Size REQUEST_RING = 256 * 1024;
constexpr Size RESPONSE_RING = 1024 * 1024;
constexpr size_t OUTBOX_LIMIT = 4 * 1024 * 1024; // frame bytes a session thread may buffer
constexpr int SPAWN_TIMEOUT_MS = 60 * 1000;

enum WorkerState : int { WORKER_EMPTY = 0, WORKER_STARTING, WORKER_RUNNING };

struct MailboxEntry {
	dsm_handle handle;
	uint64 nonce;
};

struct WorkerEntry {
	Oid dboid;
	int state;
	uint32 generation; // bumped each time a worker reaches WORKER_RUNNING
	pid_t pid;
	pid_t starter_pid; // backend that claimed the entry while WORKER_STARTING
	uint32 claim;      // identifies the spawn this claim belongs to
	Latch *latch;
	dsm_handle arena_handle; // the worker's result page arena
	int active_sessions;
	uint64 total_sessions;
	uint32 mailbox_head;
	uint32 mailbox_count;
	MailboxEntry mailbox[MAILBOX_CAP];
};

struct WorkerShmem {
	slock_t lock;
	uint64 next_nonce;
	uint32 next_claim;
	WorkerEntry entries[MAX_WORKERS];
};

// dsm segment: header, then the two shm_mq rings.
struct SessionHeader {
	uint64 nonce;
	int backend_pid;
};

constexpr Size REQUEST_OFFSET = MAXALIGN(sizeof(SessionHeader));
constexpr Size RESPONSE_OFFSET = REQUEST_OFFSET + REQUEST_RING;
constexpr Size SEGMENT_SIZE = RESPONSE_OFFSET + RESPONSE_RING;

// Result page arena: one dsm segment per worker, shared by all its sessions.
// The worker owns the page accounting; backends only read pages and report
// them free.
struct ArenaHeader {
	uint32 page_size;
	uint32 page_count;
};

constexpr Size ARENA_PAGES_OFFSET = 64;
constexpr uint32 NO_PAGE = UINT32_MAX;

WorkerShmem *g_shmem = nullptr;
DuckdbWorker *g_worker = nullptr;
#if PG_VERSION_NUM >= 150000
shmem_request_hook_type prev_shmem_request_hook = nullptr;
#endif
shmem_startup_hook_type prev_shmem_startup_hook = nullptr;

bool g_in_worker = false;
std::thread::id g_main_thread;

// Caller holds g_shmem->lock.
WorkerEntry *
FindEntry(Oid dboid) {
	for (auto &entry : g_shmem->entries) {
		if (entry.state != WORKER_EMPTY && entry.dboid == dboid) {
			return &entry;
		}
	}
	return nullptr;
}

shm_mq_result
SendNowait(shm_mq_handle *mqh, const std::string &frame) {
#if PG_VERSION_NUM >= 150000
	return shm_mq_send(mqh, frame.size(), frame.data(), true, true);
#else
	return shm_mq_send(mqh, frame.size(), frame.data(), true);
#endif
}

void
ShmemRequest() {
#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook) {
		prev_shmem_request_hook();
	}
#endif
	RequestAddinShmemSpace(sizeof(WorkerShmem));
}

void
ShmemStartup() {
	if (prev_shmem_startup_hook) {
		prev_shmem_startup_hook();
	}
	bool found;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	g_shmem = (WorkerShmem *)ShmemInitStruct(WorkerInternals::SettingsOf(*g_worker).name, sizeof(WorkerShmem), &found);
	if (!found) {
		MemSet(g_shmem, 0, sizeof(WorkerShmem));
		SpinLockInit(&g_shmem->lock);
	}
	LWLockRelease(AddinShmemInitLock);
}

} // namespace

/* ------------------------------------------------------------------------
 * Backend side
 * ------------------------------------------------------------------------ */

class BackendSession final : public RemoteResult {
public:
	explicit BackendSession(DuckdbWorker &worker);
	~BackendSession() override;

	void Open(QueryFrame &frame);
	const duckdb::vector<duckdb::LogicalType> &Types() override;
	duckdb::unique_ptr<duckdb::DataChunk> Fetch() override;

	void Release();
	SubTransactionId subxid;

private:
	void EnsureWorker();
	void Post();
	void Send(const std::string &frame);
	void WaitForWorker();
	bool WorkerAttached();
	// Receives one frame; returns a chunk, or nullptr for SCHEMA/RPC/DONE.
	duckdb::unique_ptr<duckdb::DataChunk> ReceiveOne(bool &received);
	// Reports the page of the chunk just consumed; must run before any wait.
	void FreeHeldPage();

	dsm_handle arena_handle_ = 0;
	dsm_segment *arena_seg_ = nullptr;
	char *arena_base_ = nullptr;
	uint32 arena_page_size_ = 0;
	uint32 arena_page_count_ = 0;
	uint32 held_page_ = NO_PAGE;
	duckdb::vector<duckdb::unsafe_unique_array<duckdb::string_t>> strings_;

	DuckdbWorker &worker_;
	Oid dboid_;
	uint32 worker_gen_ = 0;
	dsm_segment *seg_ = nullptr;
	shm_mq *req_mq_ = nullptr;
	shm_mq *resp_mq_ = nullptr;
	shm_mq_handle *req_ = nullptr;
	shm_mq_handle *resp_ = nullptr;
	bool posted_ = false;
	bool released_ = false;
	bool done_ = false;
	bool have_types_ = false;
	duckdb::vector<duckdb::LogicalType> types_;
};

namespace {

std::vector<BackendSession *> g_backend_sessions;
bool g_backend_exit_hook = false;

// Arena mappings are shared by this backend's sessions: dsm_attach refuses to
// map a handle twice, and dispatches nest (a function called from a
// dispatched statement dispatches its own queries).
struct ArenaMapping {
	dsm_segment *seg;
	int refs;
};
std::unordered_map<dsm_handle, ArenaMapping> g_arena_mappings;

dsm_segment *
AttachArena(dsm_handle handle) {
	auto it = g_arena_mappings.find(handle);
	if (it != g_arena_mappings.end()) {
		it->second.refs++;
		return it->second.seg;
	}
	dsm_segment *seg = dsm_attach(handle);
	if (!seg) {
		return nullptr;
	}
	dsm_pin_mapping(seg);
	g_arena_mappings[handle] = {seg, 1};
	return seg;
}

void
DetachArena(dsm_handle handle) {
	auto it = g_arena_mappings.find(handle);
	if (it == g_arena_mappings.end()) {
		return;
	}
	if (--it->second.refs == 0) {
		dsm_detach(it->second.seg);
		g_arena_mappings.erase(it);
	}
}

// Sessions are deleted only at top-level transaction end or process exit,
// when no executor state can still hold one. A subtransaction abort just
// detaches the sessions it created: their scan state may outlive it.
void
DeleteAllSessions() {
	auto sessions = g_backend_sessions;
	for (auto *s : sessions) {
		delete s;
	}
}

void
BackendXactCallback(XactEvent event, void *) {
	switch (event) {
	case XACT_EVENT_ABORT:
	case XACT_EVENT_PARALLEL_ABORT:
	case XACT_EVENT_COMMIT:
	case XACT_EVENT_PARALLEL_COMMIT:
		DeleteAllSessions();
		break;
	default:
		break;
	}
}

void
BackendSubXactCallback(SubXactEvent event, SubTransactionId my_subid, SubTransactionId parent_subid, void *) {
	if (event == SUBXACT_EVENT_ABORT_SUB) {
		for (auto *s : g_backend_sessions) {
			if (s->subxid == my_subid) {
				s->Release();
			}
		}
	} else if (event == SUBXACT_EVENT_COMMIT_SUB) {
		for (auto *s : g_backend_sessions) {
			if (s->subxid == my_subid) {
				s->subxid = parent_subid;
			}
		}
	}
}

void
BackendExitCallback(int, Datum) {
	DeleteAllSessions();
}

} // namespace

BackendSession::BackendSession(DuckdbWorker &worker)
    : subxid(GetCurrentSubTransactionId()), worker_(worker), dboid_(MyDatabaseId) {
	if (!g_backend_exit_hook) {
		before_shmem_exit(BackendExitCallback, 0);
		g_backend_exit_hook = true;
	}
	g_backend_sessions.push_back(this);
}

BackendSession::~BackendSession() {
	Release();
	for (auto it = g_backend_sessions.begin(); it != g_backend_sessions.end(); ++it) {
		if (*it == this) {
			g_backend_sessions.erase(it);
			break;
		}
	}
}

// Detaches both rings and the segment; the worker sees SHM_MQ_DETACHED.
void
BackendSession::Release() {
	if (released_) {
		return;
	}
	released_ = true;
	if (req_) {
		shm_mq_detach(req_);
		req_ = nullptr;
	}
	if (resp_) {
		shm_mq_detach(resp_);
		resp_ = nullptr;
	}
	if (seg_) {
		dsm_detach(seg_);
		seg_ = nullptr;
	}
	strings_.clear();
	if (arena_seg_) {
		DetachArena(arena_handle_);
		arena_seg_ = nullptr;
	}
}

// Finds the database's worker or spawns one; waits until it is running.
void
BackendSession::EnsureWorker() {
	auto &settings = WorkerInternals::SettingsOf(*g_worker);
	for (int waited_ms = 0;;) {
		WorkerEntry *entry;
		SpinLockAcquire(&g_shmem->lock);
		entry = FindEntry(dboid_);
		if (entry && entry->state == WORKER_RUNNING) {
			worker_gen_ = entry->generation;
			arena_handle_ = entry->arena_handle;
			SpinLockRelease(&g_shmem->lock);
			return;
		}
		if (entry && entry->state == WORKER_STARTING) {
			// A claim whose backend died before any worker took it over is withdrawn.
			bool orphaned = entry->pid == 0 && kill(entry->starter_pid, 0) != 0;
			if (orphaned) {
				entry->state = WORKER_EMPTY;
			}
			SpinLockRelease(&g_shmem->lock);
			if (orphaned) {
				continue;
			}
			if (waited_ms > SPAWN_TIMEOUT_MS) {
				ereport(ERROR, (errmsg("duckdb worker for database %u did not start", dboid_)));
			}
			CHECK_FOR_INTERRUPTS();
			WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 10, PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
			waited_ms += 10;
			continue;
		}
		for (auto &candidate : g_shmem->entries) {
			if (candidate.state == WORKER_EMPTY) {
				entry = &candidate;
				break;
			}
		}
		if (!entry) {
			SpinLockRelease(&g_shmem->lock);
			ereport(ERROR, (errmsg("too many databases with a duckdb worker (maximum %d)", MAX_WORKERS)));
		}
		MemSet(entry, 0, sizeof(WorkerEntry));
		entry->dboid = dboid_;
		entry->state = WORKER_STARTING;
		entry->starter_pid = MyProcPid;
		entry->claim = ++g_shmem->next_claim;
		uint32 claim = entry->claim;
		SpinLockRelease(&g_shmem->lock);

		BackgroundWorker bgw;
		MemSet(&bgw, 0, sizeof(BackgroundWorker));
		bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
		bgw.bgw_restart_time = BGW_NEVER_RESTART;
		snprintf(bgw.bgw_library_name, BGW_MAXLEN, "%s", settings.library_name);
		snprintf(bgw.bgw_function_name, BGW_MAXLEN, "%s", settings.function_name);
		snprintf(bgw.bgw_name, BGW_MAXLEN, "%s (db %u)", settings.name, dboid_);
		snprintf(bgw.bgw_type, BGW_MAXLEN, "%s", settings.name);
		bgw.bgw_main_arg = ObjectIdGetDatum(dboid_);
		memcpy(bgw.bgw_extra, &claim, sizeof(claim)); // the worker must find this claim
		bgw.bgw_notify_pid = MyProcPid;

		BackgroundWorkerHandle *handle;
		bool registered = RegisterDynamicBackgroundWorker(&bgw, &handle);
		if (!registered) {
			SpinLockAcquire(&g_shmem->lock);
			if (entry->state == WORKER_STARTING && entry->starter_pid == MyProcPid) {
				entry->state = WORKER_EMPTY;
			}
			SpinLockRelease(&g_shmem->lock);
			ereport(ERROR, (errmsg("could not register the duckdb worker"),
			                errhint("Consider raising max_worker_processes.")));
		}

		for (int spawn_ms = 0;; spawn_ms += 10) {
			pid_t pid;
			BgwHandleStatus status = GetBackgroundWorkerPid(handle, &pid);
			SpinLockAcquire(&g_shmem->lock);
			bool running = entry->state == WORKER_RUNNING && entry->dboid == dboid_;
			if (running) {
				worker_gen_ = entry->generation;
				arena_handle_ = entry->arena_handle;
			}
			bool stopped = status == BGWH_STOPPED;
			if (stopped && entry->state == WORKER_STARTING && entry->starter_pid == MyProcPid) {
				entry->state = WORKER_EMPTY;
			}
			SpinLockRelease(&g_shmem->lock);
			if (running) {
				return;
			}
			if (stopped) {
				ereport(ERROR, (errmsg("duckdb worker for database %u failed to start", dboid_),
				                errhint("See the server log for details.")));
			}
			if (spawn_ms > SPAWN_TIMEOUT_MS) {
				TerminateBackgroundWorker(handle);
				SpinLockAcquire(&g_shmem->lock);
				if (entry->state == WORKER_STARTING && entry->starter_pid == MyProcPid) {
					entry->state = WORKER_EMPTY;
				}
				SpinLockRelease(&g_shmem->lock);
				ereport(ERROR, (errmsg("duckdb worker for database %u did not start", dboid_)));
			}
			CHECK_FOR_INTERRUPTS();
			WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 10, PG_WAIT_EXTENSION);
			ResetLatch(MyLatch);
		}
	}
}

// Posts {handle, nonce} on the worker's mailbox and wakes it.
void
BackendSession::Post() {
	auto *hdr = (SessionHeader *)dsm_segment_address(seg_);
	for (;;) {
		Latch *latch = nullptr;
		SpinLockAcquire(&g_shmem->lock);
		WorkerEntry *entry = FindEntry(dboid_);
		bool gone = !entry || entry->state != WORKER_RUNNING || entry->generation != worker_gen_;
		if (!gone && entry->mailbox_count < MAILBOX_CAP) {
			uint32 slot = (entry->mailbox_head + entry->mailbox_count) % MAILBOX_CAP;
			entry->mailbox[slot].handle = dsm_segment_handle(seg_);
			entry->mailbox[slot].nonce = hdr->nonce;
			entry->mailbox_count++;
			latch = entry->latch;
		}
		SpinLockRelease(&g_shmem->lock);
		if (gone) {
			throw duckdb::IOException("duckdb worker exited before accepting the query");
		}
		if (latch) {
			SetLatch(latch);
			posted_ = true;
			return;
		}
		CHECK_FOR_INTERRUPTS();
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 10, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
}

bool
BackendSession::WorkerAttached() {
	return shm_mq_get_sender(resp_mq_) != nullptr;
}

// Sends a frame on the request ring, waiting for room.
void
BackendSession::Send(const std::string &frame) {
	for (;;) {
		shm_mq_result res = SendNowait(req_, frame);
		if (res == SHM_MQ_SUCCESS) {
			return;
		}
		if (res == SHM_MQ_DETACHED) {
			throw duckdb::IOException("duckdb worker exited while the query was running");
		}
		WaitForWorker();
	}
}

// One wait step of the service loop: interrupts, worker liveness, latch.
void
BackendSession::WaitForWorker() {
	if (QueryCancelPending || ProcDiePending) {
		Release();
		CHECK_FOR_INTERRUPTS();
		throw duckdb::InterruptException();
	}
	SpinLockAcquire(&g_shmem->lock);
	WorkerEntry *entry = FindEntry(dboid_);
	bool gone = !entry || entry->state != WORKER_RUNNING || entry->generation != worker_gen_;
	SpinLockRelease(&g_shmem->lock);
	if (gone) {
		throw duckdb::IOException(WorkerAttached() ? "duckdb worker exited while the query was running"
		                                           : "duckdb worker exited before accepting the query");
	}
	WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1000, PG_WAIT_EXTENSION);
	ResetLatch(MyLatch);
}

void
BackendSession::Open(QueryFrame &frame) {
	EnsureWorker();

	// Handles live in TopMemoryContext: a longjmp must not free them before Release.
	MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
	seg_ = dsm_create(SEGMENT_SIZE, 0);
	dsm_pin_mapping(seg_);
	char *base = (char *)dsm_segment_address(seg_);
	auto *hdr = (SessionHeader *)base;
	SpinLockAcquire(&g_shmem->lock);
	hdr->nonce = ++g_shmem->next_nonce;
	SpinLockRelease(&g_shmem->lock);
	hdr->backend_pid = MyProcPid;
	req_mq_ = shm_mq_create(base + REQUEST_OFFSET, REQUEST_RING);
	shm_mq_set_sender(req_mq_, MyProc);
	resp_mq_ = shm_mq_create(base + RESPONSE_OFFSET, RESPONSE_RING);
	shm_mq_set_receiver(resp_mq_, MyProc);
	req_ = shm_mq_attach(req_mq_, seg_, nullptr);
	resp_ = shm_mq_attach(resp_mq_, seg_, nullptr);
	MemoryContextSwitchTo(old_context);

	arena_seg_ = AttachArena(arena_handle_);
	if (!arena_seg_) {
		throw duckdb::IOException("duckdb worker exited before accepting the query");
	}
	auto *arena = (ArenaHeader *)dsm_segment_address(arena_seg_);
	if (ARENA_PAGES_OFFSET + (Size)arena->page_size * arena->page_count > dsm_segment_map_length(arena_seg_)) {
		throw duckdb::IOException("duckdb worker arena header does not match its segment");
	}
	arena_page_size_ = arena->page_size;
	arena_page_count_ = arena->page_count;
	arena_base_ = (char *)arena + ARENA_PAGES_OFFSET;

	Post();
	Send(EncodeQueryFrame(frame));
}

void
BackendSession::FreeHeldPage() {
	if (held_page_ == NO_PAGE || released_) {
		return;
	}
	uint32 page = held_page_;
	held_page_ = NO_PAGE;
	strings_.clear();
	std::string frame = EncodeChunkFreeFrame(page);
	for (;;) {
		shm_mq_result res = SendNowait(req_, frame);
		if (res == SHM_MQ_SUCCESS || res == SHM_MQ_DETACHED) {
			return; // a closed session has reclaimed its pages already
		}
		WaitForWorker();
	}
}

duckdb::unique_ptr<duckdb::DataChunk>
BackendSession::ReceiveOne(bool &received) {
	Size len;
	void *data;
	received = false;
	shm_mq_result res = shm_mq_receive(resp_, &len, &data, true);
	if (res == SHM_MQ_WOULD_BLOCK) {
		WaitForWorker();
		return nullptr;
	}
	if (res == SHM_MQ_DETACHED) {
		throw duckdb::IOException("duckdb worker exited while the query was running");
	}
	received = true;
	auto *bytes = (const char *)data;
	switch (DecodeFrameTag(bytes, len)) {
	case FrameTag::SCHEMA:
		types_ = DecodeSchemaFrame(bytes, len);
		have_types_ = true;
		return nullptr;
	case FrameTag::CHUNK:
		return DecodeChunkFrame(bytes, len);
	case FrameTag::CHUNK_REF: {
		uint32 page = DecodeChunkRefFrame(bytes, len);
		if (page >= arena_page_count_) {
			throw duckdb::IOException("duckdb worker referenced an invalid arena page");
		}
		held_page_ = page;
		return ReadPage(arena_base_ + (size_t)page * arena_page_size_, arena_page_size_, types_, strings_);
	}
	case FrameTag::DONE:
		done_ = true;
		return nullptr;
	case FrameTag::QUERY_ERROR: {
		auto error = DecodeErrorFrame(bytes, len);
		done_ = true;
		duckdb::ErrorData(error.type, error.message).Throw();
		return nullptr;
	}
	case FrameTag::RPC: {
		std::string reply = WorkerInternals::ServeRequest(worker_, DecodePayloadFrame(bytes, len));
		Send(EncodeRpcReplyFrame(reply));
		return nullptr;
	}
	default:
		throw duckdb::IOException("unexpected frame from the duckdb worker");
	}
}

const duckdb::vector<duckdb::LogicalType> &
BackendSession::Types() {
	while (!have_types_ && !done_) {
		bool received;
		auto chunk = ReceiveOne(received);
		if (chunk) {
			throw duckdb::IOException("duckdb worker sent a chunk before the schema");
		}
	}
	return types_;
}

duckdb::unique_ptr<duckdb::DataChunk>
BackendSession::Fetch() {
	FreeHeldPage();
	while (!done_) {
		bool received;
		auto chunk = ReceiveOne(received);
		if (chunk) {
			return chunk;
		}
	}
	return nullptr;
}

/* ------------------------------------------------------------------------
 * Worker side
 * ------------------------------------------------------------------------ */

namespace {

struct WorkerSession {
	dsm_segment *seg = nullptr;
	shm_mq *req_mq = nullptr;
	shm_mq *resp_mq = nullptr;
	shm_mq_handle *req = nullptr;
	shm_mq_handle *resp = nullptr;
	duckdb::unique_ptr<duckdb::Connection> con;
	QueryFrame frame;
	std::thread thread;
	bool started = false;  // main thread only
	bool detached = false; // main thread only
	std::string inflight;  // frame being sent; main thread only
	std::atomic<bool> done {false};

	// Shared between the main thread and the session's DuckDB threads.
	std::mutex mu;
	std::condition_variable cv;
	std::deque<std::string> outbox;
	size_t outbox_bytes = 0;
	std::atomic<bool> cancelled {false};
	enum { RPC_NONE, RPC_PENDING, RPC_REPLIED, RPC_FAILED } rpc = RPC_NONE;
	std::string rpc_reply;

	// Arena pages, guarded by g_arena.mu.
	uint32 credit = 0;           // reserved pages not in use
	uint32 overflow_held = 0;    // pages taken from the shared overflow
	std::vector<uint32> writing; // taken, not yet referenced by a frame
	std::vector<uint32> held;    // referenced by a frame; the backend frees them
};

// Page accounting, guarded by mu. free_pages always holds at least
// reserved_outstanding pages, so a session with credit never waits.
struct Arena {
	dsm_segment *seg = nullptr;
	char *base = nullptr;
	uint32 page_size = 0;
	uint32 page_count = 0;
	std::mutex mu;
	std::condition_variable cv;
	std::vector<uint32> free_pages;
	uint32 reserved_outstanding = 0;
} g_arena;

std::vector<std::unique_ptr<WorkerSession>> g_sessions;
std::mutex g_ctx_mutex;
std::unordered_map<duckdb::ClientContext *, WorkerSession *> g_ctx_sessions;
duckdb::DuckDB *g_db = nullptr;
Oid g_worker_dboid = InvalidOid;
std::atomic<bool> g_engine_invalidated {false};

WorkerSession *
SessionFor(duckdb::ClientContext &context) {
	std::lock_guard<std::mutex> lock(g_ctx_mutex);
	auto it = g_ctx_sessions.find(&context);
	return it == g_ctx_sessions.end() ? nullptr : it->second;
}

// Session thread: a reserved page if the session has credit left, else an
// overflow page beyond what other sessions are promised.
uint32
AcquirePage(WorkerSession &s) {
	std::unique_lock<std::mutex> lock(g_arena.mu);
	g_arena.cv.wait(
	    lock, [&] { return s.cancelled || s.credit > 0 || g_arena.free_pages.size() > g_arena.reserved_outstanding; });
	if (s.cancelled) {
		throw duckdb::InterruptException();
	}
	uint32 page = g_arena.free_pages.back();
	g_arena.free_pages.pop_back();
	if (s.credit > 0) {
		s.credit--;
		g_arena.reserved_outstanding--;
	} else {
		s.overflow_held++;
	}
	s.writing.push_back(page);
	return page;
}

// Session thread: the page is written and about to be referenced by a frame.
void
PublishPage(WorkerSession &s, uint32 page) {
	std::lock_guard<std::mutex> lock(g_arena.mu);
	s.writing.erase(std::find(s.writing.begin(), s.writing.end(), page));
	s.held.push_back(page);
}

// Main thread: the backend is done with one of the session's pages.
void
FreePage(WorkerSession &s, uint32 page) {
	std::lock_guard<std::mutex> lock(g_arena.mu);
	auto it = std::find(s.held.begin(), s.held.end(), page);
	if (it == s.held.end()) {
		return;
	}
	s.held.erase(it);
	g_arena.free_pages.push_back(page);
	if (s.overflow_held > 0) {
		s.overflow_held--;
	} else {
		s.credit++;
		g_arena.reserved_outstanding++;
	}
	g_arena.cv.notify_all();
}

// Main thread, after the session thread exited: everything goes back.
void
ReclaimPages(WorkerSession &s) {
	std::lock_guard<std::mutex> lock(g_arena.mu);
	for (uint32 page : s.held) {
		g_arena.free_pages.push_back(page);
	}
	for (uint32 page : s.writing) {
		g_arena.free_pages.push_back(page);
	}
	s.held.clear();
	s.writing.clear();
	g_arena.reserved_outstanding -= s.credit;
	s.credit = 0;
	s.overflow_held = 0;
	g_arena.cv.notify_all();
}

// Main thread: promise `count` pages to a session about to be admitted.
bool
ReservePages(uint32 count) {
	std::lock_guard<std::mutex> lock(g_arena.mu);
	if (g_arena.free_pages.size() - g_arena.reserved_outstanding < count) {
		return false;
	}
	g_arena.reserved_outstanding += count;
	return true;
}

void
UnreservePages(uint32 count) {
	std::lock_guard<std::mutex> lock(g_arena.mu);
	g_arena.reserved_outstanding -= count;
	g_arena.cv.notify_all();
}

// Session thread: queue a frame for the main thread; dropped once cancelled.
void
PushFrame(WorkerSession &s, std::string frame) {
	{
		std::unique_lock<std::mutex> lock(s.mu);
		s.cv.wait(lock, [&] { return s.cancelled || s.outbox_bytes < OUTBOX_LIMIT; });
		if (s.cancelled) {
			return;
		}
		s.outbox_bytes += frame.size();
		s.outbox.push_back(std::move(frame));
	}
	SetLatch(MyLatch);
}

void
RunSessionQuery(WorkerSession &s) {
	auto &con = *s.con;
	WorkerInternals::ConfigureSession(*g_worker, con, s.frame);

	auto prepared = con.Prepare(s.frame.sql);
	if (prepared->HasError()) {
		prepared->GetErrorObject().Throw();
	}
	duckdb::case_insensitive_map_t<duckdb::BoundParameterData> named_values;
	for (auto &param : s.frame.params) {
		if (prepared->named_param_map.count(param.first)) {
			named_values[param.first] = duckdb::BoundParameterData(param.second);
		}
	}
	auto pending = prepared->PendingQuery(named_values, true);
	if (pending->HasError()) {
		pending->ThrowError();
	}
	auto result = pending->Execute();
	if (result->HasError()) {
		result->ThrowError();
	}
	PushFrame(s, EncodeSchemaFrame(result->types));
	bool by_reference = PageLayoutSupported(result->types);
	while (auto chunk = result->Fetch()) {
		if (chunk->size() == 0) {
			break;
		}
		if (!by_reference) {
			PushFrame(s, EncodeChunkFrame(*chunk));
			continue;
		}
		chunk->Flatten();
		idx_t start = 0;
		while (start < chunk->size()) {
			idx_t rows = RowsThatFitPage(*chunk, start, g_arena.page_size);
			if (rows == 0) {
				// A row larger than a page: the rest of the chunk travels serialized.
				duckdb::DataChunk rest;
				rest.Initialize(duckdb::Allocator::DefaultAllocator(), chunk->GetTypes());
				chunk->Copy(rest, start);
				PushFrame(s, EncodeChunkFrame(rest));
				break;
			}
			uint32 page = AcquirePage(s);
			WritePage(*chunk, start, rows, g_arena.base + (size_t)page * g_arena.page_size);
			PublishPage(s, page);
			PushFrame(s, EncodeChunkRefFrame(page));
			start += rows;
		}
	}
	// A streaming result reports an error in a later batch through Fetch.
	if (result->HasError()) {
		result->ThrowError();
	}
	PushFrame(s, EncodeDoneFrame());
}

void
SessionThreadMain(WorkerSession *s) {
	auto *context = s->con->context.get();
	{
		std::lock_guard<std::mutex> lock(g_ctx_mutex);
		g_ctx_sessions[context] = s;
	}
	try {
		RunSessionQuery(*s);
	} catch (duckdb::Exception &ex) {
		duckdb::ErrorData error(ex);
		PushFrame(*s, EncodeErrorFrame(error.Type(), error.RawMessage()));
	} catch (std::exception &ex) {
		PushFrame(*s, EncodeErrorFrame(duckdb::ExceptionType::EXECUTOR, ex.what()));
	}
	if (duckdb::ValidChecker::IsInvalidated(*g_db->instance)) {
		g_engine_invalidated = true;
	}
	{
		std::lock_guard<std::mutex> lock(g_ctx_mutex);
		g_ctx_sessions.erase(context);
	}
	s->done = true;
	SetLatch(MyLatch);
}

void
StartSessionThread(WorkerSession &s) {
	s.started = true;
	ThreadSignalBlockGuard guard;
	s.thread = std::thread(SessionThreadMain, &s);
}

// The backend went away: interrupt the query, fail its RPC, drop its frames.
void
DetachSession(WorkerSession &s) {
	if (s.detached) {
		return;
	}
	s.detached = true;
	{
		std::lock_guard<std::mutex> lock(s.mu);
		s.cancelled = true;
		s.outbox.clear();
		s.outbox_bytes = 0;
		if (s.rpc == WorkerSession::RPC_PENDING) {
			s.rpc = WorkerSession::RPC_FAILED;
		}
	}
	s.cv.notify_all();
	{
		// Taking the arena mutex orders this after any predicate check in
		// AcquirePage that has not started waiting yet, so the wakeup is never lost.
		std::lock_guard<std::mutex> lock(g_arena.mu);
	}
	g_arena.cv.notify_all();
	if (s.started) {
		s.con->Interrupt();
	}
}

void
PumpSession(WorkerSession &s) {
	while (!s.detached) {
		Size len;
		void *data;
		shm_mq_result res = shm_mq_receive(s.req, &len, &data, true);
		if (res == SHM_MQ_WOULD_BLOCK) {
			break;
		}
		if (res == SHM_MQ_DETACHED) {
			DetachSession(s);
			break;
		}
		auto *bytes = (const char *)data;
		switch (DecodeFrameTag(bytes, len)) {
		case FrameTag::QUERY:
			if (!s.started) {
				s.frame = DecodeQueryFrame(bytes, len);
				StartSessionThread(s);
			}
			break;
		case FrameTag::RPC_REPLY: {
			std::lock_guard<std::mutex> lock(s.mu);
			if (s.rpc == WorkerSession::RPC_PENDING) {
				s.rpc_reply = DecodePayloadFrame(bytes, len);
				s.rpc = WorkerSession::RPC_REPLIED;
			}
			s.cv.notify_all();
			break;
		}
		case FrameTag::CHUNK_FREE:
			FreePage(s, DecodeChunkFreeFrame(bytes, len));
			break;
		default:
			DetachSession(s);
			break;
		}
	}
	while (!s.detached) {
		if (s.inflight.empty()) {
			std::lock_guard<std::mutex> lock(s.mu);
			if (s.outbox.empty()) {
				break;
			}
			s.inflight = std::move(s.outbox.front());
			s.outbox.pop_front();
			s.outbox_bytes -= s.inflight.size();
			s.cv.notify_all();
		}
		shm_mq_result res = SendNowait(s.resp, s.inflight);
		if (res == SHM_MQ_SUCCESS) {
			s.inflight.clear();
			continue;
		}
		if (res == SHM_MQ_WOULD_BLOCK) {
			break; // the backend's receive sets our latch
		}
		DetachSession(s);
	}
}

bool
SessionFinished(WorkerSession &s) {
	if (s.started && !s.done) {
		return false;
	}
	if (s.detached) {
		return true;
	}
	if (!s.started) {
		return false; // waiting for the QUERY frame
	}
	{
		std::lock_guard<std::mutex> lock(s.mu);
		if (!s.outbox.empty() || !s.inflight.empty()) {
			return false;
		}
	}
	// Pages stay the backend's until it frees them or detaches.
	std::lock_guard<std::mutex> lock(g_arena.mu);
	return s.held.empty() && s.writing.empty();
}

void
CloseSession(WorkerSession &s) {
	if (s.started) {
		s.thread.join();
	}
	ReclaimPages(s); // only after the join: the thread may have been writing a page
	shm_mq_detach(s.req);
	shm_mq_detach(s.resp);
	dsm_detach(s.seg);
}

void
ReapSessions() {
	for (auto it = g_sessions.begin(); it != g_sessions.end();) {
		if (SessionFinished(**it)) {
			CloseSession(**it);
			it = g_sessions.erase(it);
			SpinLockAcquire(&g_shmem->lock);
			WorkerEntry *entry = FindEntry(g_worker_dboid);
			if (entry && entry->pid == MyProcPid) {
				entry->active_sessions--;
			}
			SpinLockRelease(&g_shmem->lock);
		} else {
			++it;
		}
	}
}

bool
PopMailbox(MailboxEntry &out) {
	SpinLockAcquire(&g_shmem->lock);
	WorkerEntry *entry = FindEntry(g_worker_dboid);
	bool found = entry && entry->pid == MyProcPid && entry->mailbox_count > 0;
	if (found) {
		out = entry->mailbox[entry->mailbox_head];
		entry->mailbox_head = (entry->mailbox_head + 1) % MAILBOX_CAP;
		entry->mailbox_count--;
		entry->active_sessions++;
		entry->total_sessions++;
	}
	SpinLockRelease(&g_shmem->lock);
	return found;
}

void
UndoAdmission() {
	SpinLockAcquire(&g_shmem->lock);
	WorkerEntry *entry = FindEntry(g_worker_dboid);
	if (entry && entry->pid == MyProcPid) {
		entry->active_sessions--;
		entry->total_sessions--;
	}
	SpinLockRelease(&g_shmem->lock);
}

// Admits posted sessions while below capacity and while the arena can
// reserve their pages: attach, validate, connect.
void
AdmitSessions() {
	auto &settings = WorkerInternals::SettingsOf(*g_worker);
	for (;;) {
		int max_sessions = *settings.max_sessions;
		if ((int)g_sessions.size() >= (max_sessions < 1 ? 1 : max_sessions)) {
			return;
		}
		uint32 reserved = std::min((uint32)std::max(*settings.reserved_pages, 1), g_arena.page_count);
		if (!ReservePages(reserved)) {
			return;
		}
		MailboxEntry entry;
		if (!PopMailbox(entry)) {
			UnreservePages(reserved);
			return;
		}
		if (dsm_find_mapping(entry.handle)) {
			UndoAdmission(); // a backend reposted a session we already serve
			UnreservePages(reserved);
			continue;
		}
		WorkerInternals::BeforeSession(*g_worker);
		dsm_segment *seg = dsm_attach(entry.handle);
		if (!seg) {
			UndoAdmission(); // the backend is gone; the segment with it
			UnreservePages(reserved);
			continue;
		}
		dsm_pin_mapping(seg);
		char *base = (char *)dsm_segment_address(seg);
		auto *hdr = (SessionHeader *)base;
		if (hdr->nonce != entry.nonce) {
			dsm_detach(seg); // the handle was reused by a newer session
			UndoAdmission();
			UnreservePages(reserved);
			continue;
		}
		auto session = std::make_unique<WorkerSession>();
		session->credit = reserved;
		session->seg = seg;
		session->req_mq = (shm_mq *)(base + REQUEST_OFFSET);
		session->resp_mq = (shm_mq *)(base + RESPONSE_OFFSET);
		MemoryContext old_context = MemoryContextSwitchTo(TopMemoryContext);
		shm_mq_set_receiver(session->req_mq, MyProc);
		shm_mq_set_sender(session->resp_mq, MyProc);
		session->req = shm_mq_attach(session->req_mq, seg, nullptr);
		session->resp = shm_mq_attach(session->resp_mq, seg, nullptr);
		MemoryContextSwitchTo(old_context);
		try {
			session->con = duckdb::make_uniq<duckdb::Connection>(*g_db);
		} catch (std::exception &) {
			shm_mq_detach(session->req);
			shm_mq_detach(session->resp);
			dsm_detach(seg);
			UndoAdmission();
			UnreservePages(reserved);
			continue;
		}
		g_sessions.push_back(std::move(session));
	}
}

// Exit path: interrupt every session, join its thread, detach everything.
void
DrainSessions() {
	for (auto &s : g_sessions) {
		DetachSession(*s);
	}
	for (auto &s : g_sessions) {
		CloseSession(*s);
	}
	g_sessions.clear();
}

void
WorkerExitCallback(int, Datum) {
	DrainSessions();
	SpinLockAcquire(&g_shmem->lock);
	WorkerEntry *entry = FindEntry(g_worker_dboid);
	if (entry && entry->pid == MyProcPid) {
		entry->state = WORKER_EMPTY;
		entry->pid = 0;
		entry->latch = nullptr;
		entry->active_sessions = 0;
	}
	SpinLockRelease(&g_shmem->lock);
}

void
WorkerMainLoop(DuckdbWorker &worker, Oid dboid) {
	g_in_worker = true;
	g_main_thread = std::this_thread::get_id();
	g_worker_dboid = dboid;

	uint32 claim;
	memcpy(&claim, MyBgworkerEntry->bgw_extra, sizeof(claim));
	SpinLockAcquire(&g_shmem->lock);
	WorkerEntry *entry = FindEntry(dboid);
	bool claimed = entry && entry->state == WORKER_STARTING && entry->claim == claim;
	if (claimed) {
		entry->pid = MyProcPid;
	}
	SpinLockRelease(&g_shmem->lock);
	if (!claimed) {
		proc_exit(0); // the spawn timed out and was withdrawn
	}
	before_shmem_exit(WorkerExitCallback, 0);

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	try {
		g_db = &WorkerInternals::Prime(worker);
	} catch (std::exception &ex) {
		ereport(FATAL, (errmsg("duckdb worker failed to initialize: %s", duckdb::ErrorData(ex).Message().c_str())));
	}
	PopActiveSnapshot();
	CommitTransactionCommand();

	auto &settings = WorkerInternals::SettingsOf(worker);
	g_arena.page_size = (uint32)std::max(*settings.page_size_kb, 64) * 1024;
	g_arena.page_count = (uint32)std::max(*settings.arena_pages, 1);
	g_arena.seg = dsm_create(ARENA_PAGES_OFFSET + (Size)g_arena.page_size * g_arena.page_count, 0);
	dsm_pin_mapping(g_arena.seg);
	auto *arena = (ArenaHeader *)dsm_segment_address(g_arena.seg);
	arena->page_size = g_arena.page_size;
	arena->page_count = g_arena.page_count;
	g_arena.base = (char *)arena + ARENA_PAGES_OFFSET;
	for (uint32 page = g_arena.page_count; page > 0; page--) {
		g_arena.free_pages.push_back(page - 1);
	}

	SpinLockAcquire(&g_shmem->lock);
	entry->state = WORKER_RUNNING;
	entry->generation++;
	entry->latch = MyLatch;
	entry->arena_handle = dsm_segment_handle(g_arena.seg);
	SpinLockRelease(&g_shmem->lock);
	elog(LOG, "duckdb worker started for database %u", dboid);

	for (;;) {
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1000, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
		if (ConfigReloadPending) {
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		if (g_engine_invalidated) {
			ereport(FATAL, (errmsg("duckdb worker engine was invalidated, restarting")));
		}
		for (auto &s : g_sessions) {
			try {
				PumpSession(*s);
			} catch (std::exception &) {
				DetachSession(*s); // a malformed frame ends that session only
			}
		}
		ReapSessions();
		AdmitSessions();
	}
}

} // namespace

/* ------------------------------------------------------------------------
 * Public API
 * ------------------------------------------------------------------------ */

DuckdbWorker::DuckdbWorker(Settings settings) : settings_(settings) {
	g_worker = this;
}

void
DuckdbWorker::Init() {
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = ShmemRequest;
#else
	ShmemRequest();
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ShmemStartup;
	RegisterXactCallback(BackendXactCallback, nullptr);
	RegisterSubXactCallback(BackendSubXactCallback, nullptr);
}

duckdb::unique_ptr<RemoteResult>
DuckdbWorker::Dispatch(QueryFrame frame) {
	if (!g_shmem) {
		throw duckdb::IOException("the duckdb worker needs the extension in shared_preload_libraries");
	}
	auto session = duckdb::make_uniq<BackendSession>(*this);
	session->Open(frame);
	return duckdb::unique_ptr<RemoteResult>(session.release());
}

void
DuckdbWorker::Main(Oid database_oid) {
	try {
		WorkerMainLoop(*this, database_oid);
	} catch (std::exception &ex) {
		ereport(FATAL, (errmsg("duckdb worker: %s", duckdb::ErrorData(ex).Message().c_str())));
	}
}

bool
DuckdbWorker::GetStats(Stats &stats) {
	if (!g_shmem) {
		return false;
	}
	SpinLockAcquire(&g_shmem->lock);
	WorkerEntry *entry = FindEntry(MyDatabaseId);
	bool found = entry && entry->state == WORKER_RUNNING;
	if (found) {
		stats.pid = entry->pid;
		stats.active_sessions = entry->active_sessions;
		stats.total_sessions = entry->total_sessions;
	}
	SpinLockRelease(&g_shmem->lock);
	return found;
}

void
DuckdbWorker::ConfigureSession(duckdb::Connection &con, const QueryFrame &frame) {
	for (auto &setting : frame.settings) {
		auto result = con.Query("SET " + setting.first + " = " + setting.second.ToSQLString());
		if (result->HasError()) {
			result->ThrowError();
		}
	}
}

std::vector<std::pair<std::string, duckdb::Value>>
DispatchParameters(ParamListInfo params) {
	std::vector<std::pair<std::string, duckdb::Value>> result;
	const int num_params = params ? params->numParams : 0;
	for (int i = 0; i < num_params; i++) {
		ParamExternData *param;
		ParamExternData workspace;
		if (params->paramFetch != NULL) {
			param = params->paramFetch(params, i + 1, false, &workspace);
		} else {
			param = &params->params[i];
		}
		// A parameter without a resolved type is left out, like the in-process
		// path does for parameters the query never references.
		if (!param->isnull && !OidIsValid(param->ptype)) {
			continue;
		}
		duckdb::Value value =
		    param->isnull ? duckdb::Value() : ConvertPostgresParameterToDuckValue(param->value, param->ptype);
		result.emplace_back(std::to_string(i + 1), std::move(value));
	}
	return result;
}

bool
InDuckdbWorker() {
	return g_in_worker;
}

bool
OnWorkerMainThread() {
	return g_in_worker && std::this_thread::get_id() == g_main_thread;
}

bool
HasWorkerSession(duckdb::ClientContext &context) {
	return SessionFor(context) != nullptr;
}

// Any worker thread: one round trip per session at a time; blocks until the
// backend replies or the session is torn down.
std::string
WorkerSessionRequest(duckdb::ClientContext &context, const std::string &request) {
	WorkerSession *s = SessionFor(context);
	if (!s) {
		throw duckdb::IOException("no duckdb worker session for this connection");
	}
	std::unique_lock<std::mutex> lock(s->mu);
	s->cv.wait(lock, [&] { return s->cancelled || s->rpc == WorkerSession::RPC_NONE; });
	if (s->cancelled) {
		throw duckdb::InterruptException();
	}
	s->rpc = WorkerSession::RPC_PENDING;
	s->rpc_reply.clear();
	std::string frame = EncodeRpcFrame(request);
	s->outbox_bytes += frame.size();
	s->outbox.push_back(std::move(frame));
	lock.unlock();
	SetLatch(MyLatch);
	lock.lock();
	s->cv.wait(lock, [&] { return s->rpc == WorkerSession::RPC_REPLIED || s->rpc == WorkerSession::RPC_FAILED; });
	bool failed = s->rpc == WorkerSession::RPC_FAILED;
	std::string reply = std::move(s->rpc_reply);
	s->rpc = WorkerSession::RPC_NONE;
	lock.unlock();
	s->cv.notify_all();
	if (failed) {
		throw duckdb::InterruptException();
	}
	return reply;
}

} // namespace pgddb
