#include "pgddb/worker/transport/scan_queue.hpp"

extern "C" {
#include "postgres.h"

#include "miscadmin.h"
#include "storage/latch.h"
#include "storage/spin.h"
}

namespace pgddb {

namespace {

constexpr int TASK_RING_CAP = 256;
constexpr int POOL_WORKER_SLOTS = 32;

struct TaskEntry {
	bool valid;
	uint32_t db_oid;
	uint32_t scan_id;
	uint32_t generation;
	uint32_t lo_blk;
	uint32_t hi_blk;
};

struct PoolWorkerSlot {
	bool in_use;
	uint32_t db_oid;
	Latch *latch;
	int pid;
};

struct ScanQueueShmem {
	slock_t qlock; // guards task ring + worker table
	uint32_t q_head;
	uint32_t q_tail;
	TaskEntry tasks[TASK_RING_CAP];
	PoolWorkerSlot workers[POOL_WORKER_SLOTS];
};

ScanQueueShmem *g_queue = nullptr;

} // namespace

std::size_t
ScanQueue::ShmemSize() {
	return sizeof(ScanQueueShmem);
}

void
ScanQueue::Init(void *shmem) {
	auto *p = (ScanQueueShmem *)shmem;
	MemSet(p, 0, sizeof(ScanQueueShmem));
	SpinLockInit(&p->qlock);
}

void
ScanQueue::Attach(void *shmem) {
	g_queue = (ScanQueueShmem *)shmem;
}

bool
ScanQueue::Available() {
	return g_queue != nullptr;
}

bool
ScanQueue::Enqueue(uint32_t db_oid, uint32_t scan_id, uint32_t generation, uint32_t lo_blk, uint32_t hi_blk) {
	if (!g_queue)
		return false;
	bool ok = false;
	Latch *wake[POOL_WORKER_SLOTS];
	int nwake = 0;
	SpinLockAcquire(&g_queue->qlock);
	if ((g_queue->q_tail - g_queue->q_head) < TASK_RING_CAP) {
		TaskEntry *e = &g_queue->tasks[g_queue->q_tail % TASK_RING_CAP];
		e->valid = true;
		e->db_oid = db_oid;
		e->scan_id = scan_id;
		e->generation = generation;
		e->lo_blk = lo_blk;
		e->hi_blk = hi_blk;
		g_queue->q_tail++;
		ok = true;
	}
	// Collect this database's idle-worker latches; SetLatch (a syscall) after release.
	if (ok) {
		for (int i = 0; i < POOL_WORKER_SLOTS; i++) {
			if (g_queue->workers[i].in_use && g_queue->workers[i].db_oid == db_oid && g_queue->workers[i].latch)
				wake[nwake++] = g_queue->workers[i].latch;
		}
	}
	SpinLockRelease(&g_queue->qlock);
	for (int i = 0; i < nwake; i++)
		SetLatch(wake[i]);
	return ok;
}

bool
ScanQueue::Claim(uint32_t db_oid, ScanRange *out) {
	if (!g_queue)
		return false;
	bool found = false;
	SpinLockAcquire(&g_queue->qlock);
	// Wrap-safe: head/tail are monotonic uint32 counters, so compare with != only.
	for (uint32_t i = g_queue->q_head; i != g_queue->q_tail; i++) {
		TaskEntry *e = &g_queue->tasks[i % TASK_RING_CAP];
		if (e->valid && e->db_oid == db_oid) {
			out->scan_id = e->scan_id;
			out->generation = e->generation;
			out->lo_blk = e->lo_blk;
			out->hi_blk = e->hi_blk;
			e->valid = false;
			found = true;
			break;
		}
	}
	while (g_queue->q_head != g_queue->q_tail && !g_queue->tasks[g_queue->q_head % TASK_RING_CAP].valid)
		g_queue->q_head++;
	SpinLockRelease(&g_queue->qlock);
	return found;
}

void
ScanQueue::PublishWorker(uint32_t db_oid) {
	if (!g_queue)
		return;
	SpinLockAcquire(&g_queue->qlock);
	int free_idx = -1;
	for (int i = 0; i < POOL_WORKER_SLOTS; i++) {
		if (g_queue->workers[i].in_use && g_queue->workers[i].pid == MyProcPid) {
			free_idx = i;
			break;
		}
		if (free_idx < 0 && !g_queue->workers[i].in_use)
			free_idx = i;
	}
	if (free_idx >= 0) {
		g_queue->workers[free_idx].in_use = true;
		g_queue->workers[free_idx].db_oid = db_oid;
		g_queue->workers[free_idx].latch = MyLatch;
		g_queue->workers[free_idx].pid = MyProcPid;
	}
	SpinLockRelease(&g_queue->qlock);
}

void
ScanQueue::UnpublishWorker() {
	if (!g_queue)
		return;
	SpinLockAcquire(&g_queue->qlock);
	for (int i = 0; i < POOL_WORKER_SLOTS; i++) {
		if (g_queue->workers[i].in_use && g_queue->workers[i].pid == MyProcPid) {
			g_queue->workers[i].in_use = false;
			g_queue->workers[i].latch = nullptr;
			g_queue->workers[i].pid = 0;
		}
	}
	SpinLockRelease(&g_queue->qlock);
}

} // namespace pgddb
