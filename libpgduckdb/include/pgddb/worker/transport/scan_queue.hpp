#pragma once

#include <cstddef>
#include <cstdint>

// Shared scan task queue: a bounded task ring feeding the scan worker pool, plus the
// table of scan-worker latches used to wake idle producers when a task is enqueued. The
// duckdb worker (consumer) enqueues one block-range task per slice; idle scan workers
// publish their latch, then claim tasks for their database. State lives in PostgreSQL
// main shared memory (same address in every process via fork). The PostgreSQL/IPC types
// are confined to the implementation, like page_pool.
namespace pgddb {

// A claimed unit of work: one block range of one registered scan. The scan-registry slot
// is the low 8 bits of scan_id (see ScanRing); generation detects teardown/reuse.
struct ScanRange {
	uint32_t scan_id;
	uint32_t generation;
	uint32_t lo_blk; // inclusive start block (0 = relation start)
	uint32_t hi_blk; // exclusive end block (0 = relation end / no upper bound)
};

// Attach once per process at startup (shmem_startup_hook, main thread); after that a
// ScanQueue handle is a cheap thin binding to the one shared segment.
class ScanQueue {
public:
	ScanQueue() = default;

	static std::size_t ShmemSize();
	static void Init(void *shmem);
	static void Attach(void *shmem);
	static bool Available();

	// Enqueue one block-range task for a registered scan and wake an idle scan worker for
	// its database. Returns false if the scan is gone or the task ring is full.
	bool Enqueue(uint32_t db_oid, uint32_t scan_id, uint32_t generation, uint32_t lo_blk, uint32_t hi_blk);

	// Claim the next queued task for `db_oid`, or return false if none is ready now.
	bool Claim(uint32_t db_oid, ScanRange *out);

	// Publish this worker's latch for `db_oid` so enqueues can wake it; idempotent.
	void PublishWorker(uint32_t db_oid);

	// Release this worker's published latch slot(s); call on worker exit.
	void UnpublishWorker();
};

} // namespace pgddb
