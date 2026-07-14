#pragma once

#include <cstddef>
#include <cstdint>

// Per-scan ready-page rings: a table of scan tasks keyed by scan id, each holding the
// scan's inputs (deparsed SQL + serialized snapshot), task counters, and a ready-ring of
// produced pages. The duckdb worker (consumer) opens a scan and drains its ring; the
// scan workers (producers) push pages into it, keyed by (scan_id,
// generation). State lives in PostgreSQL main shared memory (same address in every
// process via fork), so pages are shared by index. The PostgreSQL/IPC types are confined
// to the implementation, like page_pool.
namespace pgddb {

// Handle to an opened scan. scan_id == 0 means open failed (no slot / inputs too large);
// the low 8 bits of scan_id encode the registry slot. generation detects teardown/reuse.
struct ScanHandle {
	uint32_t scan_id;
	uint32_t generation;
};

// Attach once per process at startup (shmem_startup_hook, main thread); after that a
// ScanRing handle is a cheap thin binding to the one shared segment.
class ScanRing {
public:
	ScanRing() = default;

	static std::size_t ShmemSize();
	static void Init(void *shmem);
	static void Attach(void *shmem);
	static bool Available();

	// Bytes reserved at the front of each pool page for an Arrow batch descriptor; the
	// producer writes Arrow data after this offset and the descriptor into [0, desc_len).
	static std::size_t DescReserve();

	// --- Consumer (duckdb worker) ---

	// Open a scan and copy its inputs (deparsed SQL + serialized snapshot) into a slot.
	// `ntasks` is how many block-range tasks will be enqueued. Returns a handle with a
	// non-zero scan_id, or scan_id == 0 if no slot is free or the inputs do not fit.
	ScanHandle Open(uint32_t db_oid, const char *sql, std::size_t sql_len, const char *snap, std::size_t snap_len,
	                bool count_only, uint32_t where_off, uint32_t ntasks);

	// Try to pop the next ready chunk. Returns 1 (ready: fills page_index and exactly one
	// of desc_len>0 [Arrow] / byte_len>0 [serialized]), 0 (scan finished and drained),
	// -1 (error: message copied into errbuf), or 2 (nothing ready yet -- caller waits).
	int TryNext(uint32_t scan_id, uint32_t *page_index, uint32_t *desc_len, uint32_t *byte_len, char *errbuf,
	            std::size_t errcap);

	// Tear a scan down: bump its generation so in-flight producers stop, and release any
	// pages still queued in its ready-ring through `release_page`.
	void Close(uint32_t scan_id, void (*release_page)(int));

	// --- Producer (scan worker) ---

	// Copy a scan's inputs into caller buffers; false if the slot was torn down.
	// `where_off` is the CTID-predicate splice offset into `sql` (see RemoteScanInfo).
	bool GetInputs(uint32_t scan_id, uint32_t generation, char *sql, std::size_t sqlcap, std::size_t *sqllen,
	               char *snap, std::size_t snapcap, std::size_t *snaplen, bool *count_only, uint32_t *where_off);

	// True while the scan is still alive (generation matches).
	bool Alive(uint32_t scan_id, uint32_t generation);

	// Push a ready chunk into the scan's ready-ring. Blocks while the ring is full (until
	// the consumer drains or the scan is torn down). Returns false if the scan was torn
	// down (caller must free the page itself); the chunk is dropped in that case.
	bool Push(uint32_t scan_id, uint32_t generation, uint32_t page_index, uint32_t desc_len, uint32_t byte_len);

	// Mark this task finished; the last task of a scan marks the scan finished so the
	// consumer's drain returns end-of-scan once the ring empties.
	void TaskDone(uint32_t scan_id, uint32_t generation);

	// Record a producer error for the scan; the consumer's next drain throws it.
	void SetError(uint32_t scan_id, uint32_t generation, const char *msg);
};

} // namespace pgddb
