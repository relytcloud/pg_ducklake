#pragma once

#include <cstddef>

// A global fixed-size pool of equal-size shared-memory pages (scan pages): the zero-copy
// transport area for Arrow record batches. It lives in PostgreSQL main shared memory and
// is mapped at the same address in every backend and background worker via fork, so a
// page's address is valid in every process; a scan page travels between processes by its
// integer index (carried in a scan ring). A spinlock-guarded free stack hands pages out
// and back: a scan worker (or the backend) Acquires a page, a DuckDB thread Releases it
// once the chunk referencing it is dropped.
//
// Attach once per process at startup (shmem_startup_hook, main thread); after that a
// PagePool handle is a cheap thin binding to the one shared segment -- many handles may
// coexist (one per DuckDB thread) and Acquire/Release are thread-safe via the spinlock.
namespace pgddb {

// A scan page handed out by the pool. `index` identifies it across processes (it is what
// travels in a scan ring); `data` is its address in this process.
struct PageSlot {
	int index;
	char *data;
};

class PagePool {
public:
	// Bind a handle to the process-global pool (cheap; valid only after Attach). All
	// handles in a process refer to the one shared segment.
	PagePool() = default;

	// --- process / segment setup (main thread, under the shmem init lock) ---
	static std::size_t ShmemSize(int total_pages, int page_size);
	// Initialize a freshly-created segment (first process only).
	static void Init(void *shmem, int total_pages, int page_size);
	// Point this process at the segment and build its page-slot table. Call in every
	// process at startup, after Init has run in the first.
	static void Attach(void *shmem);

	// --- handle API ---
	bool Available() const;
	int PageSize() const;
	int TotalPages() const;

	// Acquire a free scan page, or nullptr if the pool is momentarily empty.
	PageSlot *Acquire();
	// Return a scan page to the pool.
	void Release(PageSlot *slot);
	// The slot for a page index (to release a page drained from a scan ring by index).
	PageSlot *Slot(int index);
};

} // namespace pgddb
