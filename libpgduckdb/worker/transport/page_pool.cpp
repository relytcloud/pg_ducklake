#include "pgddb/worker/transport/page_pool.hpp"

#include <vector>

extern "C" {
#include "postgres.h"

#include "storage/spin.h"
}

namespace pgddb {

namespace {

// Shared-memory header; the free stack (int[total_pages]) and the pages region
// (total_pages * page_size bytes) follow it, both MAXALIGN'd.
struct ArrowPagePool {
	slock_t lock;
	int total_pages;
	int page_size;
	int free_top; // number of free entries currently in free_stack[0..free_top)
};

inline std::size_t
HeaderBytes() {
	return MAXALIGN(sizeof(ArrowPagePool));
}

inline std::size_t
FreeStackBytes(int total_pages) {
	return MAXALIGN(sizeof(int) * (std::size_t)total_pages);
}

int *
FreeStack(ArrowPagePool *pool) {
	return (int *)((char *)pool + HeaderBytes());
}

char *
PagesBase(ArrowPagePool *pool) {
	return (char *)pool + HeaderBytes() + FreeStackBytes(pool->total_pages);
}

// Process-local: pointer into the (identically-mapped) shared region, plus a per-page
// PageSlot table so Acquire can hand out stable slot pointers.
ArrowPagePool *g_pool = nullptr;
std::vector<PageSlot> g_slots;

} // namespace

std::size_t
PagePool::ShmemSize(int total_pages, int page_size) {
	return HeaderBytes() + FreeStackBytes(total_pages) + (std::size_t)total_pages * (std::size_t)page_size;
}

void
PagePool::Init(void *shmem, int total_pages, int page_size) {
	auto *pool = (ArrowPagePool *)shmem;
	SpinLockInit(&pool->lock);
	pool->total_pages = total_pages;
	pool->page_size = page_size;
	pool->free_top = total_pages;
	int *stack = FreeStack(pool);
	for (int i = 0; i < total_pages; i++) {
		stack[i] = i; // every page starts free
	}
}

void
PagePool::Attach(void *shmem) {
	g_pool = (ArrowPagePool *)shmem;
	g_slots.clear();
	g_slots.reserve(g_pool->total_pages);
	for (int i = 0; i < g_pool->total_pages; i++) {
		g_slots.push_back(PageSlot {i, PagesBase(g_pool) + (std::size_t)i * (std::size_t)g_pool->page_size});
	}
}

bool
PagePool::Available() const {
	return g_pool != nullptr && g_pool->total_pages > 0;
}

int
PagePool::PageSize() const {
	return g_pool ? g_pool->page_size : 0;
}

int
PagePool::TotalPages() const {
	return g_pool ? g_pool->total_pages : 0;
}

PageSlot *
PagePool::Acquire() {
	if (!Available()) {
		return nullptr;
	}
	int idx = -1;
	SpinLockAcquire(&g_pool->lock);
	if (g_pool->free_top > 0) {
		idx = FreeStack(g_pool)[--g_pool->free_top];
	}
	SpinLockRelease(&g_pool->lock);
	return idx < 0 ? nullptr : &g_slots[idx];
}

void
PagePool::Release(PageSlot *slot) {
	if (slot == nullptr || slot->index < 0 || !Available() || slot->index >= g_pool->total_pages) {
		return;
	}
	SpinLockAcquire(&g_pool->lock);
	// Guard against a double release overflowing the free stack and corrupting the pages
	// region (a stray extra release is dropped rather than written past the end).
	if (g_pool->free_top < g_pool->total_pages) {
		FreeStack(g_pool)[g_pool->free_top++] = slot->index;
	}
	SpinLockRelease(&g_pool->lock);
}

PageSlot *
PagePool::Slot(int index) {
	if (index < 0 || !Available() || index >= g_pool->total_pages) {
		return nullptr;
	}
	return &g_slots[index];
}

} // namespace pgddb
