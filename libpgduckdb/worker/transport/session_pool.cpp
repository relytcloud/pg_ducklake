#include "pgddb/worker/transport/session_pool.hpp"

#include "pgddb/worker/transport/session_channel.hpp"

extern "C" {
#include "postgres.h"

#include "port/atomics.h"
#include "storage/spin.h"
}

namespace pgddb {

namespace {

enum SlotState { SLOT_FREE = 0, SLOT_IN_USE = 1 };

struct SessSlot {
	int state;                 // SlotState, guarded by the pool lock
	uint32 generation;         // bumped on Acquire; detects release/re-acquire
	pg_atomic_uint32 attached; // number of attached ends (0..2); slot frees at 0
};

struct SessionPoolHeader {
	slock_t lock;
	int nslots;
};

inline std::size_t
HeaderBytes() {
	return MAXALIGN(sizeof(SessionPoolHeader));
}

inline std::size_t
SlotArrayBytes(int nslots) {
	return MAXALIGN(sizeof(SessSlot) * (std::size_t)nslots);
}

inline std::size_t
RegionBytes() {
	return MAXALIGN(SessionChannel::ChannelRegionBytes());
}

// Process-local pointers into the (identically-mapped) shared region.
SessionPoolHeader *g_hdr = nullptr;
SessSlot *g_slots = nullptr;
char *g_regions = nullptr;

} // namespace

std::size_t
SessionPool::ShmemSize(int nslots) {
	return HeaderBytes() + SlotArrayBytes(nslots) + (std::size_t)nslots * RegionBytes();
}

void
SessionPool::Init(void *shmem, int nslots) {
	auto *hdr = (SessionPoolHeader *)shmem;
	SpinLockInit(&hdr->lock);
	hdr->nslots = nslots;
	auto *slots = (SessSlot *)((char *)shmem + HeaderBytes());
	for (int i = 0; i < nslots; i++) {
		slots[i].state = SLOT_FREE;
		slots[i].generation = 0;
		pg_atomic_init_u32(&slots[i].attached, 0);
	}
}

void
SessionPool::Attach(void *shmem) {
	g_hdr = (SessionPoolHeader *)shmem;
	g_slots = (SessSlot *)((char *)shmem + HeaderBytes());
	g_regions = (char *)shmem + HeaderBytes() + SlotArrayBytes(g_hdr->nslots);
}

bool
SessionPool::Available() {
	return g_hdr != nullptr && g_hdr->nslots > 0;
}

int
SessionPool::NumSlots() {
	return g_hdr ? g_hdr->nslots : 0;
}

int
SessionPool::Acquire() {
	if (!Available())
		return -1;
	int idx = -1;
	SpinLockAcquire(&g_hdr->lock);
	for (int i = 0; i < g_hdr->nslots; i++) {
		if (g_slots[i].state == SLOT_FREE) {
			g_slots[i].state = SLOT_IN_USE;
			g_slots[i].generation++;
			pg_atomic_write_u32(&g_slots[i].attached, 0);
			idx = i;
			break;
		}
	}
	SpinLockRelease(&g_hdr->lock);
	return idx;
}

uint32_t
SessionPool::Generation(int idx) {
	SpinLockAcquire(&g_hdr->lock);
	uint32 g = g_slots[idx].generation;
	SpinLockRelease(&g_hdr->lock);
	return g;
}

char *
SessionPool::ChannelBase(int idx) {
	return g_regions + (std::size_t)idx * RegionBytes();
}

void
SessionPool::AttachEnd(int idx) {
	pg_atomic_fetch_add_u32(&g_slots[idx].attached, 1);
}

bool
SessionPool::TryAttachEnd(int idx, uint32_t generation) {
	SpinLockAcquire(&g_hdr->lock);
	bool ok = g_slots[idx].state == SLOT_IN_USE && g_slots[idx].generation == generation;
	if (ok)
		pg_atomic_fetch_add_u32(&g_slots[idx].attached, 1);
	SpinLockRelease(&g_hdr->lock);
	return ok;
}

void
SessionPool::DetachEnd(int idx) {
	// The end that brings the attach count to zero returns the slot to the pool; until
	// then the rings stay mapped and are never recreated under a live peer. The whole
	// decrement + free decision happens under the pool lock so it is atomic against
	// TryAttachEnd's check + increment (else a detach landing between them could free
	// a slot the worker just attached).
	SpinLockAcquire(&g_hdr->lock);
	uint32 prev = pg_atomic_fetch_sub_u32(&g_slots[idx].attached, 1);
	if (prev == 1)
		g_slots[idx].state = SLOT_FREE;
	SpinLockRelease(&g_hdr->lock);
}

} // namespace pgddb
