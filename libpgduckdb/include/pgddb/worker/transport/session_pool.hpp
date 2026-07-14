#pragma once

#include <cstddef>
#include <cstdint>

// A fixed pool of session slots (one per concurrent dispatched query) in PostgreSQL main shared memory. Each slot
// holds one SessionChannel's region (control + result rings) at a stable address, reused
// across queries instead of allocating a dsm segment per query. A backend Acquires a free
// slot, opens its channel, and hands the slot index to the duckdb worker; the worker
// attaches to the same slot. The slot returns to the pool only after BOTH ends have
// detached (an attach refcount), so its rings are never recreated while still mapped.
namespace pgddb {

class SessionPool {
public:
	SessionPool() = default;

	// --- segment setup (main thread, under the shmem init lock) ---
	static std::size_t ShmemSize(int nslots);
	static void Init(void *shmem, int nslots);
	static void Attach(void *shmem);

	static bool Available();
	static int NumSlots();

	// Backend: claim a free slot (state FREE -> IN_USE, generation bumped), or -1 if
	// all are occupied.
	int Acquire();
	// The slot's current generation (as of Acquire). A pending-session entry carries
	// it so the worker never attaches a slot that was released and re-acquired after
	// the entry was queued (TryAttachEnd validates it).
	uint32_t Generation(int idx);
	// Base address of slot `idx`'s channel region (for SessionChannel::Open/AttachSlot).
	char *ChannelBase(int idx);
	// Mark one end attached (call right after OpenSlot/AttachSlot).
	void AttachEnd(int idx);
	// Worker: attach only if the slot is still IN_USE at `generation`; false means the
	// backend released it (the queued session is stale) and it must not be served.
	bool TryAttachEnd(int idx, uint32_t generation);
	// Mark one end detached (call right after the SessionChannel is destroyed). When the
	// last end detaches, the slot returns to the pool.
	void DetachEnd(int idx);
};

} // namespace pgddb
