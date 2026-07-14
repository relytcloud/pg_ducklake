#include "pgddb/worker/transport/scan_ring.hpp"

#include <cstring>

extern "C" {
#include "postgres.h"

#include "miscadmin.h"
#include "storage/spin.h"
}

namespace pgddb {

namespace {

constexpr int SCAN_POOL_SLOTS = 32; // concurrent scans (slot index encoded in low 8 bits of scan_id)
constexpr int SCAN_SQL_CAP = 16384;
constexpr int SCAN_SNAP_CAP = 8192;
constexpr int SCAN_ERR_CAP = 1024;
constexpr int READY_RING_CAP = 32;
constexpr std::size_t DESC_RESERVE = 4096; // page front reserved for the Arrow descriptor

struct ReadyEntry {
	uint32_t page_index;
	uint32_t desc_len; // >0 => Arrow descriptor at page[0, desc_len), data at page[DESC_RESERVE, ..)
	uint32_t byte_len; // >0 => serialized DataChunk bytes at page[0, byte_len)
};

struct ScanSlot {
	slock_t lock;
	bool in_use;
	uint32_t generation;
	uint32_t scan_id;
	uint32_t db_oid;
	bool count_only;
	bool errored;
	uint32_t where_off; // CTID-predicate splice offset into sql
	uint32_t tasks_total;
	uint32_t tasks_done;
	uint32_t sql_len;
	uint32_t snap_len;
	uint32_t r_head; // next to pop
	uint32_t r_tail; // next to push
	ReadyEntry ring[READY_RING_CAP];
	char sql[SCAN_SQL_CAP];
	char snap[SCAN_SNAP_CAP];
	char err[SCAN_ERR_CAP];
};

struct ScanRingShmem {
	slock_t alloc_lock; // guards next_seq + slot allocation
	uint32_t next_seq;
	ScanSlot slots[SCAN_POOL_SLOTS];
};

ScanRingShmem *g_ring = nullptr;

inline ScanSlot *
SlotOf(uint32_t scan_id) {
	return &g_ring->slots[scan_id & 0xFF];
}

inline bool
SlotMatches(ScanSlot *s, uint32_t scan_id, uint32_t generation) {
	return s->in_use && s->scan_id == scan_id && s->generation == generation;
}

} // namespace

std::size_t
ScanRing::ShmemSize() {
	return sizeof(ScanRingShmem);
}

void
ScanRing::Init(void *shmem) {
	auto *p = (ScanRingShmem *)shmem;
	MemSet(p, 0, sizeof(ScanRingShmem));
	SpinLockInit(&p->alloc_lock);
	p->next_seq = 1;
	for (int i = 0; i < SCAN_POOL_SLOTS; i++) {
		SpinLockInit(&p->slots[i].lock);
	}
}

void
ScanRing::Attach(void *shmem) {
	g_ring = (ScanRingShmem *)shmem;
}

bool
ScanRing::Available() {
	return g_ring != nullptr;
}

std::size_t
ScanRing::DescReserve() {
	return DESC_RESERVE;
}

ScanHandle
ScanRing::Open(uint32_t db_oid, const char *sql, std::size_t sql_len, const char *snap, std::size_t snap_len,
               bool count_only, uint32_t where_off, uint32_t ntasks) {
	if (!g_ring || sql_len > SCAN_SQL_CAP || snap_len > SCAN_SNAP_CAP)
		return {0, 0};

	SpinLockAcquire(&g_ring->alloc_lock);
	int slot = -1;
	for (int i = 0; i < SCAN_POOL_SLOTS; i++) {
		if (!g_ring->slots[i].in_use) {
			slot = i;
			break;
		}
	}
	if (slot < 0) {
		SpinLockRelease(&g_ring->alloc_lock);
		return {0, 0};
	}
	uint32_t seq = g_ring->next_seq++;
	uint32_t scan_id = (seq << 8) | (uint32_t)slot;
	ScanSlot *s = &g_ring->slots[slot];
	// Hold the per-slot lock across the fill: a stale producer from the slot's
	// previous scan may still be reading the slot fields under s->lock.
	SpinLockAcquire(&s->lock);
	s->in_use = true;
	s->generation++;
	s->scan_id = scan_id;
	s->db_oid = db_oid;
	s->count_only = count_only;
	s->errored = false;
	s->where_off = where_off;
	s->tasks_total = ntasks;
	s->tasks_done = 0;
	s->r_head = s->r_tail = 0;
	s->sql_len = (uint32_t)sql_len;
	s->snap_len = (uint32_t)snap_len;
	memcpy(s->sql, sql, sql_len);
	memcpy(s->snap, snap, snap_len);
	uint32_t generation = s->generation;
	SpinLockRelease(&s->lock);
	SpinLockRelease(&g_ring->alloc_lock);
	return {scan_id, generation};
}

int
ScanRing::TryNext(uint32_t scan_id, uint32_t *page_index, uint32_t *desc_len, uint32_t *byte_len, char *errbuf,
                  std::size_t errcap) {
	ScanSlot *s = SlotOf(scan_id);
	SpinLockAcquire(&s->lock);
	int rc;
	if (!s->in_use || s->scan_id != scan_id) {
		rc = 0; // scan gone -> treat as finished
	} else if (s->errored) {
		std::size_t n = s->err[0] ? strlen(s->err) : 0;
		if (n >= errcap)
			n = errcap ? errcap - 1 : 0;
		if (errcap) {
			memcpy(errbuf, s->err, n);
			errbuf[n] = '\0';
		}
		rc = -1;
	} else if (s->r_head < s->r_tail) {
		ReadyEntry *e = &s->ring[s->r_head % READY_RING_CAP];
		*page_index = e->page_index;
		*desc_len = e->desc_len;
		*byte_len = e->byte_len;
		s->r_head++;
		rc = 1;
	} else if (s->tasks_done >= s->tasks_total) {
		rc = 0; // drained and all tasks done
	} else {
		rc = 2; // nothing ready yet
	}
	SpinLockRelease(&s->lock);
	return rc;
}

void
ScanRing::Close(uint32_t scan_id, void (*release_page)(int)) {
	ScanSlot *s = SlotOf(scan_id);
	SpinLockAcquire(&s->lock);
	if (s->in_use && s->scan_id == scan_id) {
		while (s->r_head < s->r_tail) {
			ReadyEntry *e = &s->ring[s->r_head % READY_RING_CAP];
			if (release_page)
				release_page((int)e->page_index);
			s->r_head++;
		}
		s->generation++; // in-flight producers stop on their next alive check
		s->in_use = false;
		s->scan_id = 0;
		s->errored = false;
		s->tasks_total = s->tasks_done = 0;
		s->r_head = s->r_tail = 0;
	}
	SpinLockRelease(&s->lock);
}

bool
ScanRing::GetInputs(uint32_t scan_id, uint32_t generation, char *sql, std::size_t sqlcap, std::size_t *sqllen,
                    char *snap, std::size_t snapcap, std::size_t *snaplen, bool *count_only, uint32_t *where_off) {
	ScanSlot *s = SlotOf(scan_id);
	SpinLockAcquire(&s->lock);
	bool ok = SlotMatches(s, scan_id, generation) && s->sql_len <= sqlcap && s->snap_len <= snapcap;
	if (ok) {
		*sqllen = s->sql_len;
		*snaplen = s->snap_len;
		memcpy(sql, s->sql, s->sql_len);
		memcpy(snap, s->snap, s->snap_len);
		*count_only = s->count_only;
		*where_off = s->where_off;
	}
	SpinLockRelease(&s->lock);
	return ok;
}

bool
ScanRing::Alive(uint32_t scan_id, uint32_t generation) {
	ScanSlot *s = SlotOf(scan_id);
	SpinLockAcquire(&s->lock);
	bool ok = SlotMatches(s, scan_id, generation);
	SpinLockRelease(&s->lock);
	return ok;
}

bool
ScanRing::Push(uint32_t scan_id, uint32_t generation, uint32_t page_index, uint32_t desc_len, uint32_t byte_len) {
	ScanSlot *s = SlotOf(scan_id);
	for (;;) {
		SpinLockAcquire(&s->lock);
		if (!SlotMatches(s, scan_id, generation) || s->errored) {
			SpinLockRelease(&s->lock);
			return false;
		}
		if ((s->r_tail - s->r_head) < READY_RING_CAP) {
			ReadyEntry *e = &s->ring[s->r_tail % READY_RING_CAP];
			e->page_index = page_index;
			e->desc_len = desc_len;
			e->byte_len = byte_len;
			s->r_tail++;
			SpinLockRelease(&s->lock);
			return true;
		}
		SpinLockRelease(&s->lock); // ring full -> wait for the consumer to drain
		CHECK_FOR_INTERRUPTS();
		pg_usleep(500);
	}
}

void
ScanRing::TaskDone(uint32_t scan_id, uint32_t generation) {
	ScanSlot *s = SlotOf(scan_id);
	SpinLockAcquire(&s->lock);
	if (SlotMatches(s, scan_id, generation))
		s->tasks_done++;
	SpinLockRelease(&s->lock);
}

void
ScanRing::SetError(uint32_t scan_id, uint32_t generation, const char *msg) {
	ScanSlot *s = SlotOf(scan_id);
	SpinLockAcquire(&s->lock);
	if (SlotMatches(s, scan_id, generation) && !s->errored) {
		std::size_t n = strlen(msg);
		if (n >= SCAN_ERR_CAP)
			n = SCAN_ERR_CAP - 1;
		memcpy(s->err, msg, n);
		s->err[n] = '\0';
		s->errored = true;
	}
	SpinLockRelease(&s->lock);
}

} // namespace pgddb
