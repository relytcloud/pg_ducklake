#include "pgducklake/direct_insert/native_writer_queue.hpp"

#include "pgducklake/guc.hpp"

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>

extern "C" {
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "miscadmin.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/lwlocknames.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#if PG_VERSION_NUM >= 180000
#include "utils/wait_classes.h"
#else
#include "utils/wait_event.h"
#endif
}

namespace pgducklake {

namespace {

/* A full queue is a harmless hint miss: that writer uses the normal retry path. */
constexpr uint32_t MAX_RESERVATIONS = 256;
constexpr long OWNER_CHECK_INTERVAL_MS = 100;
constexpr uint32_t INVALID_SLOT = std::numeric_limits<uint32_t>::max();

struct CatalogKey {
	Oid database_oid;
	Oid snapshot_relation_oid;
};

struct OwnerIdentity {
	int32_t proc_number;
	uint32_t vxid_id;
	uint32_t lxid;
	uint32_t xid;
	pid_t pid;
};

struct ReservationSlot {
	bool in_use;
	bool valid;
	bool committed;
	bool row_assigned;
	bool row_count_known;
	CatalogKey catalog;
	uint64_t ticket;
	OwnerIdentity owner;
	uint32_t owner_command_id;
	uint64_t table_id;
	uint64_t candidate_snapshot_id;
	uint64_t candidate_row_id;
	uint64_t row_count;
	uint64_t queue_wait_blocker_ticket;
	TimestampTz queue_wait_deadline;
};

struct NativeWriterQueueShmemStruct {
	slock_t lock;
	ConditionVariable changed;
	uint64_t next_ticket;
	ReservationSlot slots[MAX_RESERVATIONS];
};

struct LocalReservation {
	NativeWriterReservation reservation;
	bool published;
};

struct BlockingReservation {
	bool present;
	CatalogKey catalog;
	uint64_t ticket;
	OwnerIdentity owner;
};

NativeWriterQueueShmemStruct *QueueShmem = nullptr;
LocalReservation Local = {{INVALID_SLOT, 0, false}, false};
pid_t callback_owner_pid = 0;

#if PG_VERSION_NUM >= 150000
shmem_request_hook_type prev_queue_shmem_request_hook = nullptr;
#endif
shmem_startup_hook_type prev_queue_shmem_startup_hook = nullptr;

uint32_t
QueueCapacity() {
	return static_cast<uint32_t>(native_writer_reservation_queue_capacity);
}

bool
SameCatalog(const CatalogKey &left, const CatalogKey &right) {
	return left.database_oid == right.database_oid && left.snapshot_relation_oid == right.snapshot_relation_oid;
}

bool
SameOwner(const OwnerIdentity &left, const OwnerIdentity &right) {
	return left.proc_number == right.proc_number && left.vxid_id == right.vxid_id && left.lxid == right.lxid &&
	       left.xid == right.xid && left.pid == right.pid;
}

CatalogKey
CurrentCatalogKey() {
	Oid namespace_oid = get_namespace_oid("ducklake", false);
	Oid snapshot_oid = get_relname_relid("ducklake_snapshot", namespace_oid);
	if (!OidIsValid(snapshot_oid)) {
		return {InvalidOid, InvalidOid};
	}
	return {MyDatabaseId, snapshot_oid};
}

OwnerIdentity
CurrentOwner(uint32_t xid) {
#if PG_VERSION_NUM >= 170000
	return {static_cast<int32_t>(GetNumberFromPGProc(MyProc)), static_cast<uint32_t>(MyProc->vxid.procNumber),
	        static_cast<uint32_t>(MyProc->vxid.lxid), xid, MyProcPid};
#else
	return {static_cast<int32_t>(MyProc->pgprocno), static_cast<uint32_t>(MyProc->backendId),
	        static_cast<uint32_t>(MyProc->lxid), xid, MyProcPid};
#endif
}

ReservationSlot *
FindSlot(const NativeWriterReservation &reservation) {
	if (!QueueShmem || !reservation.active || reservation.slot >= QueueCapacity()) {
		return nullptr;
	}
	auto &slot = QueueShmem->slots[reservation.slot];
	if (!slot.in_use || slot.ticket != reservation.ticket) {
		return nullptr;
	}
	return &slot;
}

/* Every later candidate depends on its predecessors' snapshot and row ranges. */
void
InvalidateFrom(const CatalogKey &catalog, uint64_t ticket) {
	for (uint32_t i = 0; i < QueueCapacity(); i++) {
		auto &slot = QueueShmem->slots[i];
		if (slot.in_use && slot.valid && SameCatalog(slot.catalog, catalog) && slot.ticket >= ticket) {
			slot.valid = false;
		}
	}
}

void
InvalidateCatalog(const CatalogKey &catalog) {
	InvalidateFrom(catalog, 0);
}

void
ReclaimInvalidSlots() {
	for (uint32_t i = 0; i < QueueCapacity(); i++) {
		auto &slot = QueueShmem->slots[i];
		if (slot.in_use && !slot.valid) {
			MemSet(&slot, 0, sizeof(slot));
		}
	}
}

bool
ReclaimOldestCommittedAnchor() {
	ReservationSlot *oldest = nullptr;
	for (uint32_t i = 0; i < QueueCapacity(); i++) {
		auto &slot = QueueShmem->slots[i];
		if (slot.in_use && slot.committed && (!oldest || slot.ticket < oldest->ticket)) {
			oldest = &slot;
		}
	}
	if (!oldest) {
		return false;
	}
	MemSet(oldest, 0, sizeof(*oldest));
	return true;
}

ReservationSlot *
FirstPendingSlot(const CatalogKey &catalog) {
	ReservationSlot *first = nullptr;
	for (uint32_t i = 0; i < QueueCapacity(); i++) {
		auto &slot = QueueShmem->slots[i];
		if (slot.in_use && slot.valid && !slot.committed && SameCatalog(slot.catalog, catalog) &&
		    (!first || slot.ticket < first->ticket)) {
			first = &slot;
		}
	}
	return first;
}

ReservationSlot *
LastValidSlot(const CatalogKey &catalog, uint64_t table_id, bool same_table) {
	ReservationSlot *last = nullptr;
	for (uint32_t i = 0; i < QueueCapacity(); i++) {
		auto &slot = QueueShmem->slots[i];
		if (!slot.in_use || !slot.valid || !SameCatalog(slot.catalog, catalog) ||
		    (same_table && slot.table_id != table_id)) {
			continue;
		}
		if (!last || slot.ticket > last->ticket) {
			last = &slot;
		}
	}
	return last;
}

ReservationSlot *
LastPriorSlot(const ReservationSlot &current, bool same_table) {
	ReservationSlot *last = nullptr;
	for (uint32_t i = 0; i < QueueCapacity(); i++) {
		auto &slot = QueueShmem->slots[i];
		if (!slot.in_use || !slot.valid || !SameCatalog(slot.catalog, current.catalog) ||
		    slot.ticket >= current.ticket || (same_table && slot.table_id != current.table_id)) {
			continue;
		}
		if (!last || slot.ticket > last->ticket) {
			last = &slot;
		}
	}
	return last;
}

BlockingReservation
FindBlocker(const ReservationSlot &current, bool publication_wait) {
	ReservationSlot *blocker = nullptr;
	for (uint32_t i = 0; i < QueueCapacity(); i++) {
		auto &slot = QueueShmem->slots[i];
		if (!slot.in_use || !slot.valid || slot.committed || !SameCatalog(slot.catalog, current.catalog) ||
		    slot.ticket >= current.ticket) {
			continue;
		}
		if (!publication_wait && (slot.table_id != current.table_id || slot.row_count_known)) {
			continue;
		}
		if (!blocker || slot.ticket < blocker->ticket) {
			blocker = &slot;
		}
	}
	if (!blocker) {
		return {};
	}
	return {true, blocker->catalog, blocker->ticket, blocker->owner};
}

bool
OwnerTransactionIsActive(const OwnerIdentity &owner) {
	bool active = false;
	LWLockAcquire(ProcArrayLock, LW_SHARED);
	if (owner.proc_number >= 0 && static_cast<uint32_t>(owner.proc_number) < ProcGlobal->allProcCount) {
		PGPROC *proc = GetPGProcByNumber(owner.proc_number);
#if PG_VERSION_NUM >= 170000
		active = proc->pid == owner.pid && static_cast<uint32_t>(proc->vxid.procNumber) == owner.vxid_id &&
		         static_cast<uint32_t>(proc->vxid.lxid) == owner.lxid;
#else
		active = proc->pid == owner.pid && static_cast<uint32_t>(proc->backendId) == owner.vxid_id &&
		         static_cast<uint32_t>(proc->lxid) == owner.lxid;
#endif
	}
	LWLockRelease(ProcArrayLock);
	return active;
}

bool
ReclaimInactiveOwner(const BlockingReservation &candidate) {
	if (!candidate.present || OwnerTransactionIsActive(candidate.owner)) {
		return false;
	}

	bool reclaimed = false;
	SpinLockAcquire(&QueueShmem->lock);
	for (uint32_t i = 0; i < QueueCapacity(); i++) {
		auto &slot = QueueShmem->slots[i];
		if (slot.in_use && slot.ticket == candidate.ticket && SameCatalog(slot.catalog, candidate.catalog) &&
		    SameOwner(slot.owner, candidate.owner)) {
			InvalidateFrom(slot.catalog, slot.ticket);
			MemSet(&slot, 0, sizeof(slot));
			reclaimed = true;
			break;
		}
	}
	SpinLockRelease(&QueueShmem->lock);
	if (reclaimed) {
		ConditionVariableBroadcast(&QueueShmem->changed);
	}
	return reclaimed;
}

void
ReclaimInactiveOwners() {
	BlockingReservation candidates[MAX_RESERVATIONS];
	uint32_t count = 0;
	SpinLockAcquire(&QueueShmem->lock);
	for (uint32_t i = 0; i < QueueCapacity(); i++) {
		auto &slot = QueueShmem->slots[i];
		if (slot.in_use && slot.valid && !slot.committed) {
			candidates[count++] = {true, slot.catalog, slot.ticket, slot.owner};
		}
	}
	SpinLockRelease(&QueueShmem->lock);

	for (uint32_t i = 0; i < count; i++) {
		ReclaimInactiveOwner(candidates[i]);
	}
}

long
QueueWaitBudgetMs() {
	long wait_cap_ms = native_writer_reservation_queue_wait_ms;
	if (wait_cap_ms <= 0 || native_writer_max_retry_count <= 0 || native_writer_retry_wait_ms <= 0) {
		return 0;
	}

	double delay = native_writer_retry_wait_ms;
	double total = 0;
	for (int retry = 0; retry < native_writer_max_retry_count; retry++) {
		total += delay;
		if (total >= wait_cap_ms) {
			return wait_cap_ms;
		}
		if (delay >= wait_cap_ms / native_writer_retry_backoff) {
			delay = wait_cap_ms;
		} else {
			delay *= native_writer_retry_backoff;
		}
	}
	return static_cast<long>(std::ceil(total));
}

void
AbandonReservation(const NativeWriterReservation &reservation) {
	SpinLockAcquire(&QueueShmem->lock);
	ReservationSlot *slot = FindSlot(reservation);
	if (slot) {
		InvalidateFrom(slot->catalog, slot->ticket);
		MemSet(slot, 0, sizeof(*slot));
	}
	SpinLockRelease(&QueueShmem->lock);
	ConditionVariableBroadcast(&QueueShmem->changed);
}

/* Keep a successfully published ticket until top-level commit so its successor
 * cannot read metadata before the snapshot claim becomes visible. */
void
RemoveLocalReservation(bool committed) {
	if (!QueueShmem || !Local.reservation.active) {
		Local = {{INVALID_SLOT, 0, false}, false};
		return;
	}

	SpinLockAcquire(&QueueShmem->lock);
	ReservationSlot *slot = FindSlot(Local.reservation);
	if (slot) {
		if (committed && Local.published && slot->valid) {
			slot->committed = true;
			slot->queue_wait_blocker_ticket = 0;
			slot->queue_wait_deadline = 0;
			/* A committed anchor preserves the metadata-read/registration frontier.
			 * This anchor now includes every older same-table row range. */
			for (uint32_t i = 0; i < QueueCapacity(); i++) {
				auto &prior = QueueShmem->slots[i];
				if (&prior != slot && prior.in_use && prior.valid && prior.committed &&
				    SameCatalog(prior.catalog, slot->catalog) && prior.table_id == slot->table_id &&
				    prior.ticket < slot->ticket) {
					MemSet(&prior, 0, sizeof(prior));
				}
			}
		} else {
			InvalidateFrom(slot->catalog, slot->ticket);
			MemSet(slot, 0, sizeof(*slot));
		}
	}
	SpinLockRelease(&QueueShmem->lock);
	ConditionVariableBroadcast(&QueueShmem->changed);
	Local = {{INVALID_SLOT, 0, false}, false};
}

void
QueueXactCallback(XactEvent event, void *) {
	switch (event) {
	case XACT_EVENT_COMMIT:
	case XACT_EVENT_PARALLEL_COMMIT:
		RemoveLocalReservation(true);
		break;
	case XACT_EVENT_ABORT:
	case XACT_EVENT_PARALLEL_ABORT:
		RemoveLocalReservation(false);
		break;
	case XACT_EVENT_PRE_COMMIT:
	case XACT_EVENT_PARALLEL_PRE_COMMIT:
	case XACT_EVENT_PREPARE:
	case XACT_EVENT_PRE_PREPARE:
		break;
	}
}

void
QueueBackendExit(int, Datum) {
	RemoveLocalReservation(false);
}

void
EnsureBackendCallbacksRegistered() {
	if (callback_owner_pid == MyProcPid) {
		return;
	}
	callback_owner_pid = MyProcPid;
	RegisterXactCallback(QueueXactCallback, nullptr);
	before_shmem_exit(QueueBackendExit, (Datum)0);
}

void
QueueShmemRequest() {
#if PG_VERSION_NUM >= 150000
	if (prev_queue_shmem_request_hook) {
		prev_queue_shmem_request_hook();
	}
#endif
	RequestAddinShmemSpace(sizeof(NativeWriterQueueShmemStruct));
}

void
QueueShmemStartup() {
	if (prev_queue_shmem_startup_hook) {
		prev_queue_shmem_startup_hook();
	}

	bool found;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	QueueShmem = (NativeWriterQueueShmemStruct *)ShmemInitStruct("DuckLakeNativeWriterQueue",
	                                                             sizeof(NativeWriterQueueShmemStruct), &found);
	if (!found) {
		MemSet(QueueShmem, 0, sizeof(NativeWriterQueueShmemStruct));
		SpinLockInit(&QueueShmem->lock);
		ConditionVariableInit(&QueueShmem->changed);
		QueueShmem->next_ticket = 1;
	}
	LWLockRelease(AddinShmemInitLock);
}

bool
WaitUntilReadyImpl(const NativeWriterReservation &reservation, bool publication_wait) {
	long wait_budget_ms = QueueWaitBudgetMs();
	if (wait_budget_ms <= 0) {
		AbandonReservation(reservation);
		return false;
	}
	ConditionVariablePrepareToSleep(&QueueShmem->changed);
	bool check_owner = false;
	for (;;) {
		BlockingReservation blocker = {};
		TimestampTz deadline = 0;
		bool ready = false;

		SpinLockAcquire(&QueueShmem->lock);
		ReservationSlot *slot = FindSlot(reservation);
		if (!slot || !slot->valid) {
			SpinLockRelease(&QueueShmem->lock);
			ConditionVariableCancelSleep();
			return false;
		}
		if (publication_wait) {
			ReservationSlot *first = FirstPendingSlot(slot->catalog);
			ready = first && first->ticket == slot->ticket;
		} else {
			ReservationSlot *previous = LastPriorSlot(*slot, true);
			if (!previous) {
				ready = slot->row_assigned;
			} else if (previous->row_assigned && previous->row_count_known) {
				if (previous->candidate_row_id <=
				    static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - previous->row_count) {
					slot->candidate_row_id = previous->candidate_row_id + previous->row_count;
					slot->row_assigned = true;
					ready = true;
				} else {
					InvalidateFrom(slot->catalog, slot->ticket);
				}
			}
		}
		if (!ready) {
			blocker = FindBlocker(*slot, publication_wait);
			/* A new blocking predecessor is queue progress, so give it a fresh
			 * bounded interval. A stalled blocker cannot extend its own deadline. */
			if (slot->queue_wait_deadline == 0 ||
			    (blocker.present && slot->queue_wait_blocker_ticket != blocker.ticket)) {
				slot->queue_wait_blocker_ticket = blocker.present ? blocker.ticket : 0;
				slot->queue_wait_deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), wait_budget_ms);
			}
			deadline = slot->queue_wait_deadline;
		}
		SpinLockRelease(&QueueShmem->lock);

		if (ready) {
			ConditionVariableCancelSleep();
			return true;
		}
		if (check_owner && ReclaimInactiveOwner(blocker)) {
			check_owner = false;
			continue;
		}

		long remaining = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);
		if (remaining <= 0) {
			ConditionVariableCancelSleep();
			AbandonReservation(reservation);
			return false;
		}
		check_owner = ConditionVariableTimedSleep(&QueueShmem->changed, Min(remaining, OWNER_CHECK_INTERVAL_MS),
		                                          PG_WAIT_EXTENSION);
	}
}

bool
WaitUntilReady(const NativeWriterReservation &reservation, bool publication_wait) {
	volatile bool ready = false;
	PG_TRY();
	{
		ready = WaitUntilReadyImpl(reservation, publication_wait);
	}
	PG_CATCH();
	{
		ConditionVariableCancelSleep();
		PG_RE_THROW();
	}
	PG_END_TRY();
	return ready;
}

} // namespace

void
InitNativeWriterQueueShmem() {
#if PG_VERSION_NUM >= 150000
	prev_queue_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = QueueShmemRequest;
#else
	QueueShmemRequest();
#endif
	prev_queue_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = QueueShmemStartup;
}

bool
ReserveNativeWriterPublication(uint64_t table_id, uint64_t observed_snapshot_id, uint64_t observed_next_row_id,
                               uint32_t owner_xid, uint32_t owner_command_id, bool row_count_known, uint64_t row_count,
                               NativeWriterReservationResult *result) {
	*result = {};
	if (!QueueShmem || Local.reservation.active) {
		return false;
	}
	CatalogKey catalog = CurrentCatalogKey();
	if (!OidIsValid(catalog.snapshot_relation_oid)) {
		return false;
	}
	EnsureBackendCallbacksRegistered();
	OwnerIdentity owner = CurrentOwner(owner_xid);

	uint32_t free_slot = INVALID_SLOT;
	bool wake = false;
	for (int pass = 0; pass < 2; pass++) {
		SpinLockAcquire(&QueueShmem->lock);
		ReclaimInvalidSlots();
		ReservationSlot *last = LastValidSlot(catalog, table_id, false);
		/* A pending candidate must still be ahead of protocol truth. A committed
		 * anchor may equal truth; it closes the metadata-read/registration race. */
		if (last && (last->candidate_snapshot_id == 0 ||
		             (last->committed ? last->candidate_snapshot_id < observed_snapshot_id
		                              : last->candidate_snapshot_id <= observed_snapshot_id))) {
			InvalidateCatalog(catalog);
			ReclaimInvalidSlots();
			last = nullptr;
			wake = true;
		}
		if (last) {
			ReservationSlot *last_table = LastValidSlot(catalog, table_id, true);
			if (last_table && last_table->row_assigned && last_table->row_count_known) {
				bool overflow = last_table->candidate_row_id >
				                static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - last_table->row_count;
				uint64_t expected_frontier = overflow ? 0 : last_table->candidate_row_id + last_table->row_count;
				bool stale = overflow || (last_table->committed ? expected_frontier < observed_next_row_id
				                                                : last_table->candidate_row_id < observed_next_row_id);
				if (stale) {
					InvalidateCatalog(catalog);
					ReclaimInvalidSlots();
					wake = true;
				}
			}
		}
		for (uint32_t i = 0; i < QueueCapacity(); i++) {
			if (!QueueShmem->slots[i].in_use) {
				free_slot = i;
				break;
			}
		}
		if (free_slot == INVALID_SLOT && ReclaimOldestCommittedAnchor()) {
			for (uint32_t i = 0; i < QueueCapacity(); i++) {
				if (!QueueShmem->slots[i].in_use) {
					free_slot = i;
					break;
				}
			}
		}
		if (free_slot != INVALID_SLOT) {
			break;
		}
		SpinLockRelease(&QueueShmem->lock);
		if (wake) {
			ConditionVariableBroadcast(&QueueShmem->changed);
			wake = false;
		}
		if (pass == 0) {
			ReclaimInactiveOwners();
		}
	}

	if (free_slot == INVALID_SLOT || QueueShmem->next_ticket == std::numeric_limits<uint64_t>::max()) {
		if (free_slot != INVALID_SLOT) {
			SpinLockRelease(&QueueShmem->lock);
		}
		if (wake) {
			ConditionVariableBroadcast(&QueueShmem->changed);
		}
		return false;
	}

	auto &slot = QueueShmem->slots[free_slot];
	MemSet(&slot, 0, sizeof(slot));
	slot.in_use = true;
	slot.valid = true;
	slot.row_count_known = row_count_known;
	slot.catalog = catalog;
	slot.ticket = QueueShmem->next_ticket++;
	slot.owner = owner;
	slot.owner_command_id = owner_command_id;
	slot.table_id = table_id;
	slot.row_count = row_count;

	ReservationSlot *previous = LastPriorSlot(slot, false);
	bool candidate_valid =
	    !previous || previous->candidate_snapshot_id < static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
	slot.candidate_snapshot_id =
	    candidate_valid && previous ? previous->candidate_snapshot_id + 1 : observed_snapshot_id + 1;
	if (!candidate_valid) {
		slot.valid = false;
	}
	ReservationSlot *previous_table = LastPriorSlot(slot, true);
	if (!previous_table) {
		slot.candidate_row_id = observed_next_row_id;
		slot.row_assigned = true;
	} else if (previous_table->row_assigned && previous_table->row_count_known &&
	           previous_table->candidate_row_id <=
	               static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - previous_table->row_count) {
		slot.candidate_row_id = previous_table->candidate_row_id + previous_table->row_count;
		slot.row_assigned = true;
	}

	Local = {{free_slot, slot.ticket, true}, false};
	result->reservation = Local.reservation;
	result->candidate_snapshot_id = slot.candidate_snapshot_id;
	bool assigned = slot.row_assigned;
	if (assigned) {
		result->candidate_row_id = slot.candidate_row_id;
	}
	SpinLockRelease(&QueueShmem->lock);
	if (wake) {
		ConditionVariableBroadcast(&QueueShmem->changed);
	}

	if (!candidate_valid) {
		return false;
	}
	/* COPY does not know its size before streaming. A later same-table writer
	 * waits here only until COPY reports its final immutable row count. */
	if (!assigned && !WaitUntilReady(Local.reservation, false)) {
		return false;
	}
	if (!assigned) {
		SpinLockAcquire(&QueueShmem->lock);
		ReservationSlot *current = FindSlot(Local.reservation);
		if (!current || !current->valid || !current->row_assigned) {
			SpinLockRelease(&QueueShmem->lock);
			return false;
		}
		result->candidate_row_id = current->candidate_row_id;
		SpinLockRelease(&QueueShmem->lock);
	}
	return true;
}

bool
CompleteNativeWriterReservation(const NativeWriterReservation &reservation, uint64_t row_count) {
	if (!QueueShmem || !reservation.active) {
		return false;
	}
	bool valid = false;
	SpinLockAcquire(&QueueShmem->lock);
	ReservationSlot *slot = FindSlot(reservation);
	if (slot && slot->valid) {
		if ((slot->row_count_known && slot->row_count != row_count) || row_count == 0) {
			InvalidateFrom(slot->catalog, slot->ticket);
		} else {
			slot->row_count = row_count;
			slot->row_count_known = true;
			valid = true;
		}
	}
	SpinLockRelease(&QueueShmem->lock);
	ConditionVariableBroadcast(&QueueShmem->changed);
	return valid;
}

bool
WaitForNativeWriterPublication(const NativeWriterReservation &reservation) {
	if (!QueueShmem || !reservation.active) {
		return false;
	}
	return WaitUntilReady(reservation, true);
}

bool
ValidateNativeWriterReservation(const NativeWriterReservation &reservation, uint64_t snapshot_id, uint64_t row_id) {
	if (!QueueShmem || !reservation.active) {
		return false;
	}
	bool valid = false;
	SpinLockAcquire(&QueueShmem->lock);
	ReservationSlot *slot = FindSlot(reservation);
	if (slot && slot->valid && slot->row_assigned && slot->candidate_snapshot_id == snapshot_id &&
	    slot->candidate_row_id == row_id) {
		valid = true;
	} else if (slot) {
		InvalidateFrom(slot->catalog, slot->ticket);
	}
	SpinLockRelease(&QueueShmem->lock);
	if (!valid) {
		ConditionVariableBroadcast(&QueueShmem->changed);
	}
	return valid;
}

void
InvalidateNativeWriterReservation(const NativeWriterReservation &reservation) {
	if (!QueueShmem || !reservation.active) {
		return;
	}
	SpinLockAcquire(&QueueShmem->lock);
	ReservationSlot *slot = FindSlot(reservation);
	if (slot) {
		InvalidateFrom(slot->catalog, slot->ticket);
	}
	SpinLockRelease(&QueueShmem->lock);
	ConditionVariableBroadcast(&QueueShmem->changed);
}

void
MarkNativeWriterReservationPublished(const NativeWriterReservation &reservation) {
	if (Local.reservation.active && reservation.active && Local.reservation.slot == reservation.slot &&
	    Local.reservation.ticket == reservation.ticket) {
		Local.published = true;
	}
}

} // namespace pgducklake
