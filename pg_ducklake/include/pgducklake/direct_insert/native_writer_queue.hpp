#pragma once

#include <cstdint>

namespace pgducklake {

/*
 * The reservation queue is a bounded shared-memory sequencer, not a lock on
 * DuckLake metadata. Per catalog, ticket order predicts the global snapshot
 * sequence; per table, known row counts also predict nonoverlapping row-ID
 * ranges. An unknown-size producer blocks later same-table row assignment only
 * until it reports its final count after prewrite.
 *
 * A writer normally waits for prior tickets before publishing, which turns
 * optimistic snapshot races into an orderly pipeline. Waiting is capped. A
 * full queue, timeout, invalid predecessor, or dead owner invalidates dependent
 * predictions and wakes writers to use normal metadata retry and row retagging.
 * Transactional publication remains the source of truth in every case.
 */
struct NativeWriterReservation {
	uint32_t slot;
	uint64_t ticket;
	bool active;
};

struct NativeWriterReservationResult {
	NativeWriterReservation reservation;
	uint64_t candidate_snapshot_id;
	uint64_t candidate_row_id;
};

void InitNativeWriterQueueShmem();

bool ReserveNativeWriterPublication(uint64_t table_id, uint64_t observed_snapshot_id, uint64_t observed_next_row_id,
                                    uint32_t owner_xid, uint32_t owner_command_id, bool row_count_known,
                                    uint64_t row_count, NativeWriterReservationResult *result);
bool CompleteNativeWriterReservation(const NativeWriterReservation &reservation, uint64_t row_count);
bool WaitForNativeWriterPublication(const NativeWriterReservation &reservation);
bool ValidateNativeWriterReservation(const NativeWriterReservation &reservation, uint64_t snapshot_id, uint64_t row_id);
void InvalidateNativeWriterReservation(const NativeWriterReservation &reservation);
void MarkNativeWriterReservationPublished(const NativeWriterReservation &reservation);

} // namespace pgducklake
