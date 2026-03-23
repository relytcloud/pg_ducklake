/*
 * pgducklake_table.hpp -- Table lifecycle: AM handler, DDL triggers, sync.
 *
 * Declares EnsureDuckLakeTable (used by partition and sorted_by procs)
 * and table sync handlers (used by the snapshot trigger framework).
 */
#pragma once

extern "C" {
#include "postgres.h"

/* Validates that relid uses the ducklake table AM. Errors if not. */
void EnsureDuckLakeTable(Oid relid);
}

namespace pgducklake {

/* Snapshot sync handlers for table create/drop.
 * Caller must have an active SPI connection. */
void SyncNewTables(const char *snapshot_id);
void SyncDroppedTables(const char *snapshot_id);

} // namespace pgducklake
