/*
 * pgducklake_sync.hpp -- Sync framework: DuckDB metadata -> PG catalog.
 *
 * Declares the syncing_from_metadata guard and the SyncHandler type.
 */
#pragma once

namespace pgducklake {

/* Guard to prevent circular triggers during metadata->PG catalog sync.
 * Also checked by the utility hook to skip DuckDB execution when creating
 * ducklake_sorted indexes during sync. */
extern bool syncing_from_metadata;

/* When true the snapshot trigger skips all sync handlers.  Managed
 * exclusively via SkipSnapshotSyncGuard -- do not set directly. */
extern bool skip_snapshot_sync;

/* RAII guard: sets skip_snapshot_sync = true on construction, resets
 * on destruction.  Use in any code path that inserts into
 * ducklake_snapshot but has no DDL changes to reverse-sync (direct
 * insert, ExecuteCommit). */
struct SkipSnapshotSyncGuard {
  SkipSnapshotSyncGuard() {
    skip_snapshot_sync = true;
  }
  ~SkipSnapshotSyncGuard() {
    skip_snapshot_sync = false;
  }
  SkipSnapshotSyncGuard(const SkipSnapshotSyncGuard &) = delete;
  SkipSnapshotSyncGuard &operator=(const SkipSnapshotSyncGuard &) = delete;
};

/* Signature for per-object-type sync handlers.
 * Called by ducklake_snapshot_trigger for each new snapshot,
 * after SyncNewTables/SyncDroppedTables have run.
 * Caller guarantees: active SPI connection, syncing_from_metadata = true. */
using SyncHandler = void (*)(const char *snapshot_id);

} // namespace pgducklake
