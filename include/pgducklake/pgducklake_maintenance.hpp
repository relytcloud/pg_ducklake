#pragma once

/*
 * pgducklake_maintenance.hpp -- DuckLake background maintenance worker.
 *
 * Provides a launcher/worker architecture for periodic table maintenance
 * (flush inlined data, rewrite data files, merge files, expire snapshots,
 * clean up old files).
 */

/* Hard cap on concurrent maintenance workers (GUC max bound) */
#define DUCKLAKE_MAX_MAINTENANCE_WORKERS 8

namespace pgducklake {

/* Hook shmem_request + shmem_startup for shared worker state. Call from _PG_init(). */
void InitMaintenanceShmem();

/* Register the launcher as a static background worker. Call from _PG_init(). */
void RegisterMaintenanceLauncher();

} // namespace pgducklake
