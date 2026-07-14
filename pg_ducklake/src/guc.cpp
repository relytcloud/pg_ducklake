#include "pgducklake/guc.hpp"
#include "pgducklake/maintenance_worker.hpp"

extern "C" {
#include "postgres.h"

#include "utils/guc.h"
}

namespace pgducklake {

char *default_table_path = strdup("");
double vacuum_delete_threshold = 0.1;
bool enable_direct_insert = true;
bool ctas_skip_data = false;

bool enable_metadata_sync = true;

int threads = -1;
bool use_shared_worker = false;
int worker_max_sessions = 0;
int worker_arrow_pool_pages = 256;
int worker_arrow_page_size = 1024 * 1024;
int worker_scan_pool_size = 0;
int worker_scan_producers = 4;

char *superuser_role = strdup("ducklake_superuser");
char *writer_role = strdup("ducklake_writer");
char *reader_role = strdup("ducklake_reader");

bool maintenance_enabled = true;
int maintenance_naptime = 60;
int maintenance_max_workers = 3;
bool maintenance_flush_inlined_data = true;
bool maintenance_expire_snapshots = true;
bool maintenance_cleanup_old_files = false;

void
InitGUCs() {
	DefineCustomStringVariable("ducklake.default_table_path",
	                           "Default directory path for DuckLake tables. If set, tables will be "
	                           "created under this path.",
	                           NULL, &default_table_path, "", PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomRealVariable("ducklake.vacuum_delete_threshold",
	                         "Minimum fraction of deleted rows (0.0-1.0) before VACUUM rewrites a "
	                         "data file.",
	                         NULL, &vacuum_delete_threshold, 0.1, 0.0, 1.0, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ducklake.enable_direct_insert",
	                         "Enable direct insert optimization for INSERT ... "
	                         "SELECT UNNEST($n) statements.",
	                         NULL, &enable_direct_insert, true, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable(
	    "ducklake.threads", "Maximum number of DuckDB threads per Postgres backend (-1 = DuckDB default, all cores).",
	    "Takes effect when the DuckDB instance initializes; SET before the first DuckLake query in a "
	    "session, or call ducklake.recycle_ddb() to re-apply.",
	    &threads, -1, -1, 1024, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ducklake.use_shared_worker",
	                         "Dispatch eligible read-only DuckLake/file queries to the shared DuckDB worker "
	                         "process instead of executing DuckDB in this backend.",
	                         NULL, &use_shared_worker, false, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("ducklake.max_worker_sessions",
	                        "Concurrent sessions the shared DuckDB worker serves (session-pool slots in "
	                        "shared memory); further dispatches wait for a free slot. "
	                        "0 (default) disables the shared worker and reserves no shared memory.",
	                        NULL, &worker_max_sessions, 0, 0, 1024, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("ducklake.arrow_pool_pages",
	                        "Shared-memory Arrow pages for the worker scan transport (0 disables the pool "
	                        "and with it shared-worker heap scans).",
	                        NULL, &worker_arrow_pool_pages, 256, 0, 65536, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("ducklake.arrow_page_size", "Size of one Arrow scan-transport page, in bytes.", NULL,
	                        &worker_arrow_page_size, 1024 * 1024, 64 * 1024, 64 * 1024 * 1024, PGC_POSTMASTER, 0, NULL,
	                        NULL, NULL);

	DefineCustomIntVariable("ducklake.scan_pool_size",
	                        "Scan-producer background workers per database for shared-worker heap scans "
	                        "(0 = produce on the requesting backend).",
	                        NULL, &worker_scan_pool_size, 0, 0, 64, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("ducklake.scan_producers",
	                        "Parallel scan-producer tasks per shared-worker heap scan (capped by "
	                        "ducklake.scan_pool_size).",
	                        NULL, &worker_scan_producers, 4, 1, 64, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ducklake.enable_metadata_sync",
	                         "Enable reverse metadata sync from DuckDB to PostgreSQL. "
	                         "When enabled (default), a snapshot trigger detects tables "
	                         "created or dropped by external DuckDB clients and syncs "
	                         "the corresponding pg_class entries. Disable this when all "
	                         "DDL and DML goes through PostgreSQL, to avoid the per-commit "
	                         "trigger overhead.",
	                         NULL, &enable_metadata_sync, true, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomStringVariable("ducklake.superuser_role",
	                           "Role with full DDL + DML access to DuckLake tables. "
	                           "Created during CREATE EXTENSION if it does not exist.",
	                           NULL, &superuser_role, "ducklake_superuser", PGC_POSTMASTER, GUC_SUPERUSER_ONLY, NULL,
	                           NULL, NULL);

	DefineCustomStringVariable("ducklake.writer_role",
	                           "Role with DML access (SELECT/INSERT/UPDATE/DELETE) to DuckLake tables. "
	                           "Created during CREATE EXTENSION if it does not exist.",
	                           NULL, &writer_role, "ducklake_writer", PGC_POSTMASTER, GUC_SUPERUSER_ONLY, NULL, NULL,
	                           NULL);

	DefineCustomStringVariable("ducklake.reader_role",
	                           "Role with SELECT-only access to DuckLake tables. "
	                           "Created during CREATE EXTENSION if it does not exist.",
	                           NULL, &reader_role, "ducklake_reader", PGC_POSTMASTER, GUC_SUPERUSER_ONLY, NULL, NULL,
	                           NULL);

	DefineCustomBoolVariable("ducklake.maintenance_enabled", "Enable the DuckLake background maintenance worker.", NULL,
	                         &maintenance_enabled, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("ducklake.maintenance_naptime", "Seconds between DuckLake maintenance cycles.", NULL,
	                        &maintenance_naptime, 60, 1, 86400, PGC_SIGHUP, GUC_UNIT_S, NULL, NULL, NULL);

	DefineCustomIntVariable(
	    "ducklake.maintenance_max_workers", "Maximum number of concurrent DuckLake maintenance workers.", NULL,
	    &maintenance_max_workers, 3, 1, DUCKLAKE_MAX_MAINTENANCE_WORKERS, PGC_POSTMASTER, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ducklake.maintenance_flush_inlined_data",
	                         "Flush inlined data to Parquet files during maintenance.", NULL,
	                         &maintenance_flush_inlined_data, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ducklake.maintenance_expire_snapshots", "Expire old snapshots during maintenance.", NULL,
	                         &maintenance_expire_snapshots, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ducklake.maintenance_cleanup_old_files",
	                         "Clean up unreferenced data files during maintenance.", NULL,
	                         &maintenance_cleanup_old_files, false, PGC_SIGHUP, 0, NULL, NULL, NULL);
}

} // namespace pgducklake
