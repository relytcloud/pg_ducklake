/*
 * pgducklake_guc.cpp -- DuckLake GUC definitions and registration.
 *
 * @scope backend: register GUCs
 *
 * Defines extension-level configuration variables and registers them in
 * _PG_init().
 */

#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_maintenance.hpp"

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

char *superuser_role = strdup("ducklake_superuser");
char *writer_role = strdup("ducklake_writer");
char *reader_role = strdup("ducklake_reader");

bool maintenance_enabled = true;
int maintenance_naptime = 60;
int maintenance_max_workers = 3;
bool maintenance_flush_inlined_data = true;
bool maintenance_expire_snapshots = true;
bool maintenance_cleanup_old_files = false;

void RegisterGUCs() {
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

  DefineCustomIntVariable("ducklake.maintenance_max_workers",
                          "Maximum number of concurrent DuckLake maintenance workers.", NULL, &maintenance_max_workers,
                          3, 1, DUCKLAKE_MAX_MAINTENANCE_WORKERS, PGC_POSTMASTER, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("ducklake.maintenance_flush_inlined_data",
                           "Flush inlined data to Parquet files during maintenance.", NULL,
                           &maintenance_flush_inlined_data, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("ducklake.maintenance_expire_snapshots", "Expire old snapshots during maintenance.", NULL,
                           &maintenance_expire_snapshots, true, PGC_SIGHUP, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("ducklake.maintenance_cleanup_old_files",
                           "Clean up unreferenced data files during maintenance.", NULL, &maintenance_cleanup_old_files,
                           false, PGC_SIGHUP, 0, NULL, NULL, NULL);
}

} // namespace pgducklake
