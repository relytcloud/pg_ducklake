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
bool native_writer_reservation_queue = true;
int native_writer_reservation_queue_capacity = 256;
int native_writer_reservation_queue_wait_ms = 10;
int native_writer_max_retry_count = 10;
int native_writer_retry_wait_ms = 1;
double native_writer_retry_backoff = 1.5;
bool test_native_writer_force_client_retry_before_rebase = false;
int native_writer_test_fault = NATIVE_WRITER_TEST_FAULT_OFF;

static const struct config_enum_entry native_writer_test_fault_options[] = {
    {"off", NATIVE_WRITER_TEST_FAULT_OFF, false},
    {"after_prewrite", NATIVE_WRITER_TEST_FAULT_AFTER_PREWRITE, false},
    {"after_claim", NATIVE_WRITER_TEST_FAULT_AFTER_CLAIM, false},
    {"after_retag", NATIVE_WRITER_TEST_FAULT_AFTER_RETAG, false},
    {"after_table_stats", NATIVE_WRITER_TEST_FAULT_AFTER_TABLE_STATS, false},
    {"after_column_stats", NATIVE_WRITER_TEST_FAULT_AFTER_COLUMN_STATS, false},
    {"after_change_record", NATIVE_WRITER_TEST_FAULT_AFTER_CHANGE_RECORD, false},
    {"after_publication", NATIVE_WRITER_TEST_FAULT_AFTER_PUBLICATION, false},
    {NULL, 0, false},
};

bool enable_metadata_sync = true;

int threads = -1;

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
	                         "Enable the PostgreSQL-native writer for supported INSERT ... VALUES and "
	                         "INSERT ... SELECT UNNEST($n) statements.",
	                         NULL, &enable_direct_insert, true, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ducklake.native_writer_reservation_queue",
	                         "Use speculative process-shared publication and row-ID reservations.",
	                         "Reservations are performance hints; every write still validates DuckLake metadata and "
	                         "claims the snapshot primary key.",
	                         &native_writer_reservation_queue, true, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("ducklake.native_writer_reservation_queue_capacity",
	                        "Maximum number of process-shared native-writer reservations.",
	                        "A full queue safely falls back to ordinary optimistic publication.",
	                        &native_writer_reservation_queue_capacity, 256, 1, 256, PGC_POSTMASTER, GUC_SUPERUSER_ONLY,
	                        NULL, NULL, NULL);

	DefineCustomIntVariable("ducklake.native_writer_reservation_queue_wait_ms",
	                        "Maximum native-writer queue wait without predecessor progress.",
	                        "Each predecessor gets the smaller of this interruptible cap and the retry budget; "
	                        "expiration safely falls back to ordinary optimistic publication.",
	                        &native_writer_reservation_queue_wait_ms, 10, 0, 60000, PGC_USERSET, GUC_UNIT_MS, NULL,
	                        NULL, NULL);

	DefineCustomIntVariable("ducklake.native_writer_max_retry_count",
	                        "Maximum snapshot-claim retries for the PostgreSQL-native inline writer.", NULL,
	                        &native_writer_max_retry_count, 10, 0, 1000, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("ducklake.native_writer_retry_wait_ms",
	                        "Initial randomized wait between native-writer publication retries.", NULL,
	                        &native_writer_retry_wait_ms, 1, 0, 60000, PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);

	DefineCustomRealVariable("ducklake.native_writer_retry_backoff",
	                         "Exponential backoff factor for native-writer publication retries.", NULL,
	                         &native_writer_retry_backoff, 1.5, 1.0, 100.0, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("ducklake.test_native_writer_force_client_retry_before_rebase",
	                         "Force test-only client retry when native-writer publication requires a rebase.",
	                         "Used to reproduce the historical insert-then-client-retry protocol.",
	                         &test_native_writer_force_client_retry_before_rebase, false, PGC_SUSET,
	                         GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

	DefineCustomEnumVariable("ducklake.test_native_writer_fault",
	                         "Inject a test-only native-writer failure at a publication boundary.",
	                         "For protocol validation only; superusers can select one deterministic fault point.",
	                         &native_writer_test_fault, NATIVE_WRITER_TEST_FAULT_OFF, native_writer_test_fault_options,
	                         PGC_SUSET, GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);

	DefineCustomIntVariable(
	    "ducklake.threads", "Maximum number of DuckDB threads per Postgres backend (-1 = DuckDB default, all cores).",
	    "Takes effect when the DuckDB instance initializes; SET before the first DuckLake query in a "
	    "session, or call ducklake.recycle_ddb() to re-apply.",
	    &threads, -1, -1, 1024, PGC_USERSET, 0, NULL, NULL, NULL);

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
