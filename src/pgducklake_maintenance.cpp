/*
 * pgducklake_maintenance.cpp -- Background maintenance worker for DuckLake.
 *
 * @scope backend: register shmem hooks and launcher background worker
 *
 * Implements a launcher/worker architecture modeled on PostgreSQL's
 * autovacuum.  The launcher wakes periodically, scans pg_database,
 * and spawns a short-lived worker for each database.  Each worker
 * runs a two-phase maintenance pipeline:
 *
 * Phase 1 -- in-process (must run inside PG backend):
 *   - flush_inlined_data   (per-table, GUC-gated)
 *   - expire_snapshots     (catalog-level, GUC-gated)
 *
 * Phase 2 -- compaction (manipulates immutable parquet files):
 *   - rewrite_data_files   (per-table)
 *   - merge_adjacent_files (per-table)
 *   - cleanup_old_files    (catalog-level, GUC-gated)
 *
 * The two-phase split prepares for a future external DuckDB executor:
 * compaction operations only need file access + metadata, so they can
 * be offloaded to a standalone DuckDB process that reads/writes parquet
 * files and then reports metadata changes back to PG.  Phase 1 operations
 * access PG-resident data (inline tables, snapshot metadata) and must
 * stay in-process.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_maintenance.hpp"

#include <duckdb/parser/keyword_helper.hpp>

#include <string>

extern "C" {
#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_extension.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
}

/* ----------------------------------------------------------------
 * Shared memory
 * ---------------------------------------------------------------- */

namespace {

struct MaintenanceWorkerSlot {
  Oid database_oid;
  pid_t worker_pid;
  bool in_use;
};

struct MaintenanceShmemStruct {
  slock_t lock;
  MaintenanceWorkerSlot workers[DUCKLAKE_MAX_MAINTENANCE_WORKERS];
};

MaintenanceShmemStruct *MaintShmem = nullptr;

#if PG_VERSION_NUM >= 150000
shmem_request_hook_type prev_shmem_request_hook = nullptr;
#endif
shmem_startup_hook_type prev_shmem_startup_hook = nullptr;

void ShmemRequest() {
#if PG_VERSION_NUM >= 150000
  if (prev_shmem_request_hook)
    prev_shmem_request_hook();
#endif
  RequestAddinShmemSpace(sizeof(MaintenanceShmemStruct));
}

void ShmemStartup() {
  if (prev_shmem_startup_hook)
    prev_shmem_startup_hook();

  bool found;
  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
  MaintShmem =
      (MaintenanceShmemStruct *)ShmemInitStruct("DuckLakeMaintenance Data", sizeof(MaintenanceShmemStruct), &found);
  if (!found) {
    MemSet(MaintShmem, 0, sizeof(MaintenanceShmemStruct));
    SpinLockInit(&MaintShmem->lock);
  }
  LWLockRelease(AddinShmemInitLock);
}

/* ----------------------------------------------------------------
 * Slot helpers (caller must hold MaintShmem->lock)
 * ---------------------------------------------------------------- */

bool HasWorkerForDatabase(Oid db_oid) {
  for (int i = 0; i < DUCKLAKE_MAX_MAINTENANCE_WORKERS; i++) {
    if (MaintShmem->workers[i].in_use && MaintShmem->workers[i].database_oid == db_oid)
      return true;
  }
  return false;
}

int CountActiveWorkers() {
  int count = 0;
  for (int i = 0; i < DUCKLAKE_MAX_MAINTENANCE_WORKERS; i++) {
    if (MaintShmem->workers[i].in_use)
      count++;
  }
  return count;
}

/* Claim a free slot. Returns slot index or -1 if full. */
int ClaimSlot(Oid db_oid, pid_t pid) {
  for (int i = 0; i < DUCKLAKE_MAX_MAINTENANCE_WORKERS; i++) {
    if (!MaintShmem->workers[i].in_use) {
      MaintShmem->workers[i].in_use = true;
      MaintShmem->workers[i].database_oid = db_oid;
      MaintShmem->workers[i].worker_pid = pid;
      return i;
    }
  }
  return -1;
}

void ReleaseSlot(Oid db_oid) {
  for (int i = 0; i < DUCKLAKE_MAX_MAINTENANCE_WORKERS; i++) {
    if (MaintShmem->workers[i].in_use && MaintShmem->workers[i].database_oid == db_oid) {
      MaintShmem->workers[i].in_use = false;
      break;
    }
  }
}

/* ----------------------------------------------------------------
 * Quoting helper
 * ---------------------------------------------------------------- */

std::string Q(const std::string &s) {
  return duckdb::KeywordHelper::WriteQuoted(s, '\'');
}

/* ----------------------------------------------------------------
 * Phase 1: in-process maintenance (needs PG backend)
 *
 * These operations access PG-resident data or metadata tables that
 * cannot be reached by an external process.
 * ---------------------------------------------------------------- */

/* Flush inlined data from PG metadata tables to parquet files. */
void MaintainTableInProcess(const char *schema_name, const char *table_name) {
  if (!pgducklake::maintenance_flush_inlined_data)
    return;

  std::string query = "SELECT * FROM ducklake_flush_inlined_data(" + Q(PGDUCKLAKE_DUCKDB_CATALOG) +
                      ", schema_name=" + Q(schema_name) + ", table_name=" + Q(table_name) + ")";
  const char *err = nullptr;
  if (pgducklake::ExecuteDuckDBQuery(query.c_str(), &err) != 0) {
    elog(WARNING, "ducklake maintenance: flush_inlined_data failed for \"%s\".\"%s\": %s", schema_name, table_name,
         err ? err : "unknown");
  }
}

/* Expire old snapshots (pure metadata update, no file I/O). */
void MaintainCatalogInProcess() {
  if (!pgducklake::maintenance_expire_snapshots)
    return;

  std::string query = "SELECT * FROM ducklake_expire_snapshots(" + Q(PGDUCKLAKE_DUCKDB_CATALOG) + ")";
  const char *err = nullptr;
  if (pgducklake::ExecuteDuckDBQuery(query.c_str(), &err) != 0) {
    elog(WARNING, "ducklake maintenance: expire_snapshots failed: %s", err ? err : "unknown");
  }
}

/* ----------------------------------------------------------------
 * Phase 2: compaction (manipulates immutable parquet files)
 *
 * These operations read/write/delete parquet files on storage and
 * update metadata.  They only need the DuckLake catalog connection
 * string and file access -- no PG-resident data.
 *
 * TODO: offload to an external DuckDB process.  The external executor
 * would receive the ducklake metadata connection info, execute
 * rewrite/merge/cleanup via the DuckDB CLI or embedded API, and
 * report file-level changes back.  The maintenance worker would then
 * commit the metadata updates inside PG.  This avoids holding a PG
 * backend slot during potentially long-running file I/O.
 * ---------------------------------------------------------------- */

/* Rewrite data files (remove deleted rows) + merge small files. */
void CompactTable(const char *schema_name, const char *table_name) {
  std::string db(PGDUCKLAKE_DUCKDB_CATALOG);
  std::string schema(schema_name);
  std::string table(table_name);

  /* Rewrite: purge deleted rows from data files */
  {
    std::string query = "SELECT * FROM ducklake_rewrite_data_files(" + Q(db) + ", " + Q(table) +
                        ", schema=" + Q(schema) + ", delete_threshold => " +
                        std::to_string(pgducklake::vacuum_delete_threshold) + ")";
    const char *err = nullptr;
    if (pgducklake::ExecuteDuckDBQuery(query.c_str(), &err) != 0) {
      elog(WARNING, "ducklake maintenance: rewrite_data_files failed for \"%s\".\"%s\": %s", schema_name, table_name,
           err ? err : "unknown");
    }
  }

  /* Merge: consolidate small adjacent files */
  {
    std::string query =
        "SELECT * FROM ducklake_merge_adjacent_files(" + Q(db) + ", " + Q(table) + ", schema=" + Q(schema) + ")";
    const char *err = nullptr;
    if (pgducklake::ExecuteDuckDBQuery(query.c_str(), &err) != 0) {
      elog(WARNING, "ducklake maintenance: merge_adjacent_files failed for \"%s\".\"%s\": %s", schema_name, table_name,
           err ? err : "unknown");
    }
  }
}

/* Remove unreferenced data files from storage. */
void CompactCatalog() {
  if (!pgducklake::maintenance_cleanup_old_files)
    return;

  std::string query =
      "SELECT * FROM ducklake_cleanup_old_files(" + Q(PGDUCKLAKE_DUCKDB_CATALOG) + ", cleanup_all => true)";
  const char *err = nullptr;
  if (pgducklake::ExecuteDuckDBQuery(query.c_str(), &err) != 0) {
    elog(WARNING, "ducklake maintenance: cleanup_old_files failed: %s", err ? err : "unknown");
  }
}

} // anonymous namespace

/* ----------------------------------------------------------------
 * Worker entry point
 * ---------------------------------------------------------------- */

extern "C" {

PGDLLEXPORT void ducklake_maintenance_worker_main(Datum main_arg) {
  Oid database_oid = DatumGetObjectId(main_arg);

  pqsignal(SIGTERM, die);
  BackgroundWorkerUnblockSignals();

  BackgroundWorkerInitializeConnectionByOid(database_oid, InvalidOid, 0);

  elog(LOG, "ducklake maintenance worker started for database %u", database_oid);

  /* Update pid in the shmem slot pre-claimed by the launcher */
  SpinLockAcquire(&MaintShmem->lock);
  for (int i = 0; i < DUCKLAKE_MAX_MAINTENANCE_WORKERS; i++) {
    if (MaintShmem->workers[i].in_use && MaintShmem->workers[i].database_oid == database_oid) {
      MaintShmem->workers[i].worker_pid = MyProcPid;
      break;
    }
  }
  SpinLockRelease(&MaintShmem->lock);

  /* Declare up front so goto cleanup can jump past them */
  bool has_extension = false;
  int ntables = 0;
  char **schemas = nullptr;
  char **tables = nullptr;

  /*
   * Check whether pg_ducklake is installed in this database.
   * If not, release our slot and exit immediately.
   */
  StartTransactionCommand();
  SPI_connect();
  PushActiveSnapshot(GetTransactionSnapshot());

  if (SPI_execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_ducklake'", true, 1) == SPI_OK_SELECT &&
      SPI_processed > 0) {
    has_extension = true;
  }

  PopActiveSnapshot();
  SPI_finish();
  CommitTransactionCommand();

  if (!has_extension) {
    elog(DEBUG1, "ducklake maintenance worker: pg_ducklake not installed in database %u, exiting", database_oid);
    goto cleanup;
  }

  pgstat_report_activity(STATE_RUNNING, "ducklake maintenance");

  {
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    int ret = SPI_execute("SELECT n.nspname, c.relname "
                          "FROM pg_class c "
                          "JOIN pg_namespace n ON c.relnamespace = n.oid "
                          "WHERE c.relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake') "
                          "AND c.relkind = 'r'",
                          true, 0);

    if (ret == SPI_OK_SELECT && SPI_processed > 0) {
      ntables = (int)SPI_processed;
      MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);
      schemas = (char **)palloc(ntables * sizeof(char *));
      tables = (char **)palloc(ntables * sizeof(char *));
      for (int i = 0; i < ntables; i++) {
        bool isnull;
        /* nspname and relname are `name`, not `text`: decode via DatumGetName/NameStr. */
        Datum s = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
        Datum t = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull);
        schemas[i] = pstrdup(NameStr(*DatumGetName(s)));
        tables[i] = pstrdup(NameStr(*DatumGetName(t)));
      }
      MemoryContextSwitchTo(old_ctx);
    }

    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();
  }

  /*
   * Phase 1: in-process maintenance.
   * Flush inlined data per table, then expire snapshots.
   * These must run inside the PG backend.
   */
  for (int i = 0; i < ntables; i++) {
    CHECK_FOR_INTERRUPTS();

    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    PG_TRY();
    {
      MaintainTableInProcess(schemas[i], tables[i]);
    }
    PG_CATCH();
    {
      MemoryContext saved = MemoryContextSwitchTo(TopMemoryContext);
      ErrorData *edata = CopyErrorData();
      FlushErrorState();
      MemoryContextSwitchTo(saved);
      elog(WARNING, "ducklake maintenance: in-process error on \"%s\".\"%s\": %s", schemas[i], tables[i],
           edata->message ? edata->message : "unknown");
      FreeErrorData(edata);
      AbortCurrentTransaction();
      StartTransactionCommand();
      PushActiveSnapshot(GetTransactionSnapshot());
    }
    PG_END_TRY();

    PopActiveSnapshot();
    CommitTransactionCommand();
  }

  /* expire_snapshots: catalog-level, in-process */
  {
    CHECK_FOR_INTERRUPTS();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    PG_TRY();
    {
      MaintainCatalogInProcess();
    }
    PG_CATCH();
    {
      MemoryContext saved = MemoryContextSwitchTo(TopMemoryContext);
      ErrorData *edata = CopyErrorData();
      FlushErrorState();
      MemoryContextSwitchTo(saved);
      elog(WARNING, "ducklake maintenance: expire_snapshots error: %s", edata->message ? edata->message : "unknown");
      FreeErrorData(edata);
      AbortCurrentTransaction();
      StartTransactionCommand();
      PushActiveSnapshot(GetTransactionSnapshot());
    }
    PG_END_TRY();

    PopActiveSnapshot();
    CommitTransactionCommand();
  }

  /*
   * Phase 2: compaction.
   * Rewrite + merge per table, then cleanup old files.
   *
   * TODO: these operations only manipulate immutable parquet files and
   * metadata.  They could be dispatched to an external DuckDB process
   * (CLI or embedded) that connects to the ducklake catalog, performs
   * file I/O outside the PG backend, and reports metadata changes back.
   * This would free the PG backend slot during long-running compaction.
   */
  for (int i = 0; i < ntables; i++) {
    CHECK_FOR_INTERRUPTS();

    elog(DEBUG1, "ducklake maintenance: compacting \"%s\".\"%s\"", schemas[i], tables[i]);

    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    PG_TRY();
    {
      CompactTable(schemas[i], tables[i]);
    }
    PG_CATCH();
    {
      MemoryContext saved = MemoryContextSwitchTo(TopMemoryContext);
      ErrorData *edata = CopyErrorData();
      FlushErrorState();
      MemoryContextSwitchTo(saved);
      elog(WARNING, "ducklake maintenance: compaction error on \"%s\".\"%s\": %s", schemas[i], tables[i],
           edata->message ? edata->message : "unknown");
      FreeErrorData(edata);
      AbortCurrentTransaction();
      StartTransactionCommand();
      PushActiveSnapshot(GetTransactionSnapshot());
    }
    PG_END_TRY();

    PopActiveSnapshot();
    CommitTransactionCommand();
  }

  /* cleanup_old_files: catalog-level compaction */
  {
    CHECK_FOR_INTERRUPTS();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    PG_TRY();
    {
      CompactCatalog();
    }
    PG_CATCH();
    {
      MemoryContext saved = MemoryContextSwitchTo(TopMemoryContext);
      ErrorData *edata = CopyErrorData();
      FlushErrorState();
      MemoryContextSwitchTo(saved);
      elog(WARNING, "ducklake maintenance: cleanup_old_files error: %s", edata->message ? edata->message : "unknown");
      FreeErrorData(edata);
      AbortCurrentTransaction();
      StartTransactionCommand();
      PushActiveSnapshot(GetTransactionSnapshot());
    }
    PG_END_TRY();

    PopActiveSnapshot();
    CommitTransactionCommand();
  }

  /* Free table list */
  for (int i = 0; i < ntables; i++) {
    pfree(schemas[i]);
    pfree(tables[i]);
  }
  if (schemas)
    pfree(schemas);
  if (tables)
    pfree(tables);

  elog(LOG, "ducklake maintenance worker finished for database %u", database_oid);

cleanup:
  SpinLockAcquire(&MaintShmem->lock);
  ReleaseSlot(database_oid);
  SpinLockRelease(&MaintShmem->lock);
}

/* ----------------------------------------------------------------
 * Launcher entry point
 * ---------------------------------------------------------------- */

PGDLLEXPORT void ducklake_maintenance_launcher_main(Datum main_arg) {
  pqsignal(SIGHUP, SignalHandlerForConfigReload);
  pqsignal(SIGTERM, die);
  BackgroundWorkerUnblockSignals();

  /* Connect to postgres database for pg_database queries */
  BackgroundWorkerInitializeConnection("postgres", NULL, 0);

  elog(LOG, "ducklake maintenance launcher started");

  while (!ShutdownRequestPending) {
    int naptime_ms = pgducklake::maintenance_naptime * 1000;
    WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, naptime_ms, PG_WAIT_EXTENSION);
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();

    if (ConfigReloadPending) {
      ConfigReloadPending = false;
      ProcessConfigFile(PGC_SIGHUP);
    }

    if (!pgducklake::maintenance_enabled)
      continue;

    /* Query pg_database for connectable, non-template databases */
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    int ret = SPI_execute("SELECT oid FROM pg_database WHERE datallowconn AND NOT datistemplate", true, 0);

    if (ret == SPI_OK_SELECT) {
      for (uint64 i = 0; i < SPI_processed; i++) {
        bool isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
        Oid db_oid = DatumGetObjectId(d);

        /*
         * Pre-claim a shmem slot before spawning so that the next
         * loop iteration (or launcher cycle) sees the slot as in-use
         * even before the worker process starts.
         */
        SpinLockAcquire(&MaintShmem->lock);
        bool already_running = HasWorkerForDatabase(db_oid);
        int active = CountActiveWorkers();
        int slot = -1;
        if (!already_running && active < pgducklake::maintenance_max_workers)
          slot = ClaimSlot(db_oid, 0 /* pid filled by worker */);
        SpinLockRelease(&MaintShmem->lock);

        if (slot < 0)
          continue;

        /* Spawn a worker for this database */
        BackgroundWorker worker;
        MemSet(&worker, 0, sizeof(BackgroundWorker));
        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
        snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_ducklake");
        snprintf(worker.bgw_function_name, BGW_MAXLEN, "ducklake_maintenance_worker_main");
        snprintf(worker.bgw_name, BGW_MAXLEN, "ducklake maintenance worker (db %u)", db_oid);
        snprintf(worker.bgw_type, BGW_MAXLEN, "ducklake maintenance worker");
        worker.bgw_restart_time = BGW_NEVER_RESTART;
        worker.bgw_main_arg = ObjectIdGetDatum(db_oid);

        if (!RegisterDynamicBackgroundWorker(&worker, NULL)) {
          /* Registration failed -- release the pre-claimed slot */
          SpinLockAcquire(&MaintShmem->lock);
          ReleaseSlot(db_oid);
          SpinLockRelease(&MaintShmem->lock);
        }
      }
    }

    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();

    pgstat_report_stat(false);
    pgstat_report_activity(STATE_IDLE, NULL);
  }

  elog(LOG, "ducklake maintenance launcher shutting down");
}

} // extern "C"

/* ----------------------------------------------------------------
 * Public API called from _PG_init()
 * ---------------------------------------------------------------- */

namespace pgducklake {

void InitMaintenanceShmem() {
#if PG_VERSION_NUM >= 150000
  prev_shmem_request_hook = shmem_request_hook;
  shmem_request_hook = ShmemRequest;
#else
  ShmemRequest();
#endif

  prev_shmem_startup_hook = shmem_startup_hook;
  shmem_startup_hook = ShmemStartup;
}

void RegisterMaintenanceLauncher() {
  BackgroundWorker worker;
  MemSet(&worker, 0, sizeof(BackgroundWorker));
  worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
  worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
  snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_ducklake");
  snprintf(worker.bgw_function_name, BGW_MAXLEN, "ducklake_maintenance_launcher_main");
  snprintf(worker.bgw_name, BGW_MAXLEN, "ducklake maintenance launcher");
  snprintf(worker.bgw_type, BGW_MAXLEN, "ducklake maintenance launcher");
  worker.bgw_restart_time = 60; /* restart after 60s on crash */
  worker.bgw_main_arg = (Datum)0;
  worker.bgw_notify_pid = 0;
  RegisterBackgroundWorker(&worker);
}

} // namespace pgducklake
