/*
 * pgducklake_direct_insert_stats.cpp
 *
 * @scope backend: register shmem request/startup hooks; read/write counters
 *
 * Shared-memory counter matrix for direct-insert outcomes. Mirrors the
 * shmem init pattern used by src/pgducklake_maintenance.cpp.
 */

#include "pgducklake/pgducklake_direct_insert_stats.hpp"

#include <string.h>

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"
}

namespace pgducklake {

namespace {

struct DirectInsertStatsShmemStruct {
  slock_t lock;
  uint64_t counters[DI_PAT_NUM][DI_R_NUM];
};

DirectInsertStatsShmemStruct *StatsShmem = nullptr;

#if PG_VERSION_NUM >= 150000
shmem_request_hook_type prev_shmem_request_hook = nullptr;
#endif
shmem_startup_hook_type prev_shmem_startup_hook = nullptr;

void ShmemRequest() {
#if PG_VERSION_NUM >= 150000
  if (prev_shmem_request_hook)
    prev_shmem_request_hook();
#endif
  RequestAddinShmemSpace(sizeof(DirectInsertStatsShmemStruct));
}

void ShmemStartup() {
  if (prev_shmem_startup_hook)
    prev_shmem_startup_hook();

  bool found;
  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
  StatsShmem = (DirectInsertStatsShmemStruct *)ShmemInitStruct("DuckLakeDirectInsertStats",
                                                               sizeof(DirectInsertStatsShmemStruct), &found);
  if (!found) {
    MemSet(StatsShmem, 0, sizeof(DirectInsertStatsShmemStruct));
    SpinLockInit(&StatsShmem->lock);
  }
  LWLockRelease(AddinShmemInitLock);
}

} // namespace

void InitDirectInsertStatsShmem() {
#if PG_VERSION_NUM >= 150000
  prev_shmem_request_hook = shmem_request_hook;
  shmem_request_hook = ShmemRequest;
#else
  ShmemRequest();
#endif
  prev_shmem_startup_hook = shmem_startup_hook;
  shmem_startup_hook = ShmemStartup;
}

void DirectInsertStatsBump(DirectInsertPattern pattern, DirectInsertReason reason) {
  if (!StatsShmem)
    return;
  if (pattern < 0 || pattern >= DI_PAT_NUM)
    return;
  if (reason < 0 || reason >= DI_R_NUM)
    return;
  SpinLockAcquire(&StatsShmem->lock);
  StatsShmem->counters[pattern][reason]++;
  SpinLockRelease(&StatsShmem->lock);
}

void DirectInsertStatsReset() {
  if (!StatsShmem)
    return;
  SpinLockAcquire(&StatsShmem->lock);
  memset(StatsShmem->counters, 0, sizeof(StatsShmem->counters));
  SpinLockRelease(&StatsShmem->lock);
}

uint64_t DirectInsertStatsRead(DirectInsertPattern pattern, DirectInsertReason reason) {
  if (!StatsShmem)
    return 0;
  if (pattern < 0 || pattern >= DI_PAT_NUM)
    return 0;
  if (reason < 0 || reason >= DI_R_NUM)
    return 0;
  SpinLockAcquire(&StatsShmem->lock);
  uint64_t value = StatsShmem->counters[pattern][reason];
  SpinLockRelease(&StatsShmem->lock);
  return value;
}

void DirectInsertStatsReadAll(uint64_t out[DI_PAT_NUM][DI_R_NUM]) {
  if (!StatsShmem) {
    memset(out, 0, sizeof(uint64_t) * DI_PAT_NUM * DI_R_NUM);
    return;
  }
  SpinLockAcquire(&StatsShmem->lock);
  memcpy(out, StatsShmem->counters, sizeof(uint64_t) * DI_PAT_NUM * DI_R_NUM);
  SpinLockRelease(&StatsShmem->lock);
}

const char *DirectInsertPatternName(DirectInsertPattern pattern) {
  switch (pattern) {
  case DI_PAT_MATCHED_UNNEST:
    return "matched_unnest";
  case DI_PAT_MATCHED_VALUES:
    return "matched_values";
  case DI_PAT_UNMATCHED:
    return "unmatched";
  default:
    return "unknown";
  }
}

const char *DirectInsertReasonName(DirectInsertReason reason) {
  switch (reason) {
  case DI_R_OK:
    return "ok";
  case DI_R_INVALID_RTE:
    return "invalid_rte";
  case DI_R_NO_INLINED_TABLE:
    return "no_inlined_table";
  case DI_R_SCHEMA_VERSION_MISMATCH:
    return "schema_version_mismatch";
  case DI_R_COL_TYPES_UNSUPPORTED:
    return "col_types_unsupported";
  case DI_R_GREATER_THAN_LIMIT:
    return "greater_than_limit";
  case DI_R_UNSUPPORTED_INSERT_SHAPE:
    return "unsupported_insert_shape";
  case DI_R_RETRY:
    return "retry";
  default:
    return "unknown";
  }
}

} // namespace pgducklake

extern "C" {

/*
 * Return the (pattern, reason) combos that actually occur:
 *   matched_unnest + ok
 *   matched_values + ok
 *   unmatched + every reason except ok
 *
 * Always emits the same 1 + 1 + (DI_R_NUM - 1) rows so the schema is
 * stable across resets (no NULL gaps in dashboards).
 */
PG_FUNCTION_INFO_V1(ducklake_direct_insert_stats);
Datum ducklake_direct_insert_stats(PG_FUNCTION_ARGS) {
  ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

  if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("set-valued function called in context that cannot accept a set")));
  if (!(rsinfo->allowedModes & SFRM_Materialize))
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("materialize mode required, but it is not allowed in this context")));

  TupleDesc tupdesc;
  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("return type must be a row type")));

  MemoryContext per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
  MemoryContext old_ctx = MemoryContextSwitchTo(per_query_ctx);

  Tuplestorestate *tupstore = tuplestore_begin_heap(true, false, work_mem);
  rsinfo->returnMode = SFRM_Materialize;
  rsinfo->setResult = tupstore;
  rsinfo->setDesc = BlessTupleDesc(tupdesc);

  MemoryContextSwitchTo(old_ctx);

  Datum values[3];
  bool nulls[3] = {false, false, false};

  /* Snapshot the matrix once under the spinlock to keep emitted rows
   * consistent and avoid N lock round-trips. */
  uint64_t snap[pgducklake::DI_PAT_NUM][pgducklake::DI_R_NUM];
  pgducklake::DirectInsertStatsReadAll(snap);

  /* Matched rows */
  for (int p = pgducklake::DI_PAT_MATCHED_UNNEST; p <= pgducklake::DI_PAT_MATCHED_VALUES; p++) {
    values[0] = CStringGetTextDatum(pgducklake::DirectInsertPatternName((pgducklake::DirectInsertPattern)p));
    values[1] = CStringGetTextDatum(pgducklake::DirectInsertReasonName(pgducklake::DI_R_OK));
    values[2] = Int64GetDatum((int64_t)snap[p][pgducklake::DI_R_OK]);
    tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);
  }

  /* Unmatched rows for every non-ok reason */
  for (int r = pgducklake::DI_R_OK + 1; r < pgducklake::DI_R_NUM; r++) {
    values[0] = CStringGetTextDatum(pgducklake::DirectInsertPatternName(pgducklake::DI_PAT_UNMATCHED));
    values[1] = CStringGetTextDatum(pgducklake::DirectInsertReasonName((pgducklake::DirectInsertReason)r));
    values[2] = Int64GetDatum((int64_t)snap[pgducklake::DI_PAT_UNMATCHED][r]);
    tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);
  }

  return (Datum)0;
}

PG_FUNCTION_INFO_V1(ducklake_reset_direct_insert_stats);
Datum ducklake_reset_direct_insert_stats(PG_FUNCTION_ARGS) {
  pgducklake::DirectInsertStatsReset();
  PG_RETURN_VOID();
}

} // extern "C"
