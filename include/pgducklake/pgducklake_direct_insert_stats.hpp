/*
 * pgducklake_direct_insert_stats.hpp
 *
 * Shared-memory counters for direct-insert planner/exec outcomes.
 *
 * Two axes:
 *   pattern = { matched_unnest, matched_values, unmatched }
 *   reason  = { ok, invalid_rte, no_inlined_table, schema_version_mismatch,
 *               col_types_unsupported, greater_than_limit,
 *               unsupported_insert_shape, retry }
 *
 * Matched rows always pair with reason=ok. Unmatched rows carry a
 * specific reason. Gating (non-INSERT / in tx block / GUC off / non-
 * ducklake target) does NOT bump counters -- those queries are filtered
 * before direct-insert even considers them.
 */

#pragma once

#include <stdint.h>

extern "C" {
#include "postgres.h"

#include "fmgr.h"
}

namespace pgducklake {

enum DirectInsertPattern {
  DI_PAT_MATCHED_UNNEST = 0,
  DI_PAT_MATCHED_VALUES,
  DI_PAT_UNMATCHED,
  DI_PAT_NUM,
};

enum DirectInsertReason {
  DI_R_OK = 0,
  DI_R_INVALID_RTE,
  DI_R_NO_INLINED_TABLE,
  DI_R_SCHEMA_VERSION_MISMATCH,
  DI_R_COL_TYPES_UNSUPPORTED,
  DI_R_GREATER_THAN_LIMIT,
  DI_R_UNSUPPORTED_INSERT_SHAPE,
  DI_R_RETRY,
  DI_R_NUM,
};

/* Register shmem request + startup hooks. Call from _PG_init. */
void InitDirectInsertStatsShmem();

/* Bump counter; safe to call from any backend after ShmemStartup ran. */
void DirectInsertStatsBump(DirectInsertPattern pattern, DirectInsertReason reason);

/* Zero all counters. */
void DirectInsertStatsReset();

/* Read a single cell; used by tests and the SRF. */
uint64_t DirectInsertStatsRead(DirectInsertPattern pattern, DirectInsertReason reason);

/* Snapshot the whole matrix under one spinlock acquisition.  The
 * destination must be at least DI_PAT_NUM * DI_R_NUM uint64_t's. */
void DirectInsertStatsReadAll(uint64_t out[DI_PAT_NUM][DI_R_NUM]);

/* Human-readable enum labels (lowercase, snake_case). */
const char *DirectInsertPatternName(DirectInsertPattern pattern);
const char *DirectInsertReasonName(DirectInsertReason reason);

} // namespace pgducklake

extern "C" {

/* SRF: (pattern text, reason text, count bigint). */
Datum ducklake_direct_insert_stats(PG_FUNCTION_ARGS);

/* Zero all counters. */
Datum ducklake_reset_direct_insert_stats(PG_FUNCTION_ARGS);

} // extern "C"
