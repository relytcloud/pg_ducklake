/*
 * pgducklake_direct_insert.hpp
 *
 * Direct insert optimization for INSERT patterns into inlined DuckLake tables.
 *
 * Supported patterns:
 *   1. INSERT INTO <table> SELECT UNNEST($1), UNNEST($2), ...
 *      -- parameterized array bulk insert via SPI
 *   2. INSERT INTO <table> VALUES (const, ...), ...
 *      -- constant-value insert via table_multi_insert (heap AM)
 *
 * Both patterns bypass DuckDB execution and write directly to the inlined
 * data table when ducklake.enable_direct_insert = true.
 */

#pragma once

extern "C" {
#include "postgres.h"

#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"

#include "optimizer/planner.h"
}

namespace pgducklake {

struct ParamInfo {
  int param_id;
  Oid param_type;
  Oid element_type;
};

struct DirectInsertContext {
  Oid target_table_oid;
  uint64_t table_id;
  uint64_t schema_version;
  List *param_infos; // List of ParamInfo*
  int expected_row_count;
  List *target_col_names; // List of char*
  List *target_col_types; // List of Oid
};

// DirectInsertScanState is defined in the implementation file to avoid
// requiring full CustomScanState definition

void RegisterDirectInsertNode();

PlannedStmt *TryCreateDirectInsertPlan(Query *parse, ParamListInfo bound_params);

/* Clear session-level caches.  Must be called on DuckDB instance recycle
 * (recycle_ddb) since table_id/schema_version may change. */
void ResetDirectInsertCaches();

} // namespace pgducklake
