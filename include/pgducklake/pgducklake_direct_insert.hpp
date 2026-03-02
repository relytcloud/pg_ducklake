/*
 * pgducklake_direct_insert.hpp
 *
 * Direct insert optimization for INSERT UNNEST pattern.
 *
 * Bypasses DuckDB execution by directly inserting array data into inlined data
 * tables via SPI, avoiding the overhead of DuckDB query parsing and data
 * conversion.
 *
 * Pattern detected: INSERT INTO <ducklake_table> SELECT UNNEST($1), UNNEST($2),
 * ...
 *
 * Optimization applies when:
 * - ducklake.enable_direct_insert = true
 * - Target table uses ducklake access method
 * - Query is parameterized INSERT with UNNEST of Param nodes
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

PlannedStmt *TryCreateDirectInsertPlan(Query *parse,
                                       ParamListInfo bound_params);

} // namespace pgducklake
