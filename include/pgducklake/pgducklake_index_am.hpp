/*
 * pgducklake_index_am.hpp
 *
 * Shared declarations for ducklake_sorted and ducklake_partitioned index AMs.
 * Both are catalog-only markers translated into DuckDB ALTER TABLE statements
 * by the utility hook in pgducklake_hooks.cpp.
 */

#pragma once

#include <vector>

extern "C" {
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "tcop/utility.h"
}

namespace pgducklake {

extern bool syncing_from_metadata;

struct IndexDrop {
  Oid index_oid;
  Oid table_oid;
};

std::vector<IndexDrop> FindIndexDropsByAM(DropStmt *drop, const char *am_name);

void HandleCreateSortedIndex(PlannedStmt *pstmt, const char *query_string,
                             bool read_only_tree, ProcessUtilityContext context,
                             ParamListInfo params,
                             struct QueryEnvironment *query_env,
                             DestReceiver *dest, QueryCompletion *qc,
                             ProcessUtility_hook_type prev_hook);

void HandleCreatePartitionedIndex(PlannedStmt *pstmt, const char *query_string,
                                  bool read_only_tree,
                                  ProcessUtilityContext context,
                                  ParamListInfo params,
                                  struct QueryEnvironment *query_env,
                                  DestReceiver *dest, QueryCompletion *qc,
                                  ProcessUtility_hook_type prev_hook);

} // namespace pgducklake
