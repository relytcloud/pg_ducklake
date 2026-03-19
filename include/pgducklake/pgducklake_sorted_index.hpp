/*
 * pgducklake_sorted_index.hpp
 *
 * Sorted index AM handler and CREATE/DROP INDEX interception helpers.
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

struct SortedIndexDrop {
  Oid index_oid;
  Oid table_oid;
};

std::vector<SortedIndexDrop> FindSortedIndexDrops(DropStmt *drop);

void HandleCreateSortedIndex(PlannedStmt *pstmt, const char *query_string,
                             bool read_only_tree, ProcessUtilityContext context,
                             ParamListInfo params,
                             struct QueryEnvironment *query_env,
                             DestReceiver *dest, QueryCompletion *qc,
                             ProcessUtility_hook_type prev_hook);

} // namespace pgducklake
