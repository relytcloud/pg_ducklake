/*
 * pgducklake_sorted_by.hpp
 *
 * Sorted index AM handler, CREATE/DROP INDEX interception, and pg_class
 * sync helpers for ducklake_sorted.
 */

#pragma once

#include <string>
#include <vector>

extern "C" {
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "tcop/utility.h"
}

namespace pgducklake {

extern bool syncing_from_metadata;

/* When true, the snapshot trigger skips sort-key sync because set_sort/
 * reset_sort will handle the pg_class index directly after the DuckDB call. */
extern bool sort_synced_from_pg;

struct SortedIndexDrop {
  Oid index_oid;
  Oid table_oid;
};

void HandleCreateSortedIndex(PlannedStmt *pstmt, const char *query_string,
                             bool read_only_tree, ProcessUtilityContext context,
                             ParamListInfo params,
                             struct QueryEnvironment *query_env,
                             DestReceiver *dest, QueryCompletion *qc,
                             ProcessUtility_hook_type prev_hook);

std::vector<SortedIndexDrop> FindSortedIndexDrops(DropStmt *drop);
void HandleDropSortedIndex(const std::vector<SortedIndexDrop> &drops);

struct SortedIndexCreate {
  Oid relid;
  std::string sort_spec;
};

/* Batch-sync ducklake_sorted pg_class indexes: create new ones and drop
 * reset ones.  Caller must have an active SPI connection with
 * syncing_from_metadata = true. */
void SyncSortedIndexes(const std::vector<SortedIndexCreate> &creates,
                       const std::vector<Oid> &resets);

/* Single-table helpers (thin wrappers around SyncSortedIndexes). */
void CreateSortedIndexForTable(Oid relid, const char *sort_spec);
void DropSortedIndexForTable(Oid relid);

} // namespace pgducklake
