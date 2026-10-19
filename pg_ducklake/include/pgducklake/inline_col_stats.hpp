#pragma once

// Table column stats (min/max/contains_null/contains_nan) for the rows one
// direct insert or COPY FROM STDIN writes into an inlined data table.

#include "pgddb/pg/declarations.hpp"

#include <cstdint>

namespace pgducklake {

struct InlineColStats;

// src_types are the PG types of the observed values, one per user column.
InlineColStats *BeginInlineColStats(uint64_t table_id, const char *inlined_table, int ncols, const Oid *src_types);

void ObserveInlineColStats(InlineColStats *stats, int col, Datum value, bool isnull);

// Merges the observed rows into ducklake_table_column_stats. Must run inside SPI.
void MergeInlineColStats(InlineColStats *stats, uint64_t table_id, bool initializing);

} // namespace pgducklake
