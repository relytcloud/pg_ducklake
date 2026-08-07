#pragma once

#include "pgddb/pg/declarations.hpp"

#include <cstdint>

namespace pgducklake {

struct NativeInlineColumnStat;
struct InlineColStats;

/* Create an accumulator for all live columns. num_cols is the number of live
 * top-level source columns. SPI must already be connected. */
InlineColStats *CreateInlineColStats(uint64_t table_id, int num_cols);

/* Bind a source PostgreSQL type, then fold the batch's source Datums. */
void SetupInlineColStatsColumn(InlineColStats *stats, int column, Oid source_type);
void ObserveInlineColStatsDatum(InlineColStats *stats, int column, Datum value);
void ObserveInlineColStatsNull(InlineColStats *stats, int column);

/* Finalize immutable, transaction-context-owned contributions. */
NativeInlineColumnStat *FinalizeInlineColStats(InlineColStats *stats, uint64_t *count);

} // namespace pgducklake
