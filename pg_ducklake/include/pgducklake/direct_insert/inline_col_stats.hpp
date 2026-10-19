#pragma once

#include "pgddb/pg/declarations.hpp"

#include <cstdint>

namespace pgducklake {

struct NativeInlineColumnStat;
struct InlineColStats;

/*
 * The one parser for ducklake_column.column_type spellings.  type_id is a
 * duckdb::LogicalTypeId widened to int so callers need no DuckDB header;
 * is_json separates the two DuckLake types that share LogicalTypeId::VARCHAR.
 *
 * False for a spelling this build cannot parse -- callers that can decline
 * must treat that as "not our statement".  Raises only on out of memory.
 */
bool DuckLakeTypeIdentity(const char *ducklake_type, int *type_id_out, bool *is_json_out);

/* Create an accumulator for all live columns. num_cols is the number of live
 * top-level source columns. SPI must already be connected. */
InlineColStats *CreateInlineColStats(uint64_t table_id, int num_cols);

/* The DuckLake type of a top-level source column, from the catalog lookup
 * CreateInlineColStats already performs; false when that lookup came up short.
 * Writers need it because the inlined heap type alone is ambiguous -- a VARCHAR
 * column may hold a date or a list. */
bool InlineColStatsColumnTypeIdentity(const InlineColStats *stats, int column, int *type_id_out, bool *is_json_out);

/* Bind a source PostgreSQL type, then fold the batch's source Datums. */
void SetupInlineColStatsColumn(InlineColStats *stats, int column, Oid source_type);
void ObserveInlineColStatsDatum(InlineColStats *stats, int column, Datum value);
void ObserveInlineColStatsNull(InlineColStats *stats, int column);

/* Finalize immutable, transaction-context-owned contributions. */
NativeInlineColumnStat *FinalizeInlineColStats(InlineColStats *stats, uint64_t *count);

} // namespace pgducklake
