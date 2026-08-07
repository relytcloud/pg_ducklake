#pragma once

#include "pgducklake/direct_insert/native_writer_queue.hpp"

#include "pgddb/pg/declarations.hpp"

#include <cstdint>
#include <limits>

namespace pgducklake {

/*
 * Native inline writes split a statement into two phases:
 *
 * 1. The producer consumes VALUES, UNNEST, or COPY once and prewrites user
 *    rows into the PostgreSQL inline heap with candidate row and snapshot IDs.
 * 2. Publication claims the next DuckLake snapshot and updates its metadata.
 *    A lost claim is retried in a child transaction without replaying the
 *    producer. If the current IDs differ from the candidates, only the two
 *    system ID columns of the private prewritten rows are retagged.
 *
 * The reservation queue may predict IDs before the prewrite and order normal
 * publication, avoiding most claim conflicts and retags. Reservations are an
 * optimization, not a correctness lock: publication always validates current
 * transactional metadata and falls back to the retry/retag path when a
 * reservation is missing, stale, or invalidated.
 */

/* Nontransactional, process-shared native-writer work counters. */
enum NativeWriterCounter {
	NW_PAYLOAD_ROWS = 0,
	NW_PUBLICATION_ATTEMPTS,
	NW_SNAPSHOT_CLAIM_CONFLICTS,
	NW_ROWS_RETAGGED,
	NW_RETRY_EXHAUSTIONS,
	NW_COPY_ROWS_CONSUMED,
	NW_COUNTER_NUM,
};

void InitNativeWriterStatsShmem();
void NativeWriterStatsAdd(NativeWriterCounter counter, uint64_t amount = 1);
void NativeWriterStatsReset();
void NativeWriterStatsReadAll(uint64_t out[NW_COUNTER_NUM]);
const char *NativeWriterCounterName(NativeWriterCounter counter);

/* Immutable per-column contribution computed during the one-time prewrite. */
struct NativeInlineColumnStat {
	uint64_t column_id;
	const char *column_type;
	bool invalidate_all;
	bool bounds_safe;
	bool has_min;
	bool has_max;
	const char *min_value;
	const char *max_value;
	bool observed_null;
	bool nan_known;
	bool observed_nan;
};

/* Allocation and ownership state for one prewritten inline batch. Physical
 * heap pointers let retagging visit private tuples directly without a table
 * scan. They are bound to inline_table_oid and discarded at a fixed memory
 * cap; ownership fields remain available for the ordinary UPDATE fallback. */
struct NativeInlineRowPointer {
	uint32_t block_number;
	uint16_t offset_number;
};

struct NativeInlineWriteBatch {
	Oid target_table_oid;
	Oid inline_table_oid;
	uint64_t table_id;
	uint64_t schema_version;
	uint64_t start_snapshot_id;
	uint64_t candidate_snapshot_id;
	uint64_t candidate_row_id;
	uint32_t owner_xid;
	uint32_t owner_command_id;
	NativeWriterReservation reservation;
	void *row_pointer_context;
	NativeInlineRowPointer *row_pointers;
	uint64_t row_pointer_count;
	uint64_t row_pointer_capacity;
	bool row_pointer_tracking_enabled;
	uint64_t rows_inserted;
	const NativeInlineColumnStat *column_stats;
	uint64_t column_stats_count;
};

constexpr uint64_t NATIVE_WRITER_UNKNOWN_ROW_COUNT = std::numeric_limits<uint64_t>::max();

/* Validate DuckLake 1.0 and allocate candidate system values before prewrite. */
NativeInlineWriteBatch PrepareNativeInlineWrite(Oid target_table_oid, uint64_t expected_table_id,
                                                uint64_t expected_schema_version, uint64_t expected_row_count);
void BindNativeInlineWriteRelation(NativeInlineWriteBatch *batch, Relation relation);
void RecordNativeInlineWriteRows(NativeInlineWriteBatch *batch, TupleTableSlot **slots, uint64_t count);

/* Publish a prewritten batch, internally retrying snapshot-claim conflicts. */
void PublishNativeInlineWrite(const NativeInlineWriteBatch &batch);

} // namespace pgducklake
