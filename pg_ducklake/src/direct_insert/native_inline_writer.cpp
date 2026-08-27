#include "pgducklake/direct_insert/native_inline_writer.hpp"

#include "pgducklake/catalog_sync.hpp"
#include "pgducklake/duckdb_manager.hpp"
#include "pgducklake/guc.hpp"

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>
#include <new>
#include <string>

#include <common/ducklake_types.hpp>
#include <duckdb/common/exception.hpp>
#include <storage/ducklake_metadata_info.hpp>
#include <storage/ducklake_stats.hpp>
#include <storage/ducklake_transaction_changes.hpp>

extern "C" {
#include "postgres.h"

#include "access/heapam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"
#if PG_VERSION_NUM >= 180000
#include "utils/wait_classes.h"
#else
#include "utils/wait_event.h"
#endif
}

namespace pgducklake {

char *
OutputFunctionCallIso(FmgrInfo *flinfo, Datum value) {
	char *result = NULL;
	int saved_style = DateStyle;
	int saved_order = DateOrder;
	PG_TRY();
	{
		DateStyle = USE_ISO_DATES;
		DateOrder = DATEORDER_YMD;
		result = OutputFunctionCall(flinfo, value);
		DateStyle = saved_style;
		DateOrder = saved_order;
	}
	PG_CATCH();
	{
		DateStyle = saved_style;
		DateOrder = saved_order;
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
}

namespace {

struct NativeWriterStatsShmemStruct {
	slock_t lock;
	uint64_t counters[NW_COUNTER_NUM];
};

NativeWriterStatsShmemStruct *WriterStatsShmem = nullptr;

#if PG_VERSION_NUM >= 150000
shmem_request_hook_type prev_native_writer_shmem_request_hook = nullptr;
#endif
shmem_startup_hook_type prev_native_writer_shmem_startup_hook = nullptr;

void
NativeWriterShmemRequest() {
#if PG_VERSION_NUM >= 150000
	if (prev_native_writer_shmem_request_hook) {
		prev_native_writer_shmem_request_hook();
	}
#endif
	RequestAddinShmemSpace(sizeof(NativeWriterStatsShmemStruct));
}

void
NativeWriterShmemStartup() {
	if (prev_native_writer_shmem_startup_hook) {
		prev_native_writer_shmem_startup_hook();
	}

	bool found;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	WriterStatsShmem = (NativeWriterStatsShmemStruct *)ShmemInitStruct("DuckLakeNativeWriterStats",
	                                                                   sizeof(NativeWriterStatsShmemStruct), &found);
	if (!found) {
		MemSet(WriterStatsShmem, 0, sizeof(NativeWriterStatsShmemStruct));
		SpinLockInit(&WriterStatsShmem->lock);
	}
	LWLockRelease(AddinShmemInitLock);
}

struct PublicationState {
	uint64_t snapshot_id;
	uint64_t schema_version;
	uint64_t next_catalog_id;
	uint64_t next_file_id;
	uint64_t record_count;
	uint64_t next_row_id;
	uint64_t inline_schema_version;
	uint64_t change_row_count;
	uint64_t nonnull_change_count;
	bool has_table_stats;
	bool table_is_live;
	char *changes_made;
};

enum class ChangeCheckResult {
	OK,
	CONFLICT,
	INVALID,
	OUT_OF_MEMORY,
};

static uint64_t
GetRequiredProtocolUInt64(HeapTuple tuple, TupleDesc tupdesc, int column, const char *field_name) {
	bool isnull;
	Datum datum = SPI_getbinval(tuple, tupdesc, column, &isnull);
	if (isnull) {
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("DuckLake protocol field %s is NULL", field_name)));
	}
	int64 value = DatumGetInt64(datum);
	if (value < 0) {
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("DuckLake protocol field %s is negative", field_name)));
	}
	return static_cast<uint64_t>(value);
}

static PublicationState
ReadPublicationState(const NativeInlineWriteBatch &batch, uint64_t changes_after) {
	PublicationState state = {};
	MemoryContext caller_context = CurrentMemoryContext;

	SPI_connect();
	MemoryContext spi_context = CurrentMemoryContext;

	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
	                 "SELECT s.snapshot_id, s.schema_version, s.next_catalog_id, s.next_file_id, "
	                 "       (SELECT value FROM ducklake.ducklake_metadata WHERE key = 'version'), "
	                 "       EXISTS (SELECT 1 FROM ducklake.ducklake_table "
	                 "               WHERE table_id = %llu AND end_snapshot IS NULL), "
	                 "       (SELECT MAX(schema_version) FROM ducklake.ducklake_inlined_data_tables "
	                 "        WHERE table_id = %llu), "
	                 "       ts.table_id, ts.record_count, ts.next_row_id, "
	                 "       (SELECT COUNT(*) FROM ducklake.ducklake_snapshot_changes "
	                 "        WHERE snapshot_id > %llu), "
	                 "       (SELECT COUNT(changes_made) FROM ducklake.ducklake_snapshot_changes "
	                 "        WHERE snapshot_id > %llu), "
	                 "       COALESCE((SELECT STRING_AGG(changes_made, ',' ORDER BY snapshot_id) "
	                 "                 FROM ducklake.ducklake_snapshot_changes "
	                 "                 WHERE snapshot_id > %llu), '') "
	                 "FROM ducklake.ducklake_snapshot s "
	                 "LEFT JOIN ducklake.ducklake_table_stats ts ON ts.table_id = %llu "
	                 "WHERE s.snapshot_id = (SELECT MAX(snapshot_id) FROM ducklake.ducklake_snapshot)",
	                 (unsigned long long)batch.table_id, (unsigned long long)batch.table_id,
	                 (unsigned long long)changes_after, (unsigned long long)changes_after,
	                 (unsigned long long)changes_after, (unsigned long long)batch.table_id);

	/* read_only=false gives each retry a fresh READ COMMITTED SPI snapshot. */
	int ret = SPI_execute(query.data, false, 1);
	if (ret != SPI_OK_SELECT || SPI_processed != 1) {
		SPI_finish();
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
		                errmsg("native inline writer: could not read current DuckLake state")));
	}

	HeapTuple tuple = SPI_tuptable->vals[0];
	TupleDesc tupdesc = SPI_tuptable->tupdesc;
	state.snapshot_id = GetRequiredProtocolUInt64(tuple, tupdesc, 1, "snapshot_id");
	state.schema_version = GetRequiredProtocolUInt64(tuple, tupdesc, 2, "schema_version");
	state.next_catalog_id = GetRequiredProtocolUInt64(tuple, tupdesc, 3, "next_catalog_id");
	state.next_file_id = GetRequiredProtocolUInt64(tuple, tupdesc, 4, "next_file_id");

	bool isnull;
	char *version = SPI_getvalue(tuple, tupdesc, 5);
	if (!version || strcmp(version, "1.0") != 0) {
		MemoryContextSwitchTo(caller_context);
		char *found_version = pstrdup(version ? version : "<missing>");
		SPI_finish();
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("native inline writer does not support DuckLake metadata version %s", found_version),
		                errdetail("Only DuckLake metadata version 1.0 is supported.")));
	}

	Datum live_datum = SPI_getbinval(tuple, tupdesc, 6, &isnull);
	state.table_is_live = !isnull && DatumGetBool(live_datum);
	Datum inline_schema_datum = SPI_getbinval(tuple, tupdesc, 7, &isnull);
	if (isnull || DatumGetInt64(inline_schema_datum) < 0) {
		state.inline_schema_version = std::numeric_limits<uint64_t>::max();
	} else {
		state.inline_schema_version = static_cast<uint64_t>(DatumGetInt64(inline_schema_datum));
	}

	SPI_getbinval(tuple, tupdesc, 8, &isnull);
	state.has_table_stats = !isnull;
	if (state.has_table_stats) {
		state.record_count = GetRequiredProtocolUInt64(tuple, tupdesc, 9, "record_count");
		state.next_row_id = GetRequiredProtocolUInt64(tuple, tupdesc, 10, "next_row_id");
	}
	state.change_row_count = GetRequiredProtocolUInt64(tuple, tupdesc, 11, "snapshot change count");
	state.nonnull_change_count = GetRequiredProtocolUInt64(tuple, tupdesc, 12, "non-NULL snapshot change count");

	char *changes = SPI_getvalue(tuple, tupdesc, 13);
	MemoryContextSwitchTo(caller_context);
	state.changes_made = pstrdup(changes ? changes : "");
	MemoryContextSwitchTo(spi_context);

	if (!state.has_table_stats) {
		resetStringInfo(&query);
		appendStringInfo(&query,
		                 "SELECT COALESCE(MAX(row_id) + 1, 0) "
		                 "FROM ducklake.ducklake_inlined_data_%llu_%llu",
		                 (unsigned long long)batch.table_id, (unsigned long long)batch.schema_version);
		/* A child publication attempt sees its parent's prewritten tuples. They
		 * are not part of the committed allocation base when no stats row exists. */
		if (batch.rows_inserted > 0) {
			appendStringInfo(&query,
			                 " WHERE NOT (xmin = '%u'::xid AND cmin::text::bigint = %u "
			                 "AND begin_snapshot = %llu AND row_id >= %llu AND row_id < %llu)",
			                 batch.owner_xid, batch.owner_command_id, (unsigned long long)batch.candidate_snapshot_id,
			                 (unsigned long long)batch.candidate_row_id,
			                 (unsigned long long)(batch.candidate_row_id + batch.rows_inserted));
		}
		ret = SPI_execute(query.data, false, 1);
		if (ret != SPI_OK_SELECT || SPI_processed != 1) {
			SPI_finish();
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
			                errmsg("native inline writer: could not derive the initial row ID")));
		}
		state.next_row_id =
		    GetRequiredProtocolUInt64(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, "derived next_row_id");
		state.record_count = 0;
	}

	SPI_finish();
	MemoryContextSwitchTo(caller_context);
	return state;
}

static void
ValidateBinding(const NativeInlineWriteBatch &batch, const PublicationState &state) {
	if (!state.table_is_live || state.inline_schema_version != batch.schema_version) {
		ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
		                errmsg("native inline writer: target table changed during the statement"),
		                errdetail("Expected table %llu at inline schema version %llu.",
		                          (unsigned long long)batch.table_id, (unsigned long long)batch.schema_version)));
	}
}

static ChangeCheckResult
CheckInterveningChanges(const char *changes_made, uint64_t table_id) {
	try {
		auto changes = duckdb::SnapshotChangeInformation::ParseChangesMade(changes_made);
		duckdb::TableIndex target(table_id);
		if (changes.dropped_tables.find(target) != changes.dropped_tables.end() ||
		    changes.altered_tables.find(target) != changes.altered_tables.end() ||
		    changes.tables_deleted_from.find(target) != changes.tables_deleted_from.end() ||
		    changes.tables_deleted_inlined.find(target) != changes.tables_deleted_inlined.end()) {
			return ChangeCheckResult::CONFLICT;
		}
		return ChangeCheckResult::OK;
	} catch (const duckdb::OutOfMemoryException &) {
		return ChangeCheckResult::OUT_OF_MEMORY;
	} catch (const std::bad_alloc &) {
		return ChangeCheckResult::OUT_OF_MEMORY;
	} catch (...) {
		return ChangeCheckResult::INVALID;
	}
}

struct MergedColumnStat {
	uint64_t column_id;
	bool changed;
	bool has_min;
	bool has_max;
	char *min_value;
	char *max_value;
	size_t min_value_size;
	size_t max_value_size;
	bool has_contains_null;
	bool contains_null;
	bool has_contains_nan;
	bool contains_nan;
};

struct PreparedColumnStats {
	uint64_t count;
	MergedColumnStat *stats;
};

enum class ColumnStatMergeResult {
	OK,
	INVALID,
	OUT_OF_MEMORY,
};

static bool
CopyToMalloc(const std::string &value, char **result, size_t *result_size) {
	if (value.size() == std::numeric_limits<size_t>::max()) {
		return false;
	}
	*result_size = value.size() + 1;
	*result = (char *)std::malloc(*result_size);
	if (!*result) {
		return false;
	}
	memcpy(*result, value.c_str(), *result_size);
	return true;
}

/* Copy only after MergeColumnStat has destroyed every DuckDB object. The size
 * check prevents the oversized-allocation ERROR that MCXT_ALLOC_NO_OOM does
 * not suppress. */
static bool
MoveMergedBoundsToContext(MergedColumnStat *result, MemoryContext context) {
	char *min_value = NULL;
	char *max_value = NULL;
	if (result->min_value) {
		if (!AllocSizeIsValid(result->min_value_size)) {
			goto fail;
		}
		min_value = (char *)MemoryContextAllocExtended(context, result->min_value_size, MCXT_ALLOC_NO_OOM);
		if (!min_value) {
			goto fail;
		}
		memcpy(min_value, result->min_value, result->min_value_size);
	}
	if (result->max_value) {
		if (!AllocSizeIsValid(result->max_value_size)) {
			goto fail;
		}
		max_value = (char *)MemoryContextAllocExtended(context, result->max_value_size, MCXT_ALLOC_NO_OOM);
		if (!max_value) {
			goto fail;
		}
		memcpy(max_value, result->max_value, result->max_value_size);
	}
	std::free(result->min_value);
	std::free(result->max_value);
	result->min_value = min_value;
	result->max_value = max_value;
	return true;

fail:
	std::free(result->min_value);
	std::free(result->max_value);
	if (min_value) {
		pfree(min_value);
	}
	if (max_value) {
		pfree(max_value);
	}
	result->min_value = NULL;
	result->max_value = NULL;
	return false;
}

/* This function makes no PostgreSQL calls. Every C++ exception is contained
 * and all DuckDB objects are destroyed before control returns. */
static ColumnStatMergeResult
MergeColumnStat(const NativeInlineColumnStat &incoming, const char *min_value, const char *max_value,
                bool has_contains_null, bool contains_null, bool has_contains_nan, bool contains_nan,
                const char *extra_stats, bool initializing, MergedColumnStat *result) {
	if (incoming.invalidate_all) {
		return ColumnStatMergeResult::OK;
	}

	result->has_contains_null = initializing || has_contains_null;
	result->contains_null = contains_null || incoming.observed_null;
	result->has_contains_nan = has_contains_nan;
	result->contains_nan = contains_nan || (incoming.nan_known && incoming.observed_nan);
	if (!incoming.bounds_safe) {
		return ColumnStatMergeResult::OK;
	}

	try {
		duckdb::DuckLakeGlobalColumnStatsInfo persisted;
		persisted.column_id = duckdb::FieldIndex(incoming.column_id);
		if (min_value) {
			persisted.has_min = true;
			persisted.min_val = min_value;
		}
		if (max_value) {
			persisted.has_max = true;
			persisted.max_val = max_value;
		}
		persisted.has_contains_null = has_contains_null;
		persisted.contains_null = contains_null;
		persisted.has_contains_nan = has_contains_nan;
		persisted.contains_nan = contains_nan;
		if (extra_stats) {
			persisted.has_extra_stats = true;
			persisted.extra_stats = extra_stats;
		}

		auto type = duckdb::DuckLakeTypes::FromString(incoming.column_type);
		auto current = duckdb::DuckLakeColumnStats::FromGlobalStats(type, persisted);
		duckdb::DuckLakeColumnStats contribution(type);
		if (incoming.has_min) {
			contribution.min = duckdb::Value(incoming.min_value).DefaultCastAs(type).ToString();
			contribution.has_min = true;
		}
		if (incoming.has_max) {
			contribution.max = duckdb::Value(incoming.max_value).DefaultCastAs(type).ToString();
			contribution.has_max = true;
		}
		contribution.any_valid = contribution.has_min || contribution.has_max;
		contribution.has_null_count = true;
		contribution.null_count = incoming.observed_null ? 1 : 0;
		if (incoming.nan_known) {
			contribution.has_contains_nan = true;
			contribution.contains_nan = incoming.observed_nan;
		}
		current.MergeStats(contribution);

		/* Missing persisted sides are unknown over a pre-existing table. Only
		 * the first write can seed them from this batch alone. */
		result->has_min = (initializing || persisted.has_min) && current.has_min;
		result->has_max = (initializing || persisted.has_max) && current.has_max;
		if (result->has_min && !CopyToMalloc(current.min, &result->min_value, &result->min_value_size)) {
			return ColumnStatMergeResult::OUT_OF_MEMORY;
		}
		if (result->has_max && !CopyToMalloc(current.max, &result->max_value, &result->max_value_size)) {
			std::free(result->min_value);
			result->min_value = NULL;
			return ColumnStatMergeResult::OUT_OF_MEMORY;
		}
	} catch (const duckdb::OutOfMemoryException &) {
		return ColumnStatMergeResult::OUT_OF_MEMORY;
	} catch (const std::bad_alloc &) {
		return ColumnStatMergeResult::OUT_OF_MEMORY;
	} catch (...) {
		result->has_min = false;
		result->has_max = false;
		return ColumnStatMergeResult::INVALID;
	}
	return ColumnStatMergeResult::OK;
}

static const NativeInlineColumnStat *
FindIncomingColumnStat(const NativeInlineWriteBatch &batch, uint64_t column_id) {
	for (uint64_t i = 0; i < batch.column_stats_count; i++) {
		if (batch.column_stats[i].column_id == column_id) {
			return &batch.column_stats[i];
		}
	}
	return nullptr;
}

static bool
ColumnStatsChanged(const MergedColumnStat &merged, const char *min_value, const char *max_value, bool has_contains_null,
                   bool contains_null, bool has_contains_nan, bool contains_nan, const char *extra_stats) {
	return merged.has_min != (min_value != nullptr) || (merged.has_min && strcmp(merged.min_value, min_value) != 0) ||
	       merged.has_max != (max_value != nullptr) || (merged.has_max && strcmp(merged.max_value, max_value) != 0) ||
	       merged.has_contains_null != has_contains_null ||
	       (merged.has_contains_null && merged.contains_null != contains_null) ||
	       merged.has_contains_nan != has_contains_nan ||
	       (merged.has_contains_nan && merged.contains_nan != contains_nan) || extra_stats != nullptr;
}

static PreparedColumnStats
PrepareMergedColumnStats(const NativeInlineWriteBatch &batch, bool initializing) {
	PreparedColumnStats prepared = {};
	MemoryContext caller_context = CurrentMemoryContext;
	SPI_connect();
	MemoryContext spi_context = CurrentMemoryContext;

	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
	                 "SELECT column_id, min_value, max_value, contains_null, contains_nan, extra_stats "
	                 "FROM ducklake.ducklake_table_column_stats WHERE table_id = %llu",
	                 (unsigned long long)batch.table_id);
	int ret = SPI_execute(query.data, false, 0);
	if (ret != SPI_OK_SELECT) {
		SPI_finish();
		ereport(ERROR,
		        (errcode(ERRCODE_INTERNAL_ERROR), errmsg("native inline writer: could not read column statistics")));
	}

	prepared.count = initializing ? batch.column_stats_count : SPI_processed;
	MemoryContextSwitchTo(caller_context);
	if (prepared.count > 0) {
		prepared.stats = (MergedColumnStat *)palloc0(sizeof(MergedColumnStat) * prepared.count);
	}
	MemoryContextSwitchTo(spi_context);

	for (uint64_t i = 0; i < prepared.count; i++) {
		auto &merged = prepared.stats[i];
		const NativeInlineColumnStat *incoming;
		char *min_value = nullptr;
		char *max_value = nullptr;
		char *extra_stats = nullptr;
		bool has_contains_null = false;
		bool contains_null = false;
		bool has_contains_nan = false;
		bool contains_nan = false;

		if (initializing) {
			incoming = &batch.column_stats[i];
			merged.column_id = incoming->column_id;
		} else {
			HeapTuple tuple = SPI_tuptable->vals[i];
			TupleDesc tuple_desc = SPI_tuptable->tupdesc;
			bool isnull;
			merged.column_id = GetRequiredProtocolUInt64(tuple, tuple_desc, 1, "column_id");
			incoming = FindIncomingColumnStat(batch, merged.column_id);
			min_value = SPI_getvalue(tuple, tuple_desc, 2);
			max_value = SPI_getvalue(tuple, tuple_desc, 3);
			Datum contains_null_datum = SPI_getbinval(tuple, tuple_desc, 4, &isnull);
			has_contains_null = !isnull;
			contains_null = has_contains_null && DatumGetBool(contains_null_datum);
			Datum contains_nan_datum = SPI_getbinval(tuple, tuple_desc, 5, &isnull);
			has_contains_nan = !isnull;
			contains_nan = has_contains_nan && DatumGetBool(contains_nan_datum);
			extra_stats = SPI_getvalue(tuple, tuple_desc, 6);
		}

		ColumnStatMergeResult merge_result = ColumnStatMergeResult::OK;
		if (incoming) {
			merge_result = MergeColumnStat(*incoming, min_value, max_value, has_contains_null, contains_null,
			                               has_contains_nan, contains_nan, extra_stats, initializing, &merged);
		}
		if (merge_result == ColumnStatMergeResult::OK && !MoveMergedBoundsToContext(&merged, caller_context)) {
			merge_result = ColumnStatMergeResult::OUT_OF_MEMORY;
		}
		if (merge_result == ColumnStatMergeResult::OUT_OF_MEMORY) {
			SPI_finish();
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
		}
		merged.changed = ColumnStatsChanged(merged, min_value, max_value, has_contains_null, contains_null,
		                                    has_contains_nan, contains_nan, extra_stats);
		MemoryContextSwitchTo(spi_context);
	}
	SPI_finish();
	MemoryContextSwitchTo(caller_context);
	return prepared;
}

static void
FreePreparedColumnStats(PreparedColumnStats *prepared) {
	for (uint64_t i = 0; i < prepared->count; i++) {
		if (prepared->stats[i].min_value) {
			pfree(prepared->stats[i].min_value);
		}
		if (prepared->stats[i].max_value) {
			pfree(prepared->stats[i].max_value);
		}
	}
	if (prepared->stats) {
		pfree(prepared->stats);
	}
	*prepared = {};
}

static void
InjectNativeWriterFault(NativeWriterTestFault fault, const char *point) {
	if (unlikely(native_writer_test_fault == fault)) {
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("native inline writer test fault %s", point)));
	}
}

static void
ApplyMergedColumnStats(const NativeInlineWriteBatch &batch, const PreparedColumnStats &prepared) {
	StringInfoData values;
	initStringInfo(&values);
	uint64_t changed_count = 0;
	for (uint64_t i = 0; i < prepared.count; i++) {
		const auto &merged = prepared.stats[i];
		if (!merged.changed) {
			continue;
		}
		char *min_literal = merged.has_min ? quote_literal_cstr(merged.min_value) : pstrdup("NULL");
		char *max_literal = merged.has_max ? quote_literal_cstr(merged.max_value) : pstrdup("NULL");
		const char *null_literal = merged.has_contains_null ? (merged.contains_null ? "true" : "false") : "NULL";
		const char *nan_literal = merged.has_contains_nan ? (merged.contains_nan ? "true" : "false") : "NULL";
		appendStringInfo(&values,
		                 changed_count == 0 ? "(%llu::bigint, %s::text, %s::text, %s::boolean, %s::boolean)"
		                                    : ", (%llu, %s, %s, %s, %s)",
		                 (unsigned long long)merged.column_id, min_literal, max_literal, null_literal, nan_literal);
		pfree(min_literal);
		pfree(max_literal);
		changed_count++;
	}
	if (changed_count == 0) {
		return;
	}

	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query, R"(
		UPDATE ducklake.ducklake_table_column_stats s
		SET min_value = v.min_value, max_value = v.max_value,
		    contains_null = v.contains_null, contains_nan = v.contains_nan,
		    extra_stats = NULL
		FROM (VALUES %s) AS v(column_id, min_value, max_value, contains_null, contains_nan)
		WHERE s.table_id = %llu AND s.column_id = v.column_id)",
	                 values.data, (unsigned long long)batch.table_id);
	int ret = SPI_execute(query.data, false, 0);
	if (ret != SPI_OK_UPDATE || SPI_processed != changed_count) {
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
		                errmsg("native inline writer column statistics changed concurrently")));
	}
}

constexpr Size MAX_TRACKED_ROW_POINTER_BYTES = 512 * 1024;
constexpr uint64_t MAX_TRACKED_ROW_POINTERS = MAX_TRACKED_ROW_POINTER_BYTES / sizeof(NativeInlineRowPointer);

static bool
RelationSupportsInplaceRetag(Relation relation) {
	List *indexes = RelationGetIndexList(relation);
	TupleDesc tuple_desc = RelationGetDescr(relation);
	TupleConstr *constraints = tuple_desc->constr;
	bool unsafe_constraints = constraints && (constraints->num_check > 0 || constraints->has_generated_stored);
#if PG_VERSION_NUM >= 180000
	unsafe_constraints = unsafe_constraints || (constraints && constraints->has_generated_virtual);
#endif
	for (int attribute = 0; !unsafe_constraints && attribute < tuple_desc->natts; attribute++) {
		unsafe_constraints = TupleDescAttr(tuple_desc, attribute)->attgenerated != '\0';
	}
	bool safe = relation->rd_rel->relkind == RELKIND_RELATION && relation->rd_rel->relam == HEAP_TABLE_AM_OID &&
	            indexes == NIL && !relation->rd_rel->relhastriggers && relation->trigdesc == nullptr &&
	            !relation->rd_rel->relhasrules && relation->rd_rules == nullptr && !relation->rd_rel->relrowsecurity &&
	            !unsafe_constraints && !RelationIsLogicallyLogged(relation);
	list_free(indexes);
	return safe;
}

#if PG_VERSION_NUM >= 180000
static void
ReleaseInplaceRetagBuffer(void *argument) {
	ReleaseBuffer(*static_cast<Buffer *>(argument));
}
#endif

/* The retained RowExclusiveLock prevents a rewrite from invalidating these
 * TIDs. The tuples are still invisible outside this transaction. */
static void
RetagPrewrittenRowsInplace(const NativeInlineWriteBatch &batch, Relation relation, uint64_t final_snapshot_id,
                           uint64_t final_row_id) {
	TupleDesc tuple_desc = RelationGetDescr(relation);
	Datum *replacement_values = (Datum *)palloc0(sizeof(Datum) * tuple_desc->natts);
	bool *replacement_nulls = (bool *)palloc0(sizeof(bool) * tuple_desc->natts);
	bool *replace = (bool *)palloc0(sizeof(bool) * tuple_desc->natts);
	replace[0] = true;
	replace[1] = true;

	for (uint64_t index = 0; index < batch.row_pointer_count; index++) {
		HeapTupleData tuple = {};
		ItemPointerSet(&tuple.t_self, batch.row_pointers[index].block_number, batch.row_pointers[index].offset_number);
		Buffer buffer = InvalidBuffer;
#if PG_VERSION_NUM >= 150000
		bool found = heap_fetch(relation, SnapshotSelf, &tuple, &buffer, true);
#else
		bool found = heap_fetch(relation, SnapshotSelf, &tuple, &buffer);
#endif
		if (!found) {
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
			                errmsg("native inline writer: prewritten row disappeared before retag")));
		}

		bool row_id_is_null;
		bool snapshot_is_null;
		Datum row_id_datum = heap_getattr(&tuple, 1, tuple_desc, &row_id_is_null);
		Datum snapshot_datum = heap_getattr(&tuple, 2, tuple_desc, &snapshot_is_null);
		uint64_t row_id = row_id_is_null ? 0 : static_cast<uint64_t>(DatumGetInt64(row_id_datum));
		uint64_t snapshot_id = snapshot_is_null ? 0 : static_cast<uint64_t>(DatumGetInt64(snapshot_datum));
		if (row_id_is_null || snapshot_is_null || HeapTupleHeaderGetRawXmin(tuple.t_data) != batch.owner_xid ||
		    HeapTupleHeaderGetRawCommandId(tuple.t_data) != batch.owner_command_id ||
		    snapshot_id != batch.candidate_snapshot_id || row_id < batch.candidate_row_id ||
		    row_id - batch.candidate_row_id >= batch.rows_inserted) {
			ReleaseBuffer(buffer);
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
			                errmsg("native inline writer: prewritten row identity changed before retag")));
		}

		replacement_values[0] = Int64GetDatum(static_cast<int64_t>(final_row_id + row_id - batch.candidate_row_id));
		replacement_values[1] = Int64GetDatum(static_cast<int64_t>(final_snapshot_id));
		HeapTuple replacement = heap_modify_tuple(&tuple, tuple_desc, replacement_values, replacement_nulls, replace);
		ItemPointerCopy(&tuple.t_self, &replacement->t_self);
#if PG_VERSION_NUM >= 180000
		if (!heap_inplace_lock(relation, &tuple, buffer, ReleaseInplaceRetagBuffer, &buffer)) {
			heap_freetuple(replacement);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
			                errmsg("native inline writer: could not lock prewritten row for retag")));
		}
		heap_inplace_update_and_unlock(relation, &tuple, replacement, buffer);
		ReleaseBuffer(buffer);
#else
		/* heap_inplace_update() is the cross-minor API on PostgreSQL 14-17.
		 * The lock/update split was added in back-branch minor releases. */
		ReleaseBuffer(buffer);
		heap_inplace_update(relation, replacement);
#endif
		heap_freetuple(replacement);
	}
	pfree(replacement_values);
	pfree(replacement_nulls);
	pfree(replace);
}

static void
RetagPrewrittenRows(const NativeInlineWriteBatch &batch, uint64_t final_snapshot_id, uint64_t final_row_id) {
	if (!OidIsValid(batch.inline_table_oid)) {
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
		                errmsg("native inline writer: prewritten rows are not bound to an inline relation")));
	}

	/* The producer retained this lock with table_close(..., NoLock). */
	Relation relation = table_open(batch.inline_table_oid, NoLock);
	if (batch.row_pointer_tracking_enabled && batch.row_pointer_count == batch.rows_inserted &&
	    RelationSupportsInplaceRetag(relation)) {
		RetagPrewrittenRowsInplace(batch, relation, final_snapshot_id, final_row_id);
		table_close(relation, NoLock);
		return;
	}

	char *namespace_name = get_namespace_name(RelationGetNamespace(relation));
	char *qualified_name = quote_qualified_identifier(namespace_name, RelationGetRelationName(relation));
	table_close(relation, NoLock);

	int64_t row_delta = static_cast<int64_t>(final_row_id) - static_cast<int64_t>(batch.candidate_row_id);
	StringInfoData retag_query;
	initStringInfo(&retag_query);
	appendStringInfo(&retag_query,
	                 "UPDATE %s SET row_id = row_id + %lld, begin_snapshot = %llu "
	                 "WHERE xmin = '%u'::xid AND cmin::text::bigint = %u AND begin_snapshot = %llu "
	                 "AND row_id >= %llu AND row_id < %llu",
	                 qualified_name, (long long)row_delta, (unsigned long long)final_snapshot_id, batch.owner_xid,
	                 batch.owner_command_id, (unsigned long long)batch.candidate_snapshot_id,
	                 (unsigned long long)batch.candidate_row_id,
	                 (unsigned long long)(batch.candidate_row_id + batch.rows_inserted));
	int ret = SPI_execute(retag_query.data, false, 0);
	if (ret != SPI_OK_UPDATE || SPI_processed != batch.rows_inserted) {
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
		                errmsg("native inline writer retagged %llu rows, expected %llu",
		                       (unsigned long long)SPI_processed, (unsigned long long)batch.rows_inserted)));
	}
}

static bool
RunPublicationAttempt(const NativeInlineWriteBatch &batch, const PublicationState &state,
                      const PreparedColumnStats &column_stats, uint64_t final_snapshot_id, uint64_t final_row_id) {
	MemoryContext old_context = CurrentMemoryContext;
	ResourceOwner old_owner = CurrentResourceOwner;
	bool old_skip_snapshot_sync = skip_snapshot_sync;
	volatile bool claimed = false;

	StringInfoData claim_query;
	initStringInfo(&claim_query);
	appendStringInfo(&claim_query,
	                 "INSERT INTO ducklake.ducklake_snapshot "
	                 "(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) "
	                 "VALUES (%llu, NOW(), %llu, %llu, %llu) "
	                 "ON CONFLICT (snapshot_id) DO NOTHING RETURNING snapshot_id",
	                 (unsigned long long)final_snapshot_id, (unsigned long long)state.schema_version,
	                 (unsigned long long)state.next_catalog_id, (unsigned long long)(state.next_file_id + 1));

	SetAllowSubtransaction(true);
	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(old_context);

	PG_TRY();
	{
		SPI_connect();

		/* The snapshot claim is the first protocol mutation in this attempt. */
		skip_snapshot_sync = true;
		int ret = SPI_execute(claim_query.data, false, 1);
		skip_snapshot_sync = old_skip_snapshot_sync;
		if (ret != SPI_OK_INSERT_RETURNING) {
			ereport(ERROR,
			        (errcode(ERRCODE_INTERNAL_ERROR), errmsg("native inline writer: snapshot claim failed: %d", ret)));
		}
		claimed = SPI_processed == 1;
		if (claimed) {
			InjectNativeWriterFault(NATIVE_WRITER_TEST_FAULT_AFTER_CLAIM, "after claim");
		}

		if (claimed && (final_snapshot_id != batch.candidate_snapshot_id || final_row_id != batch.candidate_row_id)) {
			RetagPrewrittenRows(batch, final_snapshot_id, final_row_id);
			NativeWriterStatsAdd(NW_ROWS_RETAGGED, batch.rows_inserted);
			InjectNativeWriterFault(NATIVE_WRITER_TEST_FAULT_AFTER_RETAG, "after retag");
		}

		if (claimed) {
			StringInfoData stats_query;
			initStringInfo(&stats_query);
			if (state.has_table_stats) {
				appendStringInfo(&stats_query,
				                 "UPDATE ducklake.ducklake_table_stats "
				                 "SET record_count = %llu, next_row_id = %llu "
				                 "WHERE table_id = %llu",
				                 (unsigned long long)(state.record_count + batch.rows_inserted),
				                 (unsigned long long)(final_row_id + batch.rows_inserted),
				                 (unsigned long long)batch.table_id);
				ret = SPI_execute(stats_query.data, false, 0);
				if (ret != SPI_OK_UPDATE || SPI_processed != 1) {
					ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
					                errmsg("native inline writer: table statistics row disappeared")));
				}
				InjectNativeWriterFault(NATIVE_WRITER_TEST_FAULT_AFTER_TABLE_STATS, "after table stats");

			} else {
				appendStringInfo(&stats_query,
				                 "INSERT INTO ducklake.ducklake_table_stats "
				                 "(table_id, record_count, next_row_id, file_size_bytes) "
				                 "VALUES (%llu, %llu, %llu, 0)",
				                 (unsigned long long)batch.table_id, (unsigned long long)batch.rows_inserted,
				                 (unsigned long long)(final_row_id + batch.rows_inserted));
				ret = SPI_execute(stats_query.data, false, 0);
				if (ret != SPI_OK_INSERT) {
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					                errmsg("native inline writer: could not create table statistics")));
				}
				InjectNativeWriterFault(NATIVE_WRITER_TEST_FAULT_AFTER_TABLE_STATS, "after table stats");

				resetStringInfo(&stats_query);
				appendStringInfo(
				    &stats_query,
				    "INSERT INTO ducklake.ducklake_table_column_stats "
				    "(table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) "
				    "SELECT %llu, column_id, NULL, NULL, NULL, NULL, NULL "
				    "FROM ducklake.ducklake_column "
				    "WHERE table_id = %llu AND end_snapshot IS NULL",
				    (unsigned long long)batch.table_id, (unsigned long long)batch.table_id);
				ret = SPI_execute(stats_query.data, false, 0);
				if (ret != SPI_OK_INSERT) {
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
					                errmsg("native inline writer: could not create column statistics")));
				}
			}

			ApplyMergedColumnStats(batch, column_stats);
			InjectNativeWriterFault(NATIVE_WRITER_TEST_FAULT_AFTER_COLUMN_STATS, "after column stats");

			StringInfoData changes_query;
			initStringInfo(&changes_query);
			appendStringInfo(&changes_query,
			                 "INSERT INTO ducklake.ducklake_snapshot_changes "
			                 "(snapshot_id, changes_made, author, commit_message, commit_extra_info) "
			                 "VALUES (%llu, 'inlined_insert:%llu', NULL, NULL, NULL)",
			                 (unsigned long long)final_snapshot_id, (unsigned long long)batch.table_id);
			ret = SPI_execute(changes_query.data, false, 0);
			if (ret != SPI_OK_INSERT) {
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				                errmsg("native inline writer: could not write snapshot changes")));
			}
			InjectNativeWriterFault(NATIVE_WRITER_TEST_FAULT_AFTER_CHANGE_RECORD, "after change record");
		}

		SPI_finish();
		ReleaseCurrentSubTransaction();
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(old_context);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();
		skip_snapshot_sync = old_skip_snapshot_sync;
		/* Child-owned SPI resources are released by subtransaction rollback. */
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(old_context);
		CurrentResourceOwner = old_owner;
		SetAllowSubtransaction(false);
		ReThrowError(edata);
	}
	PG_END_TRY();

	skip_snapshot_sync = old_skip_snapshot_sync;
	MemoryContextSwitchTo(old_context);
	CurrentResourceOwner = old_owner;
	SetAllowSubtransaction(false);
	return claimed;
}

static void
RetryBackoff(int retry_number) {
	if (native_writer_retry_wait_ms <= 0) {
		CHECK_FOR_INTERRUPTS();
		return;
	}

	double delay = native_writer_retry_wait_ms * std::pow(native_writer_retry_backoff, retry_number);
	if (delay > 60000.0) {
		delay = 60000.0;
	}
	uint32_t hash = static_cast<uint32_t>(MyProcPid) * 1103515245U + static_cast<uint32_t>(retry_number) * 12345U;
	double jitter = 0.5 + static_cast<double>(hash & 0xffffU) / 131070.0;
	long wait_ms = static_cast<long>(std::ceil(delay * jitter));
	TimestampTz deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), wait_ms);
	/* Latch signals can wake us without cancellation; honor the deadline. */
	for (;;) {
		long remaining = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);
		if (remaining <= 0) {
			break;
		}
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, remaining, PG_WAIT_EXTENSION);
	}
	CHECK_FOR_INTERRUPTS();
}

} // namespace

void
InitNativeWriterStatsShmem() {
#if PG_VERSION_NUM >= 150000
	prev_native_writer_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = NativeWriterShmemRequest;
#else
	NativeWriterShmemRequest();
#endif
	prev_native_writer_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = NativeWriterShmemStartup;
}

void
NativeWriterStatsAdd(NativeWriterCounter counter, uint64_t amount) {
	if (!WriterStatsShmem || counter < 0 || counter >= NW_COUNTER_NUM || amount == 0) {
		return;
	}
	const uint64_t max_counter = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
	SpinLockAcquire(&WriterStatsShmem->lock);
	uint64_t &value = WriterStatsShmem->counters[counter];
	value = value >= max_counter || amount > max_counter - value ? max_counter : value + amount;
	SpinLockRelease(&WriterStatsShmem->lock);
}

void
NativeWriterStatsReset() {
	if (!WriterStatsShmem) {
		return;
	}
	SpinLockAcquire(&WriterStatsShmem->lock);
	MemSet(WriterStatsShmem->counters, 0, sizeof(WriterStatsShmem->counters));
	SpinLockRelease(&WriterStatsShmem->lock);
}

void
NativeWriterStatsReadAll(uint64_t out[NW_COUNTER_NUM]) {
	if (!WriterStatsShmem) {
		MemSet(out, 0, sizeof(uint64_t) * NW_COUNTER_NUM);
		return;
	}
	SpinLockAcquire(&WriterStatsShmem->lock);
	memcpy(out, WriterStatsShmem->counters, sizeof(WriterStatsShmem->counters));
	SpinLockRelease(&WriterStatsShmem->lock);
}

const char *
NativeWriterCounterName(NativeWriterCounter counter) {
	switch (counter) {
	case NW_PAYLOAD_ROWS:
		return "payload_rows";
	case NW_PUBLICATION_ATTEMPTS:
		return "publication_attempts";
	case NW_SNAPSHOT_CLAIM_CONFLICTS:
		return "snapshot_claim_conflicts";
	case NW_ROWS_RETAGGED:
		return "rows_retagged";
	case NW_RETRY_EXHAUSTIONS:
		return "retry_exhaustions";
	case NW_COPY_ROWS_CONSUMED:
		return "copy_rows_consumed";
	default:
		return "unknown";
	}
}

NativeInlineWriteBatch
PrepareNativeInlineWrite(Oid target_table_oid, uint64_t expected_table_id, uint64_t expected_schema_version,
                         uint64_t expected_row_count) {
	if (IsTransactionBlock() || IsolationUsesXactSnapshot()) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("native inline writer requires an implicit READ COMMITTED transaction")));
	}
	if (GetCurrentTransactionNestLevel() != 1) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("native inline writer cannot run inside a PostgreSQL subtransaction")));
	}

	NativeInlineWriteBatch batch = {};
	batch.target_table_oid = target_table_oid;
	batch.table_id = expected_table_id;
	batch.row_pointer_context = CurrentMemoryContext;
	batch.row_pointer_tracking_enabled = true;
	batch.schema_version = expected_schema_version;

	PublicationState state = ReadPublicationState(batch, static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
	ValidateBinding(batch, state);
	pfree(state.changes_made);

	if (state.snapshot_id >= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
	    state.next_row_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
		ereport(ERROR,
		        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("native inline writer exhausted DuckLake IDs")));
	}

	batch.start_snapshot_id = state.snapshot_id;
	batch.candidate_snapshot_id = state.snapshot_id + 1;
	batch.candidate_row_id = state.next_row_id;
	batch.owner_xid = static_cast<uint32_t>(GetCurrentTransactionId());
	batch.owner_command_id = static_cast<uint32_t>(GetCurrentCommandId(true));

	if (native_writer_reservation_queue) {
		NativeWriterReservationResult reservation = {};
		bool row_count_known = expected_row_count != NATIVE_WRITER_UNKNOWN_ROW_COUNT;
		bool reserved = ReserveNativeWriterPublication(batch.table_id, state.snapshot_id, state.next_row_id,
		                                               batch.owner_xid, batch.owner_command_id, row_count_known,
		                                               row_count_known ? expected_row_count : 0, &reservation);
		batch.reservation = reservation.reservation;
		if (reserved) {
			batch.candidate_snapshot_id = reservation.candidate_snapshot_id;
			batch.candidate_row_id = reservation.candidate_row_id;
		}
	}
	return batch;
}

void
BindNativeInlineWriteRelation(NativeInlineWriteBatch *batch, Relation relation) {
	Oid relation_oid = RelationGetRelid(relation);
	if (OidIsValid(batch->inline_table_oid) && batch->inline_table_oid != relation_oid) {
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
		                errmsg("native inline writer: row pointers span multiple inline relations")));
	}
	batch->inline_table_oid = relation_oid;
	batch->row_pointer_tracking_enabled = RelationSupportsInplaceRetag(relation);
}

void
RecordNativeInlineWriteRows(NativeInlineWriteBatch *batch, TupleTableSlot **slots, uint64_t count) {
	if (count == 0 || !batch->row_pointer_tracking_enabled) {
		return;
	}
	if (count > MAX_TRACKED_ROW_POINTERS - batch->row_pointer_count) {
		if (batch->row_pointers) {
			pfree(batch->row_pointers);
		}
		batch->row_pointers = nullptr;
		batch->row_pointer_count = 0;
		batch->row_pointer_capacity = 0;
		batch->row_pointer_tracking_enabled = false;
		return;
	}

	uint64_t required = batch->row_pointer_count + count;
	MemoryContext old_context = MemoryContextSwitchTo(static_cast<MemoryContext>(batch->row_pointer_context));
	if (required > batch->row_pointer_capacity) {
		uint64_t capacity = batch->row_pointer_capacity ? batch->row_pointer_capacity : 1024;
		while (capacity < required) {
			capacity = Min(capacity * 2, MAX_TRACKED_ROW_POINTERS);
		}
		Size allocation_size = static_cast<Size>(capacity * sizeof(NativeInlineRowPointer));
		batch->row_pointers = batch->row_pointers
		                          ? (NativeInlineRowPointer *)repalloc(batch->row_pointers, allocation_size)
		                          : (NativeInlineRowPointer *)palloc(allocation_size);
		batch->row_pointer_capacity = capacity;
	}

	for (uint64_t index = 0; index < count; index++) {
		if (!ItemPointerIsValid(&slots[index]->tts_tid)) {
			pfree(batch->row_pointers);
			batch->row_pointers = nullptr;
			batch->row_pointer_count = 0;
			batch->row_pointer_capacity = 0;
			batch->row_pointer_tracking_enabled = false;
			MemoryContextSwitchTo(old_context);
			return;
		}
		auto &pointer = batch->row_pointers[batch->row_pointer_count++];
		pointer.block_number = ItemPointerGetBlockNumber(&slots[index]->tts_tid);
		pointer.offset_number = ItemPointerGetOffsetNumber(&slots[index]->tts_tid);
	}
	MemoryContextSwitchTo(old_context);
}

void
PublishNativeInlineWrite(const NativeInlineWriteBatch &batch) {
	if (batch.rows_inserted == 0) {
		return;
	}
	if (batch.candidate_row_id + batch.rows_inserted < batch.candidate_row_id ||
	    batch.candidate_row_id + batch.rows_inserted > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("native inline writer exhausted row IDs")));
	}
	InjectNativeWriterFault(NATIVE_WRITER_TEST_FAULT_AFTER_PREWRITE, "after prewrite");

	bool reservation_valid = CompleteNativeWriterReservation(batch.reservation, batch.rows_inserted);
	if (reservation_valid) {
		reservation_valid = WaitForNativeWriterPublication(batch.reservation);
	}

	/* Make the parent transaction's prewritten tuples visible to child attempts. */
	CommandCounterIncrement();

	for (int attempt = 0; attempt <= native_writer_max_retry_count; attempt++) {
		PublicationState state = ReadPublicationState(batch, batch.start_snapshot_id);
		ValidateBinding(batch, state);

		if (state.snapshot_id < batch.start_snapshot_id ||
		    state.change_row_count != state.snapshot_id - batch.start_snapshot_id ||
		    state.nonnull_change_count != state.snapshot_id - batch.start_snapshot_id) {
			pfree(state.changes_made);
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
			                errmsg("native inline writer: incomplete DuckLake snapshot change history")));
		}
		if (test_native_writer_force_client_retry_before_rebase && state.snapshot_id != batch.start_snapshot_id) {
			pfree(state.changes_made);
			NativeWriterStatsAdd(NW_RETRY_EXHAUSTIONS);
			ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
			                errmsg("native inline writer test forced client retry before rebase")));
		}

		ChangeCheckResult change_result = CheckInterveningChanges(state.changes_made, batch.table_id);
		pfree(state.changes_made);
		if (change_result == ChangeCheckResult::CONFLICT) {
			ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
			                errmsg("native inline writer: conflicting change to table %llu",
			                       (unsigned long long)batch.table_id)));
		}
		if (change_result == ChangeCheckResult::INVALID) {
			ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
			                errmsg("native inline writer: invalid DuckLake snapshot change record")));
		}
		if (change_result == ChangeCheckResult::OUT_OF_MEMORY) {
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
		}

		uint64_t max_protocol_id = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
		if (state.snapshot_id >= max_protocol_id || state.next_file_id >= max_protocol_id ||
		    batch.rows_inserted > max_protocol_id || state.next_row_id > max_protocol_id - batch.rows_inserted ||
		    state.record_count > max_protocol_id - batch.rows_inserted) {
			ereport(ERROR,
			        (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("native inline writer exhausted DuckLake IDs")));
		}
		uint64_t final_snapshot_id = state.snapshot_id + 1;
		uint64_t final_row_id = state.next_row_id;
		if (reservation_valid && !ValidateNativeWriterReservation(batch.reservation, final_snapshot_id, final_row_id)) {
			reservation_valid = false;
		}
		PreparedColumnStats column_stats = PrepareMergedColumnStats(batch, !state.has_table_stats);

		NativeWriterStatsAdd(NW_PUBLICATION_ATTEMPTS);
		if (RunPublicationAttempt(batch, state, column_stats, final_snapshot_id, final_row_id)) {
			FreePreparedColumnStats(&column_stats);
			MarkNativeWriterReservationPublished(batch.reservation);
			InjectNativeWriterFault(NATIVE_WRITER_TEST_FAULT_AFTER_PUBLICATION, "after publication");
			elog(DEBUG1, "native inline writer published %llu rows in snapshot %llu after %d retries",
			     (unsigned long long)batch.rows_inserted, (unsigned long long)final_snapshot_id, attempt);
			return;
		}
		FreePreparedColumnStats(&column_stats);
		NativeWriterStatsAdd(NW_SNAPSHOT_CLAIM_CONFLICTS);
		if (reservation_valid) {
			InvalidateNativeWriterReservation(batch.reservation);
			reservation_valid = false;
		}

		if (attempt == native_writer_max_retry_count) {
			break;
		}
		RetryBackoff(attempt);
	}

	NativeWriterStatsAdd(NW_RETRY_EXHAUSTIONS);
	ereport(ERROR,
	        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
	         errmsg("native inline writer exceeded %d snapshot publication retries", native_writer_max_retry_count),
	         errhint("Increase ducklake.native_writer_max_retry_count or retry the statement.")));
}

} // namespace pgducklake

extern "C" {

PG_FUNCTION_INFO_V1(ducklake_native_writer_stats);
Datum
ducklake_native_writer_stats(PG_FUNCTION_ARGS) {
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	if (!rsinfo || !IsA(rsinfo, ReturnSetInfo) || !(rsinfo->allowedModes & SFRM_Materialize)) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("native_writer_stats must be called in a materializing set context")));
	}

	TupleDesc tupdesc;
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("return type must be a row type")));
	}

	MemoryContext old_context = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	Tuplestorestate *tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = BlessTupleDesc(tupdesc);
	MemoryContextSwitchTo(old_context);

	uint64_t counters[pgducklake::NW_COUNTER_NUM];
	pgducklake::NativeWriterStatsReadAll(counters);
	bool nulls[2] = {false, false};
	for (int counter = 0; counter < pgducklake::NW_COUNTER_NUM; counter++) {
		Datum values[2] = {
		    CStringGetTextDatum(
		        pgducklake::NativeWriterCounterName(static_cast<pgducklake::NativeWriterCounter>(counter))),
		    Int64GetDatum(static_cast<int64_t>(counters[counter])),
		};
		tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);
	}
	return (Datum)0;
}

PG_FUNCTION_INFO_V1(ducklake_reset_native_writer_stats);
Datum
ducklake_reset_native_writer_stats(PG_FUNCTION_ARGS) {
	pgducklake::NativeWriterStatsReset();
	PG_RETURN_VOID();
}

} // extern "C"
