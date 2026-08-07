#include "pgducklake/direct_insert/inline_col_stats.hpp"
#include "pgducklake/direct_insert/native_inline_writer.hpp"

#include <cmath>
#include <new>

#include <duckdb.hpp>

#include <common/ducklake_types.hpp>
#include <duckdb/common/exception.hpp>
#include <storage/ducklake_stats.hpp>

extern "C" {
#include "postgres.h"

#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
}

namespace pgducklake {

namespace {

struct InlineColStatsEntry {
	uint64_t column_id;
	char *column_type;
	bool bounds_safe;
	bool value_cmp;
	bool floating;
	bool ready;
	bool has_value;
	bool observed_null;
	bool observed_nan;
	Oid source_type;
	FmgrInfo output;
	FmgrInfo *compare;
	int16 typlen;
	bool typbyval;
	Datum min_value;
	Datum max_value;
	char *min_text;
	char *max_text;
};

struct TypeProperties {
	bool bounds_safe;
	bool value_cmp;
	bool floating;
	bool out_of_memory;
};

TypeProperties
GetTypeProperties(const char *type_name) {
	TypeProperties result = {};
	try {
		auto type = duckdb::DuckLakeTypes::FromString(type_name);
		result.value_cmp = duckdb::RequiresValueComparison(type);
		switch (type.id()) {
		case duckdb::LogicalTypeId::BOOLEAN:
		case duckdb::LogicalTypeId::TINYINT:
		case duckdb::LogicalTypeId::SMALLINT:
		case duckdb::LogicalTypeId::INTEGER:
		case duckdb::LogicalTypeId::BIGINT:
		case duckdb::LogicalTypeId::HUGEINT:
		case duckdb::LogicalTypeId::UTINYINT:
		case duckdb::LogicalTypeId::USMALLINT:
		case duckdb::LogicalTypeId::UINTEGER:
		case duckdb::LogicalTypeId::UBIGINT:
		case duckdb::LogicalTypeId::UHUGEINT:
		case duckdb::LogicalTypeId::DECIMAL:
		case duckdb::LogicalTypeId::DATE:
		case duckdb::LogicalTypeId::TIME:
		case duckdb::LogicalTypeId::TIMESTAMP:
		case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		case duckdb::LogicalTypeId::TIMESTAMP_SEC:
		case duckdb::LogicalTypeId::TIMESTAMP_MS:
		case duckdb::LogicalTypeId::TIMESTAMP_NS:
		case duckdb::LogicalTypeId::UUID:
		case duckdb::LogicalTypeId::VARCHAR:
			result.bounds_safe = true;
			break;
		case duckdb::LogicalTypeId::FLOAT:
		case duckdb::LogicalTypeId::DOUBLE:
			result.bounds_safe = true;
			result.floating = true;
			break;
		default:
			break;
		}
	} catch (const duckdb::OutOfMemoryException &) {
		result.out_of_memory = true;
	} catch (const std::bad_alloc &) {
		result.out_of_memory = true;
	} catch (...) {
	}
	return result;
}

bool
IsNaN(const InlineColStatsEntry &entry, Datum value) {
	if (!entry.floating) {
		return false;
	}
	if (entry.source_type == FLOAT4OID) {
		return std::isnan(DatumGetFloat4(value));
	}
	if (entry.source_type == FLOAT8OID) {
		return std::isnan(DatumGetFloat8(value));
	}
	return false;
}

void
FreeDatumIfNeeded(Datum value, bool typbyval) {
	if (!typbyval && DatumGetPointer(value)) {
		pfree(DatumGetPointer(value));
	}
}

} // namespace

struct InlineColStats {
	MemoryContext context;
	int source_count;
	int count;
	InlineColStatsEntry *entries;
};

InlineColStats *
CreateInlineColStats(uint64_t table_id, int num_cols) {
	MemoryContext context = AllocSetContextCreate(CurTransactionContext, "InlineColStats", ALLOCSET_SMALL_SIZES);
	MemoryContext old = MemoryContextSwitchTo(context);
	auto *stats = (InlineColStats *)palloc0(sizeof(InlineColStats));
	stats->context = context;
	stats->source_count = num_cols;
	MemoryContextSwitchTo(old);

	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query, R"(
		SELECT column_id, column_type, parent_column FROM ducklake.ducklake_column
		WHERE table_id = %llu AND end_snapshot IS NULL
		ORDER BY (parent_column IS NOT NULL), column_order, column_id)",
	                 (unsigned long long)table_id);
	int ret = SPI_execute(query.data, true, 0);
	if (ret != SPI_OK_SELECT) {
		return stats;
	}

	int top_level_count = 0;
	for (uint64_t i = 0; i < SPI_processed; i++) {
		bool isnull;
		SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull);
		if (isnull) {
			top_level_count++;
		}
	}
	if (top_level_count != num_cols) {
		return stats;
	}

	stats->count = (int)SPI_processed;
	old = MemoryContextSwitchTo(context);
	stats->entries = (InlineColStatsEntry *)palloc0(sizeof(InlineColStatsEntry) * stats->count);
	MemoryContextSwitchTo(old);

	for (int i = 0; i < stats->count; i++) {
		bool isnull;
		Datum id = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
		char *type_name = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
		if (isnull || !type_name) {
			stats->count = 0;
			return stats;
		}
		old = MemoryContextSwitchTo(context);
		stats->entries[i].column_id = (uint64_t)DatumGetInt64(id);
		stats->entries[i].column_type = pstrdup(type_name);
		MemoryContextSwitchTo(old);
		pfree(type_name);
	}

	/* DuckDB objects are confined to GetTypeProperties, which performs no PostgreSQL calls. */
	for (int i = 0; i < stats->count; i++) {
		auto properties = GetTypeProperties(stats->entries[i].column_type);
		if (properties.out_of_memory) {
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
		}
		stats->entries[i].bounds_safe = properties.bounds_safe;
		stats->entries[i].value_cmp = properties.value_cmp;
		stats->entries[i].floating = properties.floating;
	}
	return stats;
}

void
SetupInlineColStatsColumn(InlineColStats *stats, int column, Oid source_type) {
	if (!stats || column < 0 || column >= stats->source_count || column >= stats->count) {
		return;
	}
	auto &entry = stats->entries[column];
	entry.source_type = source_type;
	if (!entry.bounds_safe) {
		return;
	}

	Oid output;
	bool varlena;
	getTypeOutputInfo(source_type, &output, &varlena);
	MemoryContext old = MemoryContextSwitchTo(stats->context);
	fmgr_info(output, &entry.output);
	MemoryContextSwitchTo(old);

	if (entry.value_cmp) {
		TypeCacheEntry *type_cache = lookup_type_cache(source_type, TYPECACHE_CMP_PROC_FINFO);
		if (!type_cache || !OidIsValid(type_cache->cmp_proc_finfo.fn_oid)) {
			entry.bounds_safe = false;
			return;
		}
		entry.compare = &type_cache->cmp_proc_finfo;
		entry.typlen = type_cache->typlen;
		entry.typbyval = type_cache->typbyval;
	}
	entry.ready = true;
}

void
ObserveInlineColStatsDatum(InlineColStats *stats, int column, Datum value) {
	if (!stats || column < 0 || column >= stats->source_count || column >= stats->count) {
		return;
	}
	auto &entry = stats->entries[column];
	if (!entry.bounds_safe || !entry.ready) {
		return;
	}
	if (IsNaN(entry, value)) {
		entry.observed_nan = true;
		return;
	}

	if (entry.value_cmp) {
		if (!entry.has_value) {
			MemoryContext old = MemoryContextSwitchTo(stats->context);
			entry.min_value = datumCopy(value, entry.typbyval, entry.typlen);
			entry.max_value = datumCopy(value, entry.typbyval, entry.typlen);
			MemoryContextSwitchTo(old);
			entry.has_value = true;
			return;
		}
		if (DatumGetInt32(FunctionCall2Coll(entry.compare, InvalidOid, value, entry.min_value)) < 0) {
			MemoryContext old = MemoryContextSwitchTo(stats->context);
			Datum replacement = datumCopy(value, entry.typbyval, entry.typlen);
			MemoryContextSwitchTo(old);
			FreeDatumIfNeeded(entry.min_value, entry.typbyval);
			entry.min_value = replacement;
		}
		if (DatumGetInt32(FunctionCall2Coll(entry.compare, InvalidOid, value, entry.max_value)) > 0) {
			MemoryContext old = MemoryContextSwitchTo(stats->context);
			Datum replacement = datumCopy(value, entry.typbyval, entry.typlen);
			MemoryContextSwitchTo(old);
			FreeDatumIfNeeded(entry.max_value, entry.typbyval);
			entry.max_value = replacement;
		}
		return;
	}

	char *value_text = OutputFunctionCall(&entry.output, value);
	MemoryContext old = MemoryContextSwitchTo(stats->context);
	char *copy = pstrdup(value_text);
	MemoryContextSwitchTo(old);
	pfree(value_text);
	if (!entry.has_value) {
		old = MemoryContextSwitchTo(stats->context);
		entry.min_text = copy;
		entry.max_text = pstrdup(copy);
		MemoryContextSwitchTo(old);
		entry.has_value = true;
	} else if (strcmp(copy, entry.min_text) < 0) {
		pfree(entry.min_text);
		entry.min_text = copy;
	} else if (strcmp(copy, entry.max_text) > 0) {
		pfree(entry.max_text);
		entry.max_text = copy;
	} else {
		pfree(copy);
	}
}

void
ObserveInlineColStatsNull(InlineColStats *stats, int column) {
	if (stats && column >= 0 && column < stats->source_count && column < stats->count) {
		stats->entries[column].observed_null = true;
	}
}

NativeInlineColumnStat *
FinalizeInlineColStats(InlineColStats *stats, uint64_t *count) {
	*count = stats ? (uint64_t)stats->count : 0;
	if (!stats || stats->count == 0) {
		return nullptr;
	}
	MemoryContext old = MemoryContextSwitchTo(stats->context);
	auto *result = (NativeInlineColumnStat *)palloc0(sizeof(NativeInlineColumnStat) * stats->count);
	MemoryContextSwitchTo(old);

	int saved_style = DateStyle;
	int saved_order = DateOrder;
	PG_TRY();
	{
		DateStyle = USE_ISO_DATES;
		DateOrder = DATEORDER_YMD;
		for (int i = 0; i < stats->count; i++) {
			auto &entry = stats->entries[i];
			auto &out = result[i];
			out.column_id = entry.column_id;
			out.column_type = entry.column_type;
			out.invalidate_all = i >= stats->source_count;
			out.bounds_safe = entry.bounds_safe && entry.ready;
			out.has_min = entry.has_value;
			out.has_max = entry.has_value;
			out.observed_null = entry.observed_null;
			out.nan_known = entry.floating && entry.ready;
			out.observed_nan = entry.observed_nan;
			if (!entry.has_value) {
				continue;
			}
			if (entry.value_cmp) {
				char *min_value = OutputFunctionCall(&entry.output, entry.min_value);
				char *max_value = OutputFunctionCall(&entry.output, entry.max_value);
				old = MemoryContextSwitchTo(stats->context);
				out.min_value = pstrdup(min_value);
				out.max_value = pstrdup(max_value);
				MemoryContextSwitchTo(old);
				pfree(min_value);
				pfree(max_value);
			} else {
				out.min_value = entry.min_text;
				out.max_value = entry.max_text;
			}
		}
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

} // namespace pgducklake
