#include "pgducklake/direct_insert.hpp"
#include "pgducklake/inline_col_stats.hpp"

#include <cmath>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include <common/ducklake_types.hpp>
#include <duckdb/common/types/value.hpp>
#include <storage/ducklake_metadata_info.hpp>
#include <storage/ducklake_stats.hpp>

extern "C" {
#include "postgres.h"

#include "catalog/pg_type.h"
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

/* Whether DuckLake bounds are kept for a column, and how its values compare. */
enum class BoundsKind {
	NONE,     // bounds not maintained
	VALUE,    // PG ordering of the source type matches DuckDB's
	FLOATING, // as VALUE; NaN is tracked as contains_nan, not as a bound
	STRING,   // compared bytewise, as DuckDB compares strings
};

/* Makes no PG calls. */
BoundsKind
GetBoundsKind(const char *column_type) {
	try {
		switch (duckdb::DuckLakeTypes::FromString(column_type).id()) {
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
			return BoundsKind::VALUE;
		case duckdb::LogicalTypeId::FLOAT:
		case duckdb::LogicalTypeId::DOUBLE:
			return BoundsKind::FLOATING;
		case duckdb::LogicalTypeId::VARCHAR:
			return BoundsKind::STRING;
		default:
			return BoundsKind::NONE;
		}
	} catch (...) {
		return BoundsKind::NONE;
	}
}

struct ColumnEntry {
	uint64_t column_id;
	char *column_type;
	BoundsKind kind;
	Oid src_type;
	FmgrInfo *cmp;
	int16 typlen;
	bool typbyval;
	bool coerce;
	FmgrInfo coerce_finfo;
	int32 typmod;

	bool observed_null;
	bool observed_nan;
	bool has_value;
	Datum min;
	Datum max;
};

int
CompareValues(const ColumnEntry &col, Datum a, Datum b) {
	if (col.kind == BoundsKind::STRING) {
		auto *va = (struct varlena *)DatumGetPointer(a);
		auto *vb = (struct varlena *)DatumGetPointer(b);
		size_t la = VARSIZE_ANY_EXHDR(va);
		size_t lb = VARSIZE_ANY_EXHDR(vb);
		int r = memcmp(VARDATA_ANY(va), VARDATA_ANY(vb), la < lb ? la : lb);
		return r != 0 ? r : (la < lb ? -1 : (la > lb ? 1 : 0));
	}
	return DatumGetInt32(FunctionCall2Coll(col.cmp, InvalidOid, a, b));
}

bool
IsNaN(const ColumnEntry &col, Datum value) {
	return (col.src_type == FLOAT4OID && std::isnan(DatumGetFloat4(value))) ||
	       (col.src_type == FLOAT8OID && std::isnan(DatumGetFloat8(value)));
}

/* DuckDB-parseable text for a bound; DateStyle must be ISO. */
std::string
BoundText(const ColumnEntry &col, Datum value) {
	char buf[64];
	switch (col.src_type) {
	case FLOAT4OID:
		snprintf(buf, sizeof(buf), "%.9g", (double)DatumGetFloat4(value));
		return buf;
	case FLOAT8OID:
		snprintf(buf, sizeof(buf), "%.17g", DatumGetFloat8(value));
		return buf;
	default:
		break;
	}
	if (col.kind == BoundsKind::STRING) {
		auto *v = (struct varlena *)DatumGetPointer(value);
		return std::string(VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
	}
	Oid typoutput;
	bool typisvarlena;
	getTypeOutputInfo(col.src_type, &typoutput, &typisvarlena);
	char *str = OidOutputFunctionCall(typoutput, value);
	std::string result(str);
	pfree(str);
	return result;
}

struct Contribution {
	BoundsKind kind;
	const char *column_type;
	bool observed_null;
	bool observed_nan;
	bool has_value;
	std::string min;
	std::string max;
};

/* Merges with DuckLake's own rules; makes no PG calls. */
void
MergeColumnStats(const Contribution &in, bool initializing, duckdb::DuckLakeGlobalColumnStatsInfo &stats) {
	/* Unset bounds with a known contains_null mean "no values yet"; otherwise they are unknown. */
	bool seed_bounds = initializing || stats.has_contains_null;
	stats.has_contains_null = initializing || stats.has_contains_null;
	stats.contains_null = stats.contains_null || in.observed_null;
	if (in.kind == BoundsKind::FLOATING) {
		stats.has_contains_nan = initializing || stats.has_contains_nan;
		stats.contains_nan = stats.contains_nan || in.observed_nan;
	}
	if (in.kind == BoundsKind::NONE) {
		/* No safe bounds for this type: mark everything unknown. */
		stats.has_contains_null = stats.has_contains_nan = stats.has_min = stats.has_max = false;
		return;
	}
	if (!in.has_value) {
		return;
	}

	try {
		auto type = duckdb::DuckLakeTypes::FromString(in.column_type);
		auto current = duckdb::DuckLakeColumnStats::FromGlobalStats(type, stats);
		duckdb::DuckLakeColumnStats contribution(type);
		contribution.min = duckdb::Value(in.min).DefaultCastAs(type).ToString();
		contribution.max = duckdb::Value(in.max).DefaultCastAs(type).ToString();
		contribution.has_min = contribution.has_max = contribution.any_valid = true;
		current.MergeStats(contribution);

		stats.has_min = (seed_bounds || stats.has_min) && current.has_min;
		stats.has_max = (seed_bounds || stats.has_max) && current.has_max;
		stats.min_val = current.min;
		stats.max_val = current.max;
	} catch (...) {
		stats.has_min = stats.has_max = false;
	}
}

} // namespace

struct InlineColStats {
	MemoryContext context;
	int ncols;
	ColumnEntry *cols;
};

InlineColStats *
BeginInlineColStats(uint64_t table_id, const char *inlined_table, int ncols, const Oid *src_types) {
	MemoryContext context = AllocSetContextCreate(CurTransactionContext, "InlineColStats", ALLOCSET_SMALL_SIZES);
	auto *stats = (InlineColStats *)MemoryContextAllocZero(context, sizeof(InlineColStats));
	stats->context = context;
	stats->ncols = ncols;
	stats->cols = (ColumnEntry *)MemoryContextAllocZero(context, sizeof(ColumnEntry) * ncols);

	if (SPI_connect() != SPI_OK_CONNECT) {
		elog(ERROR, "inline column stats: SPI_connect failed");
	}
	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query, R"(
SELECT c.column_id, c.column_type, a.atttypmod
FROM ducklake.ducklake_column c
JOIN pg_catalog.pg_attribute a ON a.attrelid = '%s'::regclass AND a.attname = c.column_name
WHERE c.table_id = %llu AND c.end_snapshot IS NULL AND c.parent_column IS NULL
ORDER BY c.column_order)",
	                 inlined_table, (unsigned long long)table_id);
	if (SPI_execute(query.data, true, 0) != SPI_OK_SELECT || (int)SPI_processed != ncols) {
		elog(ERROR, "inline column stats: expected %d columns for table %llu", ncols, (unsigned long long)table_id);
	}

	for (int i = 0; i < ncols; i++) {
		HeapTuple tuple = SPI_tuptable->vals[i];
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		auto &col = stats->cols[i];
		bool isnull;
		col.column_id = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 1, &isnull));
		col.column_type = MemoryContextStrdup(context, SPI_getvalue(tuple, tupdesc, 2));
		int32 typmod = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 3, &isnull));
		col.src_type = src_types[i];
		col.kind = GetBoundsKind(col.column_type);

		if (col.kind == BoundsKind::STRING) {
			col.typlen = -1;
			col.typbyval = false;
			if (get_typlen(col.src_type) != -1) {
				col.kind = BoundsKind::NONE;
			}
		} else if (col.kind != BoundsKind::NONE) {
			TypeCacheEntry *tc = lookup_type_cache(col.src_type, TYPECACHE_CMP_PROC_FINFO);
			if (!OidIsValid(tc->cmp_proc_finfo.fn_oid)) {
				col.kind = BoundsKind::NONE;
			} else {
				col.cmp = &tc->cmp_proc_finfo;
				col.typlen = tc->typlen;
				col.typbyval = tc->typbyval;
				/* Bound the value as stored, e.g. numeric inlined as numeric(18,3). */
				Oid coerce_func = TypmodCoercionFunc(col.src_type, typmod);
				if (OidIsValid(coerce_func)) {
					fmgr_info_cxt(coerce_func, &col.coerce_finfo, context);
					col.typmod = typmod;
					col.coerce = true;
				}
			}
		}
	}
	SPI_finish();
	return stats;
}

void
ObserveInlineColStats(InlineColStats *stats, int col_idx, Datum value, bool isnull) {
	auto &col = stats->cols[col_idx];
	if (isnull) {
		col.observed_null = true;
		return;
	}
	if (col.kind == BoundsKind::NONE) {
		return;
	}
	if (col.kind == BoundsKind::FLOATING && IsNaN(col, value)) {
		col.observed_nan = true;
		return;
	}
	if (col.coerce) {
		value = FunctionCall3(&col.coerce_finfo, value, Int32GetDatum(col.typmod), BoolGetDatum(false));
	}
	if (col.kind == BoundsKind::STRING) {
		value = PointerGetDatum(pg_detoast_datum_packed((struct varlena *)DatumGetPointer(value)));
	}

	bool new_min = !col.has_value || CompareValues(col, value, col.min) < 0;
	bool new_max = !col.has_value || CompareValues(col, value, col.max) > 0;
	if (!new_min && !new_max) {
		return;
	}
	MemoryContext old = MemoryContextSwitchTo(stats->context);
	if (new_min) {
		if (col.has_value && !col.typbyval) {
			pfree(DatumGetPointer(col.min));
		}
		col.min = datumCopy(value, col.typbyval, col.typlen);
	}
	if (new_max) {
		if (col.has_value && !col.typbyval) {
			pfree(DatumGetPointer(col.max));
		}
		col.max = datumCopy(value, col.typbyval, col.typlen);
	}
	MemoryContextSwitchTo(old);
	col.has_value = true;
}

void
MergeInlineColStats(InlineColStats *stats, uint64_t table_id, bool initializing) {
	std::vector<Contribution> contributions(stats->ncols);
	int saved_date_style = DateStyle;
	int saved_date_order = DateOrder;
	PG_TRY();
	{
		DateStyle = USE_ISO_DATES;
		DateOrder = DATEORDER_YMD;
		for (int i = 0; i < stats->ncols; i++) {
			auto &col = stats->cols[i];
			auto &in = contributions[i];
			in.kind = col.kind;
			in.column_type = col.column_type;
			in.observed_null = col.observed_null;
			in.observed_nan = col.observed_nan;
			in.has_value = col.has_value;
			if (col.has_value) {
				in.min = BoundText(col, col.min);
				in.max = BoundText(col, col.max);
			}
		}
	}
	PG_FINALLY();
	{
		DateStyle = saved_date_style;
		DateOrder = saved_date_order;
	}
	PG_END_TRY();

	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query, R"(
SELECT column_id, contains_null, contains_nan, min_value, max_value
FROM ducklake.ducklake_table_column_stats WHERE table_id = %llu)",
	                 (unsigned long long)table_id);
	/* read_only = false: the stats rows may have been inserted by this same command. */
	if (SPI_execute(query.data, false, 0) != SPI_OK_SELECT) {
		elog(ERROR, "inline column stats: failed to read column stats");
	}
	std::vector<duckdb::DuckLakeGlobalColumnStatsInfo> persisted(stats->ncols);
	std::vector<bool> found(stats->ncols, false);
	for (uint64 r = 0; r < SPI_processed; r++) {
		HeapTuple tuple = SPI_tuptable->vals[r];
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		bool isnull;
		uint64_t column_id = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 1, &isnull));
		for (int i = 0; i < stats->ncols; i++) {
			if (stats->cols[i].column_id != column_id) {
				continue;
			}
			auto &p = persisted[i];
			found[i] = true;
			Datum d = SPI_getbinval(tuple, tupdesc, 2, &isnull);
			p.has_contains_null = !isnull;
			p.contains_null = !isnull && DatumGetBool(d);
			d = SPI_getbinval(tuple, tupdesc, 3, &isnull);
			p.has_contains_nan = !isnull;
			p.contains_nan = !isnull && DatumGetBool(d);
			char *min_value = SPI_getvalue(tuple, tupdesc, 4);
			char *max_value = SPI_getvalue(tuple, tupdesc, 5);
			p.has_min = min_value != nullptr;
			p.min_val = min_value ? min_value : "";
			p.has_max = max_value != nullptr;
			p.max_val = max_value ? max_value : "";
		}
	}

	for (int i = 0; i < stats->ncols; i++) {
		if (!found[i]) {
			continue;
		}
		auto &p = persisted[i];
		MergeColumnStats(contributions[i], initializing, p);

		Oid argtypes[6] = {INT8OID, INT8OID, BOOLOID, BOOLOID, TEXTOID, TEXTOID};
		Datum values[6] = {Int64GetDatum((int64)table_id),
		                   Int64GetDatum((int64)stats->cols[i].column_id),
		                   BoolGetDatum(p.contains_null),
		                   BoolGetDatum(p.contains_nan),
		                   p.has_min ? CStringGetTextDatum(p.min_val.c_str()) : (Datum)0,
		                   p.has_max ? CStringGetTextDatum(p.max_val.c_str()) : (Datum)0};
		char nulls[6] = {' ',
		                 ' ',
		                 p.has_contains_null ? ' ' : 'n',
		                 p.has_contains_nan ? ' ' : 'n',
		                 p.has_min ? ' ' : 'n',
		                 p.has_max ? ' ' : 'n'};
		int ret = SPI_execute_with_args(R"(
UPDATE ducklake.ducklake_table_column_stats
SET contains_null = $3, contains_nan = $4, min_value = $5, max_value = $6
WHERE table_id = $1 AND column_id = $2)",
		                                6, argtypes, values, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			elog(ERROR, "inline column stats: failed to update column stats: %d", ret);
		}
	}
	MemoryContextDelete(stats->context);
}

} // namespace pgducklake
