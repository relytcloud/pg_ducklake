#include "pgddb/worker/transport/arrow_codec.hpp"

#include <cstdio>
#include <cstring>

#include "duckdb.hpp" // duckdb_free, base types
#include <duckdb/common/arrow/arrow.hpp>
#include <duckdb/common/arrow/arrow_wrapper.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/table/arrow.hpp>
#include <duckdb/function/table/arrow/arrow_duck_schema.hpp>

#include "pgddb/pgddb_types.hpp" // PGDUCKDB_DUCK_DATE_OFFSET

extern "C" {
#include "postgres.h"

#include "executor/tuptable.h"
#include "utils/date.h"
}

#include "pgddb/pgddb_detoast.hpp" // postgres-dependent: after postgres.h

namespace pgddb {

namespace {

inline std::size_t
Align8(std::size_t n) {
	return (n + 7) & ~((std::size_t)7);
}

inline bool
IsVarlen(ArrowColCode code) {
	return code == ArrowColCode::Utf8 || code == ArrowColCode::Binary;
}

// Fixed-width element size for a code, or 0 for variable/unsupported.
uint32_t
ElemWidth(ArrowColCode code) {
	switch (code) {
	case ArrowColCode::Int16:
		return 2;
	case ArrowColCode::Int32:
	case ArrowColCode::Float:
	case ArrowColCode::Date32:
		return 4;
	case ArrowColCode::Int64:
	case ArrowColCode::Double:
	case ArrowColCode::Decimal64:
	case ArrowColCode::TimestampUs:
	case ArrowColCode::TimestampTzUs:
		return 8;
	case ArrowColCode::Decimal32:
		return 4;
	case ArrowColCode::Decimal128:
		return 16;
	case ArrowColCode::Utf8:
	case ArrowColCode::Binary:
		return 4; // offsets element width
	case ArrowColCode::Bool:
		return 0; // bit-packed like validity
	default:
		return 0;
	}
}

const char *
ArrowFormat(ArrowColCode code) {
	switch (code) {
	case ArrowColCode::Int16:
		return "s";
	case ArrowColCode::Int32:
		return "i";
	case ArrowColCode::Int64:
		return "l";
	case ArrowColCode::Float:
		return "f";
	case ArrowColCode::Double:
		return "g";
	case ArrowColCode::Date32:
		return "tdD";
	case ArrowColCode::Bool:
		return "b";
	case ArrowColCode::Utf8:
		return "u";
	case ArrowColCode::Binary:
		return "z";
	case ArrowColCode::TimestampUs:
		return "tsu:";
	case ArrowColCode::TimestampTzUs:
		return "tsu:UTC";
	default:
		return "";
	}
}

// Per-type value writers; `values` is the column's value buffer base in the page.
void
AppendInt16Val(char *values, int row, Datum v) {
	*(int16_t *)(values + (std::size_t)row * 2) = DatumGetInt16(v);
}
void
AppendInt32Val(char *values, int row, Datum v) {
	*(int32_t *)(values + (std::size_t)row * 4) = DatumGetInt32(v);
}
void
AppendInt64Val(char *values, int row, Datum v) {
	*(int64_t *)(values + (std::size_t)row * 8) = DatumGetInt64(v);
}
void
AppendFloatVal(char *values, int row, Datum v) {
	*(float *)(values + (std::size_t)row * 4) = DatumGetFloat4(v);
}
void
AppendDoubleVal(char *values, int row, Datum v) {
	*(double *)(values + (std::size_t)row * 8) = DatumGetFloat8(v);
}
void
AppendDate32Val(char *values, int row, Datum v) {
	*(int32_t *)(values + (std::size_t)row * 4) = (int32_t)(DatumGetDateADT(v) + PGDUCKDB_DUCK_DATE_OFFSET);
}
void
AppendBoolVal(char *values, int row, Datum v) {
	if (DatumGetBool(v))
		values[row / 8] |= (uint8_t)(1u << (row % 8));
}
// PG timestamps count microseconds from 2000-01-01; Arrow from 1970-01-01.
void
AppendTimestampVal(char *values, int row, Datum v) {
	*(int64_t *)(values + (std::size_t)row * 8) = DatumGetInt64(v) + PGDUCKDB_DUCK_TIMESTAMP_OFFSET;
}
void
AppendDecimal32Val(char *values, int row, Datum v) {
	pgddb::NumericToDecimalBytes(v, 4, values + (std::size_t)row * 4);
}
void
AppendDecimal64Val(char *values, int row, Datum v) {
	pgddb::NumericToDecimalBytes(v, 8, values + (std::size_t)row * 8);
}
void
AppendDecimal128Val(char *values, int row, Datum v) {
	pgddb::NumericToDecimalBytes(v, 16, values + (std::size_t)row * 16);
}
void
AppendNoopVal(char *, int, Datum) {
}

// Resolved once per column at builder setup; the row loop calls through it with
// no type switch. Varlen (Utf8/Binary) is staged separately, never dispatched here.
ArrowAppendValueFn
ResolveAppendValueFn(ArrowColCode code) {
	switch (code) {
	case ArrowColCode::Int16:
		return AppendInt16Val;
	case ArrowColCode::Int32:
		return AppendInt32Val;
	case ArrowColCode::Int64:
		return AppendInt64Val;
	case ArrowColCode::Float:
		return AppendFloatVal;
	case ArrowColCode::Double:
		return AppendDoubleVal;
	case ArrowColCode::Date32:
		return AppendDate32Val;
	case ArrowColCode::Bool:
		return AppendBoolVal;
	case ArrowColCode::TimestampUs:
	case ArrowColCode::TimestampTzUs:
		return AppendTimestampVal;
	case ArrowColCode::Decimal32:
		return AppendDecimal32Val;
	case ArrowColCode::Decimal64:
		return AppendDecimal64Val;
	case ArrowColCode::Decimal128:
		return AppendDecimal128Val;
	default:
		return AppendNoopVal; // Unsupported never reaches AppendSlot (ArrowColumnSpec rejects it)
	}
}

void
NoopReleaseSchema(ArrowSchema *s) {
	s->release = nullptr;
}
void
NoopReleaseArray(ArrowArray *a) {
	a->release = nullptr;
}

// Top-level array release: hands the page back to the pool exactly once.
struct PageReleaseHolder {
	std::function<void()> on_release;
};
void
PageReleaseArray(ArrowArray *a) {
	auto *h = reinterpret_cast<PageReleaseHolder *>(a->private_data);
	if (h) {
		if (h->on_release)
			h->on_release();
		delete h;
	}
	a->private_data = nullptr;
	a->release = nullptr;
}

} // namespace

bool
ArrowColumnSpec(Oid /*pg_type*/, const duckdb::LogicalType &duck_type, ArrowColCode &code, uint8_t &precision,
                uint8_t &scale) {
	precision = 0;
	scale = 0;
	switch (duck_type.id()) {
	case duckdb::LogicalTypeId::SMALLINT:
		code = ArrowColCode::Int16;
		return true;
	case duckdb::LogicalTypeId::INTEGER:
		code = ArrowColCode::Int32;
		return true;
	case duckdb::LogicalTypeId::BIGINT:
		code = ArrowColCode::Int64;
		return true;
	case duckdb::LogicalTypeId::FLOAT:
		code = ArrowColCode::Float;
		return true;
	case duckdb::LogicalTypeId::DOUBLE:
		code = ArrowColCode::Double;
		return true;
	case duckdb::LogicalTypeId::DATE:
		code = ArrowColCode::Date32;
		return true;
	case duckdb::LogicalTypeId::BOOLEAN:
		code = ArrowColCode::Bool;
		return true;
	case duckdb::LogicalTypeId::TIMESTAMP:
		code = ArrowColCode::TimestampUs;
		return true;
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		code = ArrowColCode::TimestampTzUs;
		return true;
	case duckdb::LogicalTypeId::VARCHAR:
		code = ArrowColCode::Utf8;
		return true;
	case duckdb::LogicalTypeId::BLOB:
		code = ArrowColCode::Binary;
		return true;
	case duckdb::LogicalTypeId::DECIMAL: {
		uint8_t w = duckdb::DecimalType::GetWidth(duck_type);
		precision = w;
		scale = duckdb::DecimalType::GetScale(duck_type);
		code = w <= 9 ? ArrowColCode::Decimal32 : (w <= 18 ? ArrowColCode::Decimal64 : ArrowColCode::Decimal128);
		return true;
	}
	default:
		code = ArrowColCode::Unsupported;
		return false;
	}
}

ArrowBatchBuilder::ArrowBatchBuilder(char *page, std::size_t capacity, const std::vector<ArrowColCode> &codes,
                                     const std::vector<uint8_t> &precisions, const std::vector<uint8_t> &scales,
                                     int max_rows)
    : page_(page), capacity_(capacity), codes_(codes), precisions_(precisions), scales_(scales), max_rows_(max_rows),
      validity_off_(), values_off_(), elem_width_(), append_fn_(), is_varlen_(), null_count_(), utf8_data_() {
	std::size_t ncols = codes_.size();
	validity_off_.resize(ncols);
	values_off_.resize(ncols);
	elem_width_.resize(ncols);
	append_fn_.resize(ncols);
	is_varlen_.resize(ncols);
	null_count_.assign(ncols, 0);
	utf8_data_.resize(ncols);

	std::size_t cursor = 0;
	std::size_t validity_bytes = (std::size_t)((max_rows + 7) / 8);
	for (std::size_t c = 0; c < ncols; c++) {
		elem_width_[c] = ElemWidth(codes_[c]);
		append_fn_[c] = ResolveAppendValueFn(codes_[c]);
		is_varlen_[c] = IsVarlen(codes_[c]);
		validity_off_[c] = (uint32_t)cursor;
		cursor = Align8(cursor + validity_bytes);
		values_off_[c] = (uint32_t)cursor;
		if (is_varlen_[c]) {
			cursor = Align8(cursor + (std::size_t)(max_rows + 1) * 4); // offsets[max_rows+1]
		} else if (codes_[c] == ArrowColCode::Bool) {
			cursor = Align8(cursor + validity_bytes); // bit-packed values
		} else {
			cursor = Align8(cursor + (std::size_t)max_rows * elem_width_[c]);
		}
	}
	fixed_end_ = cursor;
	if (fixed_end_ > capacity_) {
		layout_ok_ = false;
		return;
	}
	// Validity bitmaps default to all-valid (1); cleared per null below. Bool value
	// bitmaps start all-false and are set per true value.
	for (std::size_t c = 0; c < ncols; c++) {
		std::memset(page_ + validity_off_[c], 0xFF, validity_bytes);
		if (codes_[c] == ArrowColCode::Bool)
			std::memset(page_ + values_off_[c], 0, validity_bytes);
	}
}

bool
ArrowBatchBuilder::AppendSlot(TupleTableSlot *slot) {
	if (!layout_ok_ || nrows_ >= max_rows_)
		return false;
	int row = nrows_;
	Datum *values = slot->tts_values;
	bool *nulls = slot->tts_isnull;
	std::size_t ncols = codes_.size();

	// First pass: detoast utf8 values and check the page still fits, WITHOUT mutating
	// builder state, so an overflowing row can be deferred to the next batch.
	std::vector<const char *> sv_ptr(ncols, nullptr);
	std::vector<std::size_t> sv_len(ncols, 0);
	std::vector<Datum> sv_free(ncols, 0);
	std::size_t utf8_total = 0;
	for (std::size_t c = 0; c < ncols; c++) {
		if (!is_varlen_[c])
			continue;
		utf8_total += Align8(utf8_data_[c].size());
		if (nulls[c])
			continue;
		bool should_free = false;
		Datum v = pgddb::DetoastIfExternal(values[c], &should_free);
		void *vp = DatumGetPointer(v); // PG19: Datum no longer converts to a pointer implicitly
		sv_ptr[c] = VARDATA_ANY(vp);
		sv_len[c] = (std::size_t)VARSIZE_ANY_EXHDR(vp);
		sv_free[c] = should_free ? v : 0;
	}
	std::size_t add = 0;
	for (std::size_t c = 0; c < ncols; c++)
		add += sv_len[c];
	if (fixed_end_ + utf8_total + add > capacity_) {
		for (std::size_t c = 0; c < ncols; c++)
			if (sv_free[c])
				duckdb_free(reinterpret_cast<void *>(sv_free[c]));
		return false; // defer this row to a fresh page
	}

	// Second pass: commit, dispatching through the per-column writer resolved at setup.
	for (std::size_t c = 0; c < ncols; c++) {
		if (is_varlen_[c]) {
			((uint32_t *)(page_ + values_off_[c]))[row] = (uint32_t)utf8_data_[c].size();
		}
		if (nulls[c]) {
			(page_ + validity_off_[c])[row / 8] &= (uint8_t)~(1u << (row % 8));
			null_count_[c]++;
			continue;
		}
		if (is_varlen_[c])
			utf8_data_[c].append(sv_ptr[c], sv_len[c]);
		else
			append_fn_[c](page_ + values_off_[c], row, values[c]);
	}
	for (std::size_t c = 0; c < ncols; c++)
		if (sv_free[c])
			duckdb_free(reinterpret_cast<void *>(sv_free[c]));
	nrows_++;
	return true;
}

std::size_t
ArrowBatchBuilder::Finalize(int page_index, char *out, std::size_t out_cap) {
	std::size_t ncols = codes_.size();
	// Place utf8 data contiguously per column after the fixed region; finalize offsets.
	std::vector<uint32_t> utf8_buf2_off(ncols, 0), utf8_buf2_len(ncols, 0);
	std::size_t cursor = fixed_end_;
	for (std::size_t c = 0; c < ncols; c++) {
		if (!IsVarlen(codes_[c]))
			continue;
		uint32_t *offs = (uint32_t *)(page_ + values_off_[c]);
		offs[nrows_] = (uint32_t)utf8_data_[c].size(); // final end offset
		std::memcpy(page_ + cursor, utf8_data_[c].data(), utf8_data_[c].size());
		utf8_buf2_off[c] = (uint32_t)cursor;
		utf8_buf2_len[c] = (uint32_t)utf8_data_[c].size();
		cursor = Align8(cursor + utf8_data_[c].size());
	}

	std::size_t need = sizeof(ArrowBatchHeader) + ncols * sizeof(ArrowColDesc);
	if (need > out_cap)
		return 0;
	auto *hdr = (ArrowBatchHeader *)out;
	hdr->page_index = (uint32_t)page_index;
	hdr->nrows = (uint32_t)nrows_;
	hdr->ncols = (uint32_t)ncols;
	auto *cd = (ArrowColDesc *)(out + sizeof(ArrowBatchHeader));
	uint32_t validity_len = (uint32_t)((nrows_ + 7) / 8);
	for (std::size_t c = 0; c < ncols; c++) {
		cd[c].code = (uint8_t)codes_[c];
		cd[c].precision = precisions_[c];
		cd[c].scale = scales_[c];
		cd[c].pad = 0;
		cd[c].null_count = null_count_[c];
		cd[c].validity_off = validity_off_[c];
		cd[c].validity_len = validity_len;
		cd[c].buf1_off = values_off_[c];
		if (IsVarlen(codes_[c])) {
			cd[c].buf1_len = (uint32_t)((nrows_ + 1) * 4);
			cd[c].buf2_off = utf8_buf2_off[c];
			cd[c].buf2_len = utf8_buf2_len[c];
		} else if (codes_[c] == ArrowColCode::Bool) {
			cd[c].buf1_len = validity_len; // bit-packed values
			cd[c].buf2_off = 0;
			cd[c].buf2_len = 0;
		} else {
			cd[c].buf1_len = (uint32_t)((std::size_t)nrows_ * elem_width_[c]);
			cd[c].buf2_off = 0;
			cd[c].buf2_len = 0;
		}
	}
	return need;
}

void
ImportArrowBatch(duckdb::ClientContext &context, char *page, const char *descriptor, std::size_t /*desc_len*/,
                 const duckdb::vector<duckdb::LogicalType> &output_types, duckdb::DataChunk &out,
                 std::function<void()> on_release) {
	auto *hdr = (const ArrowBatchHeader *)descriptor;
	auto *cd = (const ArrowColDesc *)(descriptor + sizeof(ArrowBatchHeader));
	duckdb::idx_t nrows = hdr->nrows;
	duckdb::idx_t ncols = hdr->ncols;

	out.Initialize(duckdb::Allocator::DefaultAllocator(), output_types);

	// One wrapper owns the page lifetime: its release frees the page when the last
	// importing vector drops it.
	auto wrapper = duckdb::make_shared_ptr<duckdb::ArrowArrayWrapper>();
	wrapper->arrow_array.release = PageReleaseArray;
	wrapper->arrow_array.private_data = new PageReleaseHolder {std::move(on_release)};

	for (idx_t c = 0; c < ncols; c++) {
		auto code = (ArrowColCode)cd[c].code;
		const void *buffers[3];
		ArrowArray col {};
		col.length = (int64_t)nrows;
		col.null_count = cd[c].null_count;
		col.offset = 0;
		col.n_children = 0;
		col.children = nullptr;
		col.dictionary = nullptr;
		col.release = NoopReleaseArray;
		col.private_data = nullptr;
		buffers[0] = cd[c].null_count > 0 ? (const void *)(page + cd[c].validity_off) : nullptr;
		buffers[1] = (const void *)(page + cd[c].buf1_off);
		if (code == ArrowColCode::Utf8 || code == ArrowColCode::Binary) {
			buffers[2] = (const void *)(page + cd[c].buf2_off);
			col.n_buffers = 3;
		} else {
			col.n_buffers = 2;
		}
		col.buffers = buffers;

		char decfmt[32];
		const char *fmt;
		if (code == ArrowColCode::Decimal32 || code == ArrowColCode::Decimal64 || code == ArrowColCode::Decimal128) {
			int bits = code == ArrowColCode::Decimal32 ? 32 : (code == ArrowColCode::Decimal64 ? 64 : 128);
			snprintf(decfmt, sizeof(decfmt), "d:%u,%u,%d", cd[c].precision, cd[c].scale, bits);
			fmt = decfmt;
		} else {
			fmt = ArrowFormat(code);
		}
		ArrowSchema schema {};
		schema.format = fmt;
		schema.name = "";
		schema.metadata = nullptr;
		schema.flags = 2; // ARROW_FLAG_NULLABLE
		schema.n_children = 0;
		schema.children = nullptr;
		schema.dictionary = nullptr;
		schema.release = NoopReleaseSchema;
		schema.private_data = nullptr;

		auto arrow_type = duckdb::ArrowType::GetArrowLogicalType(context, schema);
		duckdb::ArrowArrayScanState array_state(context);
		array_state.owned_data = wrapper;
		duckdb::ArrowToDuckDBConversion::SetValidityMask(out.data[c], col, 0, nrows, 0, -1);
		duckdb::ArrowToDuckDBConversion::ColumnArrowToDuckDB(out.data[c], col, 0, array_state, nrows, *arrow_type);
	}
	out.SetCardinality(nrows);
}

} // namespace pgddb
