#pragma once

#include "pgddb/pg/declarations.hpp"

#include <cstdint>

namespace pgducklake {

/* How one column's source Datum reaches its inlined heap column. */
enum InlineConversionKind {
	/* Same PG type on both sides; the inlined attribute's typmod still
	 * applies. */
	INLINE_CONV_IDENTITY = 0,
	/* Text-shaped varlena into the BYTEA column DuckDB picks for VARCHAR and
	 * BLOB: identical in-memory layout, so no conversion at all. */
	INLINE_CONV_VARLENA,
	/* DuckDB reads the column back by parsing text. */
	INLINE_CONV_TEXT,
	/* Value-preserving widening into the larger integer column DuckDB picked
	 * for an unsigned type. */
	INLINE_CONV_WIDEN,
};

struct InlineConversion {
	InlineConversionKind kind;
	/* INLINE_CONV_WIDEN only: one-argument cast function. */
	Oid cast_func;
};

/* COPY has no fallback path, so it accepts a type the fast path declines
 * rather than fails on; nothing else differs between the two. */
enum InlineWriterKind {
	INLINE_WRITER_FAST_PATH = 0, /* VALUES and UNNEST */
	INLINE_WRITER_COPY,
};

/*
 * Which result the caller gets decides the error it raises: an unsupported type
 * is the user's to act on and INSERT still takes it, while a mismatch means the
 * facade and the inlined heap disagree about a type both support.
 */
enum InlineConversionResult {
	INLINE_CONV_OK = 0,
	/* Absent from the allowlist. */
	INLINE_CONV_UNSUPPORTED_TYPE,
	/* Allowlisted, but the attribute or source type does not match the entry. */
	INLINE_CONV_LAYOUT_MISMATCH,
	/* Storable, but only where a fallback exists -- never an error. */
	INLINE_CONV_FAST_PATH_DECLINED,
};

/* type_id and is_json are DuckLakeTypeIdentity's parse of
 * ducklake_column.column_type.  Callers that can decline must treat anything
 * but INLINE_CONV_OK as "not our statement" rather than as an error. */
InlineConversionResult DuckLakeTypeInlineConversion(InlineWriterKind writer, int type_id, bool is_json, Oid source_type,
                                                    Oid inline_type, int32_t inline_typmod,
                                                    InlineConversion *conversion_out);

/*
 * One column's resolved plan for moving a source Datum into its inlined heap
 * attribute.  Opaque so the resolved FmgrInfo stays out of the header, matching
 * MakeInlinedTypmodCoercion next door.
 *
 * Every writer compiles and applies through these, so a conversion kind cannot
 * be handled in one writer and silently fall through to a raw store in another.
 */
struct InlineColumnConv;

/* NULL when the allowlist declines the column, with result_out saying which
 * error to raise: a writer with a fallback treats any decline as "not our
 * statement", COPY has none and must raise. */
InlineColumnConv *MakeInlineColumnConv(InlineWriterKind writer, int type_id, bool is_json, Oid source_type,
                                       Form_pg_attribute inl_att, InlineConversionResult *result_out);

/* Two steps, not one: all three writers bind their statistics accumulator to
 * the source type, so they observe after the typmod coercion and before a
 * widened Datum stops reading as one. */
Datum ApplyInlineColumnTypmod(InlineColumnConv *conv, Datum value);
Datum ApplyInlineColumnConversion(InlineColumnConv *conv, Datum value);

} // namespace pgducklake
