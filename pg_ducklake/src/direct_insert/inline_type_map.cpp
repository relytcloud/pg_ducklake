/* Planner-time capability checks and execution-time conversion both resolve
 * through this table, so the accepted set and the implemented set cannot drift
 * apart. */

#include "pgducklake/direct_insert/inline_type_map.hpp"

#include "pgducklake/direct_insert/native_inline_writer.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"

/* Only the LogicalTypeId enumeration crosses into the PostgreSQL frames below:
 * no object, no allocation, nothing that can throw. */
#include <duckdb/common/types.hpp>

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

namespace pgducklake {

namespace {

using duckdb::LogicalTypeId;

constexpr int MAX_INLINE_TYPES = 2;
constexpr int MAX_SOURCE_TYPES = 3;

struct InlineTypeEntry {
	/* Keyed on the type DuckLake parses column_type into rather than on the
	 * spelling, so several spellings of one type cannot disagree here. */
	LogicalTypeId type_id;
	/* json and varchar both parse to VARCHAR, and they inline from different
	 * source types. */
	bool is_json;
	/* What PostgresMetadataManager::GetColumnType creates for this type. */
	Oid inline_types[MAX_INLINE_TYPES];
	/* The user-facing type the value arrives as: what the user declared, or
	 * what DuckLakeTypeToPgType chose when reverse-syncing a table an external
	 * DuckDB client created. */
	Oid source_types[MAX_SOURCE_TYPES];
	InlineConversionKind kind;
	/* Storable, but VALUES and UNNEST decline it -- see timestamptz below. */
	bool fast_path_declines;
};

/*
 * The list fails closed: a DuckLake type that is absent, or present but whose
 * inlined attribute does not match the entry, is declined.  A wrong entry is
 * not a failed statement -- it stores bytes the reader silently rejects or
 * misreads, permanently.
 *
 * Each entry names all three type systems because they drift apart per type: a
 * synced list is a VARCHAR in the heap and text to the user, so neither side
 * alone identifies the column.
 *
 * Why types are missing rather than listed:
 *  - struct, map and list inline as VARCHAR holding DuckDB's text format,
 *    which no PostgreSQL output function produces: array_out writes {1,2}
 *    where DuckDB expects [1, 2].
 *  - variant and geometry have no inlined representation at all.
 *  - uint64, int128 and uint128 are wider than every PostgreSQL integer, so the
 *    facade carries them as unconstrained numeric over a VARCHAR heap column.
 *    numeric admits values those types cannot hold, and a fractional or
 *    out-of-range one serializes to text the reader rejects.  Nothing can
 *    create such a column until reverse sync fires, so there is no test to
 *    hold an entry honest; declining costs a fallback that writes them
 *    correctly.
 *  - time_ns has no PostgreSQL type to inline into.
 *  - jsonb shares json's mapping, but its varlena holds jsonb's binary
 *    encoding instead of text, so it cannot take the varlena path.
 *  - "char", regclass and other types that map onto a DuckLake type by a
 *    different route than the entry's source type: the Datum is not the shape
 *    the entry describes.
 *
 * INLINE_CONV_TEXT routes a type through a PostgreSQL output function, which
 * follows session GUCs.  Only DateStyle is pinned (OutputFunctionCallIso); it
 * covers timestamptz because timestamptz_out emits an explicit offset.
 */
constexpr InlineTypeEntry INLINE_TYPE_ALLOWLIST[] = {
    {LogicalTypeId::BOOLEAN, false, {BOOLOID}, {BOOLOID}, INLINE_CONV_IDENTITY, false},
    {LogicalTypeId::TINYINT, false, {INT2OID}, {INT2OID}, INLINE_CONV_IDENTITY, false},
    {LogicalTypeId::SMALLINT, false, {INT2OID}, {INT2OID}, INLINE_CONV_IDENTITY, false},
    {LogicalTypeId::INTEGER, false, {INT4OID}, {INT4OID}, INLINE_CONV_IDENTITY, false},
    {LogicalTypeId::BIGINT, false, {INT8OID}, {INT8OID}, INLINE_CONV_IDENTITY, false},
    /* Unsigned types have no PostgreSQL counterpart, so DuckDB inlines them
     * into the next integer up while the facade keeps the smaller signed type
     * that holds the same range. */
    {LogicalTypeId::UTINYINT, false, {INT4OID}, {INT2OID}, INLINE_CONV_WIDEN, false},
    {LogicalTypeId::USMALLINT, false, {INT4OID}, {INT4OID}, INLINE_CONV_IDENTITY, false},
    {LogicalTypeId::UINTEGER, false, {INT8OID}, {INT8OID}, INLINE_CONV_IDENTITY, false},
    {LogicalTypeId::FLOAT, false, {FLOAT4OID}, {FLOAT4OID}, INLINE_CONV_IDENTITY, false},
    {LogicalTypeId::DOUBLE, false, {FLOAT8OID}, {FLOAT8OID}, INLINE_CONV_IDENTITY, false},
    /* Width and scale ride along in the spelling -- decimal(18,3) -- and
     * DuckLake parses them into this id. */
    {LogicalTypeId::DECIMAL, false, {NUMERICOID}, {NUMERICOID}, INLINE_CONV_IDENTITY, false},
    {LogicalTypeId::TIME, false, {TIMEOID}, {TIMEOID}, INLINE_CONV_IDENTITY, false},
    {LogicalTypeId::TIME_TZ, false, {TIMETZOID}, {TIMETZOID}, INLINE_CONV_IDENTITY, false},
    {LogicalTypeId::INTERVAL, false, {INTERVALOID}, {INTERVALOID}, INLINE_CONV_IDENTITY, false},
    /* PostgreSQL's date and timestamp ranges are narrower than DuckDB's, so
     * DuckDB stores them as text rather than risk a value it cannot round
     * trip. */
    {LogicalTypeId::DATE, false, {VARCHAROID, TEXTOID}, {DATEOID}, INLINE_CONV_TEXT, false},
    /* Covers both timestamp and timestamp_us: DuckLake spells microsecond
     * precision two ways and parses both to TIMESTAMP. */
    {LogicalTypeId::TIMESTAMP, false, {VARCHAROID, TEXTOID}, {TIMESTAMPOID}, INLINE_CONV_TEXT, false},
    {LogicalTypeId::TIMESTAMP_MS, false, {VARCHAROID, TEXTOID}, {TIMESTAMPOID}, INLINE_CONV_TEXT, false},
    {LogicalTypeId::TIMESTAMP_NS, false, {VARCHAROID, TEXTOID}, {TIMESTAMPOID}, INLINE_CONV_TEXT, false},
    {LogicalTypeId::TIMESTAMP_SEC, false, {VARCHAROID, TEXTOID}, {TIMESTAMPOID}, INLINE_CONV_TEXT, false},
    /* Stores correctly, but flush_inlined_data derives delete positions from
     * an uncast ORDER BY over the heap text, and local time plus an offset
     * does not sort chronologically.  Only the fast path declines: COPY has no
     * fallback, so it keeps writing these as it did before the allowlist. */
    {LogicalTypeId::TIMESTAMP_TZ, false, {VARCHAROID, TEXTOID}, {TIMESTAMPTZOID}, INLINE_CONV_TEXT, true},
    /* PostgreSQL cannot store null bytes in text, so DuckDB inlines strings as
     * BYTEA -- same varlena layout, no conversion.  bpchar joins text and
     * varchar because pgddb_types maps all three to VARCHAR; leaving it out
     * would refuse a char(n) column that COPY stored before this allowlist
     * existed. */
    {LogicalTypeId::VARCHAR, false, {BYTEAOID}, {TEXTOID, VARCHAROID, BPCHAROID}, INLINE_CONV_VARLENA, false},
    /* text as well as json: DuckLakeTypeToPgType has no IsJSONType branch, so a
     * reverse-synced json column reaches the facade as plain text. */
    {LogicalTypeId::VARCHAR, true, {BYTEAOID}, {JSONOID, TEXTOID}, INLINE_CONV_VARLENA, false},
    {LogicalTypeId::BLOB, false, {BYTEAOID}, {BYTEAOID}, INLINE_CONV_IDENTITY, false},
    {LogicalTypeId::UUID, false, {UUIDOID}, {UUIDOID}, INLINE_CONV_IDENTITY, false},
};

const InlineTypeEntry *
FindInlineTypeEntry(int type_id, bool is_json) {
	for (const auto &entry : INLINE_TYPE_ALLOWLIST) {
		if ((int)entry.type_id == type_id && entry.is_json == is_json) {
			return &entry;
		}
	}
	return nullptr;
}

bool
OidIsListed(const Oid *oids, int len, Oid needle) {
	for (int i = 0; i < len && OidIsValid(oids[i]); i++) {
		if (oids[i] == needle) {
			return true;
		}
	}
	return false;
}

/*
 * Resolved through PostgreSQL's own coercion lookup rather than a hardcoded
 * cast oid, so the stored value matches what a plain INSERT would write.
 * COERCION_IMPLICIT is the narrowest context PostgreSQL offers and excludes
 * the lossy assignment casts (numeric -> int4 rounds); the allowlist has
 * already restricted the pair to a widening one.
 */
bool
FindWideningCast(Oid source_type, Oid inline_type, Oid *funcid_out) {
	Oid funcid = InvalidOid;
	if (find_coercion_pathway(inline_type, source_type, COERCION_IMPLICIT, &funcid) != COERCION_PATH_FUNC ||
	    !OidIsValid(funcid)) {
		return false;
	}

	/* A cast taking typmod or an explicit-cast flag is doing more than
	 * widening; the writers call this one-argument form directly. */
	if (get_func_nargs(funcid) != 1) {
		return false;
	}

	*funcid_out = funcid;
	return true;
}

} // namespace

InlineConversionResult
DuckLakeTypeInlineConversion(InlineWriterKind writer, int type_id, bool is_json, Oid source_type, Oid inline_type,
                             int32_t inline_typmod, InlineConversion *conversion_out) {
	const InlineTypeEntry *entry = FindInlineTypeEntry(type_id, is_json);
	if (entry == nullptr) {
		return INLINE_CONV_UNSUPPORTED_TYPE;
	}
	if (writer == INLINE_WRITER_FAST_PATH && entry->fast_path_declines) {
		return INLINE_CONV_FAST_PATH_DECLINED;
	}
	if (!OidIsListed(entry->inline_types, MAX_INLINE_TYPES, inline_type) ||
	    !OidIsListed(entry->source_types, MAX_SOURCE_TYPES, source_type)) {
		return INLINE_CONV_LAYOUT_MISMATCH;
	}
	/* Only an identity conversion leaves a Datum the inlined attribute's own
	 * typmod function can be applied to. */
	if (entry->kind != INLINE_CONV_IDENTITY && inline_typmod >= 0) {
		return INLINE_CONV_LAYOUT_MISMATCH;
	}

	InlineConversion conversion = {};
	conversion.kind = entry->kind;
	if (entry->kind == INLINE_CONV_WIDEN && !FindWideningCast(source_type, inline_type, &conversion.cast_func)) {
		return INLINE_CONV_LAYOUT_MISMATCH;
	}

	*conversion_out = conversion;
	return INLINE_CONV_OK;
}

struct InlineColumnConv {
	InlineConversion conversion;
	/* The output function for INLINE_CONV_TEXT, the cast for INLINE_CONV_WIDEN;
	 * the kinds are mutually exclusive, so one slot serves both. */
	FmgrInfo finfo;
	InlinedTypmodCoercion *typmod_coercion;
};

InlineColumnConv *
MakeInlineColumnConv(InlineWriterKind writer, int type_id, bool is_json, Oid source_type, Form_pg_attribute inl_att,
                     InlineConversionResult *result_out) {
	InlineConversion conversion;
	*result_out = DuckLakeTypeInlineConversion(writer, type_id, is_json, source_type, inl_att->atttypid,
	                                           inl_att->atttypmod, &conversion);
	if (*result_out != INLINE_CONV_OK) {
		return NULL;
	}

	InlineColumnConv *conv = (InlineColumnConv *)palloc0(sizeof(InlineColumnConv));
	conv->conversion = conversion;

	switch (conversion.kind) {
	case INLINE_CONV_TEXT: {
		Oid output_oid;
		bool typisvarlena;
		getTypeOutputInfo(source_type, &output_oid, &typisvarlena);
		fmgr_info(output_oid, &conv->finfo);
		break;
	}
	case INLINE_CONV_WIDEN:
		fmgr_info(conversion.cast_func, &conv->finfo);
		break;
	default:
		break;
	}

	/* Until the native writer landed, these paths inserted through SPI, which
	 * applied the target typmod on the caller's behalf. */
	conv->typmod_coercion = MakeInlinedTypmodCoercion(inl_att->atttypid, inl_att->atttypmod);
	return conv;
}

Datum
ApplyInlineColumnTypmod(InlineColumnConv *conv, Datum value) {
	if (conv->typmod_coercion == NULL) {
		return value;
	}
	return ApplyInlinedTypmodCoercion(conv->typmod_coercion, value);
}

/* Allocates in the current memory context; callers own that context for as long
 * as the slot holding the result is unread. */
Datum
ApplyInlineColumnConversion(InlineColumnConv *conv, Datum value) {
	switch (conv->conversion.kind) {
	case INLINE_CONV_TEXT: {
		char *str = OutputFunctionCallIso(&conv->finfo, value);
		Datum converted = CStringGetTextDatum(str);
		pfree(str);
		return converted;
	}
	case INLINE_CONV_WIDEN:
		return FunctionCall1(&conv->finfo, value);
	default:
		return value;
	}
}

} // namespace pgducklake
