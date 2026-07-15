/*
 * PG-side murmur3_32 for DuckLake's Iceberg-compatible bucket partition
 * transform. flush_inlined_data()'s bookkeeping SQL embeds
 * (murmur3_32(col) & 2147483647) % N and runs over SPI, so PostgreSQL needs
 * the hash with byte-for-byte DuckLake semantics. The core hash is reused
 * from the vendored header; each overload mirrors the input encoding of
 * DuckLake's Murmur3ScalarFunction (ducklake_murmur3.cpp):
 *   - bool/int16/int32/int64: sign-extended to int64, hashed as 8 bytes
 *   - float/double: widened to double, -0.0 normalized, IEEE754 bits as int64
 *   - text: raw UTF-8 bytes
 *   - date: days since 1970-01-01 as int64 (PG stores days since 2000-01-01)
 *   - time: microseconds since midnight (same encoding in PG and DuckDB)
 *   - timestamp/timestamptz: microseconds since 1970-01-01 UTC (PG epoch 2000)
 */

#include <cstring>

#include <common/ducklake_murmur3.hpp>

extern "C" {
#include "postgres.h"

#if PG_VERSION_NUM >= 160000
#include "varatt.h"
#endif

#include "fmgr.h"
#include "utils/date.h"
#include "utils/timestamp.h"
}

static constexpr int64_t PG_TO_UNIX_EPOCH_DAYS = POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
static constexpr int64_t PG_TO_UNIX_EPOCH_USECS = PG_TO_UNIX_EPOCH_DAYS * USECS_PER_DAY;

static int32_t
MurmurInt64(int64_t val) {
	return duckdb::DuckLakeMurmur3::HashValue<int64_t>(val);
}

static int32_t
MurmurDouble(double val) {
	if (val == 0.0) {
		val = 0.0; // normalize -0.0, as DuckLake does
	}
	int64_t bits;
	memcpy(&bits, &val, sizeof(bits));
	return MurmurInt64(bits);
}

extern "C" {

PG_FUNCTION_INFO_V1(ducklake_murmur3_bool);
Datum
ducklake_murmur3_bool(PG_FUNCTION_ARGS) {
	PG_RETURN_INT32(MurmurInt64(PG_GETARG_BOOL(0) ? 1 : 0));
}

PG_FUNCTION_INFO_V1(ducklake_murmur3_int2);
Datum
ducklake_murmur3_int2(PG_FUNCTION_ARGS) {
	PG_RETURN_INT32(MurmurInt64(PG_GETARG_INT16(0)));
}

PG_FUNCTION_INFO_V1(ducklake_murmur3_int4);
Datum
ducklake_murmur3_int4(PG_FUNCTION_ARGS) {
	PG_RETURN_INT32(MurmurInt64(PG_GETARG_INT32(0)));
}

PG_FUNCTION_INFO_V1(ducklake_murmur3_int8);
Datum
ducklake_murmur3_int8(PG_FUNCTION_ARGS) {
	PG_RETURN_INT32(MurmurInt64(PG_GETARG_INT64(0)));
}

PG_FUNCTION_INFO_V1(ducklake_murmur3_float4);
Datum
ducklake_murmur3_float4(PG_FUNCTION_ARGS) {
	PG_RETURN_INT32(MurmurDouble((double)PG_GETARG_FLOAT4(0)));
}

PG_FUNCTION_INFO_V1(ducklake_murmur3_float8);
Datum
ducklake_murmur3_float8(PG_FUNCTION_ARGS) {
	PG_RETURN_INT32(MurmurDouble(PG_GETARG_FLOAT8(0)));
}

/* Backs both the text and bytea overloads: DuckLake hashes a VARCHAR's UTF-8
 * bytes and a BLOB's raw bytes, the same raw-varlena-bytes rule. */
PG_FUNCTION_INFO_V1(ducklake_murmur3_text);
Datum
ducklake_murmur3_text(PG_FUNCTION_ARGS) {
	text *val = PG_GETARG_TEXT_PP(0);
	auto data = reinterpret_cast<const uint8_t *>(VARDATA_ANY(val));
	PG_RETURN_INT32(duckdb::DuckLakeMurmur3::Hash(data, VARSIZE_ANY_EXHDR(val)));
}

PG_FUNCTION_INFO_V1(ducklake_murmur3_date);
Datum
ducklake_murmur3_date(PG_FUNCTION_ARGS) {
	PG_RETURN_INT32(MurmurInt64((int64_t)PG_GETARG_DATEADT(0) + PG_TO_UNIX_EPOCH_DAYS));
}

PG_FUNCTION_INFO_V1(ducklake_murmur3_time);
Datum
ducklake_murmur3_time(PG_FUNCTION_ARGS) {
	PG_RETURN_INT32(MurmurInt64(PG_GETARG_TIMEADT(0)));
}

/* Timestamp and TimestampTz share the representation (microseconds since
 * 2000-01-01 UTC), so this one entry point backs both SQL overloads. */
PG_FUNCTION_INFO_V1(ducklake_murmur3_timestamp);
Datum
ducklake_murmur3_timestamp(PG_FUNCTION_ARGS) {
	PG_RETURN_INT32(MurmurInt64(PG_GETARG_TIMESTAMP(0) + PG_TO_UNIX_EPOCH_USECS));
}

} // extern "C"
