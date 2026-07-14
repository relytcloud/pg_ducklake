#include "duckdb.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/extra_type_info.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/uuid.hpp"

#include "pgddb/pgddb_types.hpp"
#include "pgddb/pgddb_utils.hpp"
#include "pgddb/scan/postgres_scan.hpp"
#include "pgddb/pg/memory.hpp"
#include "pgddb/pg/types.hpp"

extern "C" {

#include "pgddb/vendor/pg_numeric_c.hpp"

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/tupdesc_details.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "executor/tuptable.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"
#include "utils/varbit.h"
}

#include "pgddb/pgddb_detoast.hpp"
#include "pgddb/pgddb_process_lock.hpp"
#include "pgddb/pgddb_types_array.hpp"

namespace pgddb {

// Type-extension hooks: each conversion handles built-in PG types first, then tries these in registration order.
static std::vector<ConvertPostgresToBaseDuckColumnType_hook_t> g_pg_to_base_duck_hooks;
static std::vector<GetPostgresDuckDBType_hook_t> g_duck_to_pg_hooks;
static std::vector<GetPostgresArrayDuckDBType_hook_t> g_duck_to_pg_array_hooks;
static std::vector<ConvertDuckToPostgresValue_hook_t> g_duck_value_to_pg_hooks;
static std::vector<GetPostgresToDuckValueFn_hook_t> g_pg_to_duck_value_fn_hooks;

bool convert_unsupported_numeric_to_double = false; // see pgddb_types.hpp

extern "C" __attribute__((visibility("default"))) void
Register_ConvertPostgresToBaseDuckColumnType(ConvertPostgresToBaseDuckColumnType_hook_t fn) {
	g_pg_to_base_duck_hooks.push_back(fn);
}
extern "C" __attribute__((visibility("default"))) void
Register_GetPostgresDuckDBType(GetPostgresDuckDBType_hook_t fn) {
	g_duck_to_pg_hooks.push_back(fn);
}
extern "C" __attribute__((visibility("default"))) void
Register_GetPostgresArrayDuckDBType(GetPostgresArrayDuckDBType_hook_t fn) {
	g_duck_to_pg_array_hooks.push_back(fn);
}
extern "C" __attribute__((visibility("default"))) void
Register_ConvertDuckToPostgresValue(ConvertDuckToPostgresValue_hook_t fn) {
	g_duck_value_to_pg_hooks.push_back(fn);
}
extern "C" __attribute__((visibility("default"))) void
Register_GetPostgresToDuckValueFn(GetPostgresToDuckValueFn_hook_t fn) {
	g_pg_to_duck_value_fn_hooks.push_back(fn);
}

NumericVar FromNumeric(Numeric num);

struct NumericAsDouble : public duckdb::ExtraTypeInfo {
	// Marker that the DOUBLE originated from a PG Numeric.
public:
	NumericAsDouble() : ExtraTypeInfo(duckdb::ExtraTypeInfoType::INVALID_TYPE_INFO) {
	}

	duckdb::shared_ptr<ExtraTypeInfo>
	Copy() const override {
		return duckdb::make_shared_ptr<NumericAsDouble>(*this);
	}
};

// FIXME: perhaps we want to just make a generic ExtraTypeInfo that holds the Postgres type OID
struct IsBpChar : public duckdb::ExtraTypeInfo {
public:
	IsBpChar() : ExtraTypeInfo(duckdb::ExtraTypeInfoType::INVALID_TYPE_INFO) {
	}

	duckdb::shared_ptr<ExtraTypeInfo>
	Copy() const override {
		return duckdb::make_shared_ptr<IsBpChar>(*this);
	}
};

using duckdb::hugeint_t;
using duckdb::uhugeint_t;

struct DecimalConversionInteger {
	static int64_t
	GetPowerOfTen(idx_t index) {
		static const int64_t POWERS_OF_TEN[] {1,
		                                      10,
		                                      100,
		                                      1000,
		                                      10000,
		                                      100000,
		                                      1000000,
		                                      10000000,
		                                      100000000,
		                                      1000000000,
		                                      10000000000,
		                                      100000000000,
		                                      1000000000000,
		                                      10000000000000,
		                                      100000000000000,
		                                      1000000000000000,
		                                      10000000000000000,
		                                      100000000000000000,
		                                      1000000000000000000};
		if (index >= 19) {
			throw duckdb::InternalException("DecimalConversionInteger::GetPowerOfTen - Out of range");
		}
		return POWERS_OF_TEN[index];
	}

	template <class T>
	static T
	Finalize(const NumericVar &, T result) {
		return result;
	}
};

struct DecimalConversionHugeint {
	static hugeint_t
	GetPowerOfTen(idx_t index) {
		static const hugeint_t POWERS_OF_TEN[] {
		    hugeint_t(1),
		    hugeint_t(10),
		    hugeint_t(100),
		    hugeint_t(1000),
		    hugeint_t(10000),
		    hugeint_t(100000),
		    hugeint_t(1000000),
		    hugeint_t(10000000),
		    hugeint_t(100000000),
		    hugeint_t(1000000000),
		    hugeint_t(10000000000),
		    hugeint_t(100000000000),
		    hugeint_t(1000000000000),
		    hugeint_t(10000000000000),
		    hugeint_t(100000000000000),
		    hugeint_t(1000000000000000),
		    hugeint_t(10000000000000000),
		    hugeint_t(100000000000000000),
		    hugeint_t(1000000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10),
		    hugeint_t(1000000000000000000) * hugeint_t(100),
		    hugeint_t(1000000000000000000) * hugeint_t(1000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(10000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(100000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(10),
		    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(100)};
		if (index >= 39) {
			throw duckdb::InternalException("DecimalConversionHugeint::GetPowerOfTen - Out of range");
		}
		return POWERS_OF_TEN[index];
	}

	static hugeint_t
	Finalize(const NumericVar &, hugeint_t result) {
		return result;
	}
};

struct DecimalConversionDouble {
	static double
	GetPowerOfTen(idx_t index) {
		return pow(10, double(index));
	}

	static double
	Finalize(const NumericVar &numeric, double result) {
		return result / GetPowerOfTen(numeric.dscale);
	}
};

// duckdb BIT maps to both BITOID and VARBITOID; convert to VARBITOID for generality.
static Datum
ConvertVarbitDatum(const duckdb::Value &value) {
	const std::string value_str = value.ToString();

	// Rely on PG conversion: BIT padding differs between duckdb and PG, so memcpy-ing the bits is non-trivial.
	return pgddb::pg::StringToVarbit(value_str.c_str());
}

static inline bool
ValidDate(duckdb::date_t dt) {
	if (dt == duckdb::date_t::infinity() || dt == duckdb::date_t::ninfinity())
		return true;
	return dt >= PGDUCKDB_PG_MIN_DATE_VALUE && dt <= PGDUCKDB_PG_MAX_DATE_VALUE;
}

static inline bool
ValidTimestampOrTimestampTz(int64_t timestamp) {
	// Intersection of PG and DuckDB timestamp ranges, capped at a whole year (294246-12-31).
	return timestamp >= PGDUCKDB_MIN_TIMESTAMP_VALUE && timestamp < PGDUCKDB_MAX_TIMESTAMP_VALUE;
}

Datum
ConvertToStringDatum(const duckdb::Value &value) {
	auto str = value.ToString();
	auto varchar = str.c_str();
	auto varchar_len = str.size();

	text *result = (text *)palloc0(varchar_len + VARHDRSZ);
	SET_VARSIZE(result, varchar_len + VARHDRSZ);
	memcpy(VARDATA(result), varchar, varchar_len);
	return PointerGetDatum(result);
}

static inline Datum
ConvertBoolDatum(const duckdb::Value &value) {
	return value.GetValue<bool>();
}

static inline Datum
ConvertCharDatum(const duckdb::Value &value) {
	return value.GetValue<int8_t>();
}

static inline Datum
ConvertInt2Datum(const duckdb::Value &value) {
	if (value.type().id() == duckdb::LogicalTypeId::UTINYINT) {
		return UInt8GetDatum(value.GetValue<uint8_t>());
	}
	return Int16GetDatum(value.GetValue<int16_t>());
}

static inline Datum
ConvertInt4Datum(const duckdb::Value &value) {
	if (value.type().id() == duckdb::LogicalTypeId::USMALLINT) {
		return UInt16GetDatum(value.GetValue<uint16_t>());
	}
	return Int32GetDatum(value.GetValue<int32_t>());
}

static inline Datum
ConvertInt8Datum(const duckdb::Value &value) {
	if (value.type().id() == duckdb::LogicalTypeId::UINTEGER) {
		return UInt32GetDatum(value.GetValue<uint32_t>());
	}
	return Int64GetDatum(value.GetValue<int64_t>());
}

static Datum
ConvertBinaryDatum(const duckdb::Value &value) {
	auto str = value.GetValueUnsafe<duckdb::string_t>();
	auto blob_len = str.GetSize();
	auto blob = str.GetDataUnsafe();
	bytea *result = (bytea *)palloc0(blob_len + VARHDRSZ);
	SET_VARSIZE(result, blob_len + VARHDRSZ);
	memcpy(VARDATA(result), blob, blob_len);
	return PointerGetDatum(result);
}

inline Datum
ConvertDateDatum(const duckdb::Value &value) {
	duckdb::date_t date = value.GetValue<duckdb::date_t>();
	if (!ValidDate(date))
		throw duckdb::OutOfRangeException("The value should be between min and max value (%s <-> %s)",
		                                  duckdb::Date::ToString(PGDUCKDB_PG_MIN_DATE_VALUE),
		                                  duckdb::Date::ToString(PGDUCKDB_PG_MAX_DATE_VALUE));

	// +/-infinity use distinct sentinels in PG vs duckdb
	if (date == duckdb::date_t::ninfinity())
		return DateADTGetDatum(DATEVAL_NOBEGIN);
	else if (date == duckdb::date_t::infinity())
		return DateADTGetDatum(DATEVAL_NOEND);

	return DateADTGetDatum(date.days - PGDUCKDB_DUCK_DATE_OFFSET);
}

static Datum
ConvertIntervalDatum(const duckdb::Value &value) {
	duckdb::interval_t duckdb_interval = value.GetValue<duckdb::interval_t>();
	Interval *pg_interval = static_cast<Interval *>(palloc(sizeof(Interval)));
	pg_interval->month = duckdb_interval.months;
	pg_interval->day = duckdb_interval.days;
	pg_interval->time = duckdb_interval.micros;
	return IntervalPGetDatum(pg_interval);
}

static Datum
ConvertTimeDatum(const duckdb::Value &value) {
	const int64_t microsec = value.GetValue<int64_t>();
	const TimeADT pg_time = microsec;
	return Int64GetDatum(pg_time);
}

static Datum
ConvertTimeTzDatum(const duckdb::Value &value) {
	duckdb::dtime_tz_t dt_tz = value.GetValue<duckdb::dtime_tz_t>();
	const int64_t micros = dt_tz.time().micros;
	const int32_t tz_offset = dt_tz.offset();

	TimeTzADT *result = static_cast<TimeTzADT *>(palloc(sizeof(TimeTzADT)));
	result->time = micros;
	// PG and duckdb store tz offsets with opposite signs (e.g. +05: duckdb 18000, PG -18000).
	result->zone = -tz_offset;
	return TimeTzADTPGetDatum(result);
}

inline Datum
ConvertTimestampDatum(const duckdb::Value &value) {
	int64_t rawValue = value.GetValue<int64_t>();

	if (rawValue == static_cast<int64_t>(duckdb::timestamp_t::ninfinity()))
		return TimestampGetDatum(DT_NOBEGIN);
	else if (rawValue == static_cast<int64_t>(duckdb::timestamp_t::infinity()))
		return TimestampGetDatum(DT_NOEND);

	// Normalize to microseconds based on the timestamp unit.
	switch (value.type().id()) {
	case duckdb::LogicalType::TIMESTAMP_MS:
		rawValue *= 1000;
		break;
	case duckdb::LogicalType::TIMESTAMP_NS:
		rawValue /= 1000;
		break;
	case duckdb::LogicalType::TIMESTAMP_S:
		rawValue *= 1000000;
		break;
	default:
		break;
	}

	if (!ValidTimestampOrTimestampTz(rawValue))
		throw duckdb::OutOfRangeException(
		    "The Timestamp value should be between min and max value (%s <-> %s)",
		    duckdb::Timestamp::ToString(static_cast<duckdb::timestamp_t>(PGDUCKDB_MIN_TIMESTAMP_VALUE)),
		    duckdb::Timestamp::ToString(static_cast<duckdb::timestamp_t>(PGDUCKDB_MAX_TIMESTAMP_VALUE)));

	return TimestampGetDatum(rawValue - PGDUCKDB_DUCK_TIMESTAMP_OFFSET);
}

inline Datum
ConvertTimestampTzDatum(const duckdb::Value &value) {
	duckdb::timestamp_tz_t timestamp = value.GetValue<duckdb::timestamp_tz_t>();
	int64_t rawValue = timestamp.value;

	if (rawValue == static_cast<int64_t>(duckdb::timestamp_t::ninfinity()))
		return TimestampTzGetDatum(DT_NOBEGIN);
	else if (rawValue == static_cast<int64_t>(duckdb::timestamp_t::infinity()))
		return TimestampTzGetDatum(DT_NOEND);

	if (!ValidTimestampOrTimestampTz(rawValue))
		throw duckdb::OutOfRangeException(
		    "The TimestampTz value should be between min and max value (%s <-> %s)",
		    duckdb::Timestamp::ToString(static_cast<duckdb::timestamp_tz_t>(PGDUCKDB_MIN_TIMESTAMP_VALUE)),
		    duckdb::Timestamp::ToString(static_cast<duckdb::timestamp_tz_t>(PGDUCKDB_MAX_TIMESTAMP_VALUE)));

	return TimestampTzGetDatum(rawValue - PGDUCKDB_DUCK_TIMESTAMP_OFFSET);
}

inline Datum
ConvertFloatDatum(const duckdb::Value &value) {
	return Float4GetDatum(value.GetValue<float>());
}

inline Datum
ConvertDoubleDatum(const duckdb::Value &value) {
	return Float8GetDatum(value.GetValue<double>());
}

template <class T, class OP = DecimalConversionInteger>
void
ConvertNumeric(const duckdb::Value &ddb_value, idx_t scale, NumericVar &result) {
	result.dscale = scale;

	T value = ddb_value.GetValueUnsafe<T>();
	if (value < 0) {
		value = -value;
		result.sign = NUMERIC_NEG;
	} else {
		result.sign = NUMERIC_POS;
	}

	// Split into integer and fractional parts around the decimal point.
	T integer_part;
	T fractional_part;
	if (scale == 0) {
		integer_part = value;
		fractional_part = 0;
	} else {
		integer_part = value / T(OP::GetPowerOfTen(scale));
		fractional_part = value % T(OP::GetPowerOfTen(scale));
	}

	constexpr idx_t MAX_DIGITS = sizeof(T) * 4;
	uint16_t integral_digits[MAX_DIGITS];
	uint16_t fractional_digits[MAX_DIGITS];
	int32_t integral_ndigits;

	// Split the integral part into NBASE digits (0..9999 each).
	integral_ndigits = 0;
	while (integer_part > 0) {
		integral_digits[integral_ndigits++] = uint16_t(integer_part % T(NBASE));
		integer_part /= T(NBASE);
	}

	result.weight = integral_ndigits - 1;
	// Always emit NBASE digits for the full scale (trailing zeros included).
	idx_t fractional_ndigits = (scale + DEC_DIGITS - 1) / DEC_DIGITS;
	// PG stores fractional NBASE digits LEFT-aligned: ".12" at scale 2 is "1200", so scale 12 by the gap to the next
	// full digit.
	int32_t correction = fractional_ndigits * DEC_DIGITS - scale;
	fractional_part *= T(OP::GetPowerOfTen(correction));
	for (idx_t i = 0; i < fractional_ndigits; i++) {
		fractional_digits[i] = uint16_t(fractional_part % NBASE);
		fractional_part /= NBASE;
	}

	result.ndigits = integral_ndigits + fractional_ndigits;

	result.buf = (NumericDigit *)palloc(result.ndigits * sizeof(NumericDigit));
	result.digits = result.buf;
	auto &digits = result.digits;

	idx_t digits_idx = 0;
	for (idx_t i = integral_ndigits; i > 0; i--) {
		digits[digits_idx++] = integral_digits[i - 1];
	}
	for (idx_t i = fractional_ndigits; i > 0; i--) {
		digits[digits_idx++] = fractional_digits[i - 1];
	}
}

static Datum
ConvertNumericDatum(const duckdb::Value &value) {
	auto value_type_id = value.type().id();

	if (value.type().id() == duckdb::LogicalTypeId::BIGNUM) {
		// Round-trip via string for simplicity rather than parsing BIGNUM directly.
		const std::string value_str = value.ToString();
		Datum pg_numeric = pgddb::pg::StringToNumeric(value_str.c_str());
		return pg_numeric;
	}

	if (value_type_id == duckdb::LogicalTypeId::DOUBLE) {
		return ConvertDoubleDatum(value);
	}

	NumericVar numeric_var;
	D_ASSERT(value_type_id == duckdb::LogicalTypeId::DECIMAL || value_type_id == duckdb::LogicalTypeId::HUGEINT ||
	         value_type_id == duckdb::LogicalTypeId::UBIGINT || value_type_id == duckdb::LogicalTypeId::UHUGEINT);
	const bool is_decimal = value_type_id == duckdb::LogicalTypeId::DECIMAL;
	uint8_t scale = is_decimal ? duckdb::DecimalType::GetScale(value.type()) : 0;

	switch (value.type().InternalType()) {
	case duckdb::PhysicalType::INT16:
		ConvertNumeric<int16_t>(value, scale, numeric_var);
		break;
	case duckdb::PhysicalType::INT32:
		ConvertNumeric<int32_t>(value, scale, numeric_var);
		break;
	case duckdb::PhysicalType::INT64:
		ConvertNumeric<int64_t>(value, scale, numeric_var);
		break;
	case duckdb::PhysicalType::UINT64:
		ConvertNumeric<uint64_t>(value, scale, numeric_var);
		break;
	case duckdb::PhysicalType::INT128:
		ConvertNumeric<hugeint_t, DecimalConversionHugeint>(value, scale, numeric_var);
		break;
	case duckdb::PhysicalType::UINT128:
		ConvertNumeric<uhugeint_t, DecimalConversionHugeint>(value, scale, numeric_var);
		break;
	default:
		throw duckdb::InvalidInputException(
		    "(PGDuckDB/ConvertNumericDatum) Unrecognized physical type for DECIMAL value");
	}

	auto numeric = PostgresFunctionGuard(make_result, &numeric_var);
	return NumericGetDatum(numeric);
}

static Datum
ConvertUUIDDatum(const duckdb::Value &value) {
	D_ASSERT(value.type().id() == duckdb::LogicalTypeId::UUID);
	D_ASSERT(value.type().InternalType() == duckdb::PhysicalType::INT128);
	auto duckdb_uuid = value.GetValue<hugeint_t>();
	pg_uuid_t *postgres_uuid = (pg_uuid_t *)palloc(sizeof(pg_uuid_t));

	duckdb_uuid.upper ^= (uint64_t(1) << 63);
	uint8_t *uuid_bytes = (uint8_t *)&duckdb_uuid;

	for (int i = 0; i < UUID_LEN; ++i) {
		postgres_uuid->data[i] = uuid_bytes[UUID_LEN - 1 - i];
	}

	return UUIDPGetDatum(postgres_uuid);
}

template <class T>
static inline T DatumGet(Datum value);

// clang-format off
template <> inline bool      DatumGet<bool>(Datum value)      { return DatumGetBool(value); }
template <> inline int16_t   DatumGet<int16_t>(Datum value)   { return DatumGetInt16(value); }
template <> inline int32_t   DatumGet<int32_t>(Datum value)   { return DatumGetInt32(value); }
template <> inline uint32_t  DatumGet<uint32_t>(Datum value)  { return DatumGetUInt32(value); }
template <> inline int64_t   DatumGet<int64_t>(Datum value)   { return DatumGetInt64(value); }
template <> inline float     DatumGet<float>(Datum value)     { return DatumGetFloat4(value); }
template <> inline double    DatumGet<double>(Datum value)    { return DatumGetFloat8(value); }
// clang-format on

template <>
inline duckdb::interval_t
DatumGet<duckdb::interval_t>(Datum value) {
	Interval *pg_interval = DatumGetIntervalP(value);
	duckdb::interval_t duck_interval;
	duck_interval.months = pg_interval->month;
	duck_interval.days = pg_interval->day;
	duck_interval.micros = pg_interval->time;
	return duck_interval;
}

static std::string
DatumGetBitString(Datum value) {
	// Rely on PG conversion: BIT padding differs between duckdb and PG. VarbitToString works for both BIT and VARBIT
	// since PG stores BIT internally as VARBIT.
	return std::string(pgddb::pg::VarbitToString(value));
}

template <>
inline duckdb::dtime_t
DatumGet<duckdb::dtime_t>(Datum value) {
	const TimeADT pg_time = DatumGetTimeADT(value);
	duckdb::dtime_t duckdb_time {pg_time};
	return duckdb_time;
}

template <>
inline duckdb::dtime_tz_t
DatumGet<duckdb::dtime_tz_t>(Datum value) {
	TimeTzADT *tzt = static_cast<TimeTzADT *>(DatumGetTimeTzADTP(value));
	// PG and duckdb store tz offsets with opposite signs (e.g. +05: duckdb 18000, PG -18000).
	const uint64_t bits = duckdb::dtime_tz_t::encode_micros(static_cast<int64_t>(tzt->time)) |
	                      duckdb::dtime_tz_t::encode_offset(-tzt->zone);
	const duckdb::dtime_tz_t duck_time_tz {bits};
	return duck_time_tz;
}

template <>
inline hugeint_t
DatumGet<hugeint_t>(Datum value) {
	const Pointer pg_uuid = DatumGetPointer(value);
	hugeint_t duck_uuid;
	D_ASSERT(UUID_LEN == sizeof(hugeint_t));
	for (idx_t i = 0; i < UUID_LEN; i++) {
		((uint8_t *)&duck_uuid)[UUID_LEN - 1 - i] = ((uint8_t *)pg_uuid)[i];
	}
	duck_uuid.upper ^= (uint64_t(1) << 63);
	return duck_uuid;
}

static inline duckdb::interval_t
DatumGetInterval(Datum value) {
	return DatumGet<duckdb::interval_t>(value);
}
static inline duckdb::dtime_t
DatumGetTime(Datum value) {
	return DatumGet<duckdb::dtime_t>(value);
}
static inline duckdb::dtime_tz_t
DatumGetTimeTz(Datum value) {
	return DatumGet<duckdb::dtime_tz_t>(value);
}
static inline hugeint_t
DatumGetUUID(Datum value) {
	return DatumGet<hugeint_t>(value);
}

template <int32_t OID>
struct PostgresTypeTraits;

template <>
struct PostgresTypeTraits<BOOLOID> {
	static constexpr int16_t typlen = 1;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'c';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertBoolDatum(val);
	}
};

template <>
struct PostgresTypeTraits<CHAROID> {
	static constexpr int16_t typlen = 1;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'c';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertCharDatum(val);
	}
};

template <>
struct PostgresTypeTraits<INT2OID> {
	static constexpr int16_t typlen = 2;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 's';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertInt2Datum(val);
	}
};

template <>
struct PostgresTypeTraits<INT4OID> {
	static constexpr int16_t typlen = 4;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertInt4Datum(val);
	}
};

template <>
struct PostgresTypeTraits<INT8OID> {
	static constexpr int16_t typlen = 8;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'd';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertInt8Datum(val);
	}
};

template <>
struct PostgresTypeTraits<FLOAT4OID> {
	static constexpr int16_t typlen = 4;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertFloatDatum(val);
	}
};

template <>
struct PostgresTypeTraits<FLOAT8OID> {
	static constexpr int16_t typlen = 8;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'd';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertDoubleDatum(val);
	}
};

template <>
struct PostgresTypeTraits<TIMESTAMPOID> {
	static constexpr int16_t typlen = 8;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'd';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertTimestampDatum(val);
	}
};

template <>
struct PostgresTypeTraits<TIMESTAMPTZOID> {
	static constexpr int16_t typlen = 8;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'd';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertTimestampTzDatum(val);
	}
};

template <>
struct PostgresTypeTraits<INTERVALOID> {
	static constexpr int16_t typlen = 16;
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'c';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertIntervalDatum(val);
	}
};

template <>
struct PostgresTypeTraits<VARBITOID> {
	static constexpr int16_t typlen = -1;
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertVarbitDatum(val);
	}
};

template <>
struct PostgresTypeTraits<TIMEOID> {
	static constexpr int16_t typlen = 8;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'd';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertTimeDatum(val);
	}
};

template <>
struct PostgresTypeTraits<TIMETZOID> {
	static constexpr int16_t typlen = 12;
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'd';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertTimeTzDatum(val);
	}
};

template <>
struct PostgresTypeTraits<DATEOID> {
	static constexpr int16_t typlen = 4;
	static constexpr bool typbyval = true;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertDateDatum(val);
	}
};

template <>
struct PostgresTypeTraits<UUIDOID> {
	static constexpr int16_t typlen = 16;
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'c';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertUUIDDatum(val);
	}
};

template <>
struct PostgresTypeTraits<NUMERICOID> {
	static constexpr int16_t typlen = -1; // variable-length
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertNumericDatum(val);
	}
};

template <>
struct PostgresTypeTraits<TEXTOID> {
	static constexpr int16_t typlen = -1; // variable-length
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertToStringDatum(val);
	}
};

template <>
struct PostgresTypeTraits<BYTEAOID> {
	static constexpr int16_t typlen = -1; // variable-length
	static constexpr bool typbyval = false;
	static constexpr char typalign = 'i';

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return ConvertBinaryDatum(val);
	}
};

template <int32_t OID>
struct PostgresOIDMapping {
	static constexpr int32_t postgres_oid = OID;
	static constexpr int16_t typlen = PostgresTypeTraits<OID>::typlen;
	static constexpr bool typbyval = PostgresTypeTraits<OID>::typbyval;
	static constexpr char typalign = PostgresTypeTraits<OID>::typalign;

	static inline Datum
	ToDatum(const duckdb::Value &val) {
		return PostgresTypeTraits<OID>::ToDatum(val);
	}
};

template <class MAPPING>
struct PODArray {
public:
	static ArrayType *
	ConstructArray(Datum *datums, bool *nulls, int ndims, int *dims, int *lower_bound) {
		return construct_md_array(datums, nulls, ndims, dims, lower_bound, MAPPING::postgres_oid, MAPPING::typlen,
		                          MAPPING::typbyval, MAPPING::typalign);
	}

	static Datum
	ConvertToPostgres(const duckdb::Value &val) {
		return MAPPING::ToDatum(val);
	}
};

using BoolArray = PODArray<PostgresOIDMapping<BOOLOID>>;
using CharArray = PODArray<PostgresOIDMapping<CHAROID>>;
using Int2Array = PODArray<PostgresOIDMapping<INT2OID>>;
using Int4Array = PODArray<PostgresOIDMapping<INT4OID>>;
using Int8Array = PODArray<PostgresOIDMapping<INT8OID>>;
using Float4Array = PODArray<PostgresOIDMapping<FLOAT4OID>>;
using Float8Array = PODArray<PostgresOIDMapping<FLOAT8OID>>;
using DateArray = PODArray<PostgresOIDMapping<DATEOID>>;
using TimestampArray = PODArray<PostgresOIDMapping<TIMESTAMPOID>>;
using TimestampTzArray = PODArray<PostgresOIDMapping<TIMESTAMPTZOID>>;
using IntervalArray = PODArray<PostgresOIDMapping<INTERVALOID>>;
using BitArray = PODArray<PostgresOIDMapping<VARBITOID>>;
using TimeArray = PODArray<PostgresOIDMapping<TIMEOID>>;
using TimeTzArray = PODArray<PostgresOIDMapping<TIMETZOID>>;
using UUIDArray = PODArray<PostgresOIDMapping<UUIDOID>>;
using TextArray = PODArray<PostgresOIDMapping<TEXTOID>>;
using NumericArray = PODArray<PostgresOIDMapping<NUMERICOID>>;
using ByteArray = PODArray<PostgresOIDMapping<BYTEAOID>>;

bool
IsNestedType(const duckdb::LogicalTypeId type_id) {
	/* TODO: Add more nested type*/
	return type_id == duckdb::LogicalTypeId::LIST || type_id == duckdb::LogicalTypeId::ARRAY;
}

const duckdb::LogicalType &
GetChildType(const duckdb::LogicalType &type) {
	/* TODO: Add more nested type*/
	switch (type.id()) {
	case duckdb::LogicalTypeId::LIST:
		return duckdb::ListType::GetChildType(type);
	case duckdb::LogicalTypeId::ARRAY:
		return duckdb::ArrayType::GetChildType(type);
	default:
		throw duckdb::InvalidInputException("Expected a LIST or ARRAY type, got '%s' instead", type.ToString());
	}
}

namespace {

static duckdb::LogicalType
CreateUnsupportedPostgresType(std::string error_message) {
	duckdb::LogicalType type = duckdb::LogicalType::INVALID;
	type.SetAlias("UnsupportedPostgresType");
	auto info = duckdb::make_uniq<duckdb::ExtensionTypeInfo>();
	info->modifiers.emplace_back(duckdb::Value(error_message));
	type.SetExtensionInfo(std::move(info));
	return type;
}

} // namespace

bool
ConvertDuckToPostgresValue(TupleTableSlot *slot, duckdb::Value &value, idx_t col) {
	Oid oid = TupleDescAttr(slot->tts_tupleDescriptor, col)->atttypid;

	switch (oid) {
	case BITOID:
	case VARBITOID: {
		slot->tts_values[col] = ConvertVarbitDatum(value);
		break;
	}
	case BOOLOID:
		slot->tts_values[col] = ConvertBoolDatum(value);
		break;
	case CHAROID:
		slot->tts_values[col] = ConvertCharDatum(value);
		break;
	case INT2OID: {
		slot->tts_values[col] = ConvertInt2Datum(value);
		break;
	}
	case INT4OID: {
		slot->tts_values[col] = ConvertInt4Datum(value);
		break;
	}
	case INT8OID: {
		slot->tts_values[col] = ConvertInt8Datum(value);
		break;
	}
	case BPCHAROID:
	case TEXTOID:
	case JSONOID:
	case VARCHAROID: {
		slot->tts_values[col] = ConvertToStringDatum(value);
		break;
	}
	case DATEOID: {
		slot->tts_values[col] = ConvertDateDatum(value);
		break;
	}
	case TIMESTAMPOID: {
		slot->tts_values[col] = ConvertTimestampDatum(value);
		break;
	}
	case TIMESTAMPTZOID: {
		slot->tts_values[col] = ConvertTimestampTzDatum(value);
		break;
	}
	case INTERVALOID: {
		slot->tts_values[col] = ConvertIntervalDatum(value);
		break;
	}
	case TIMEOID: {
		slot->tts_values[col] = ConvertTimeDatum(value);
		break;
	}
	case TIMETZOID:
		slot->tts_values[col] = ConvertTimeTzDatum(value);
		break;
	case FLOAT4OID: {
		slot->tts_values[col] = ConvertFloatDatum(value);
		break;
	}
	case FLOAT8OID: {
		slot->tts_values[col] = ConvertDoubleDatum(value);
		break;
	}
	case NUMERICOID: {
		slot->tts_values[col] = ConvertNumericDatum(value);
		break;
	}
	case UUIDOID: {
		slot->tts_values[col] = ConvertUUIDDatum(value);
		break;
	}
	case BYTEAOID: {
		slot->tts_values[col] = ConvertBinaryDatum(value);
		break;
	}
	case BOOLARRAYOID: {
		ConvertDuckToPostgresArray<BoolArray>(slot, value, col);
		break;
	}
	case CHARARRAYOID: {
		ConvertDuckToPostgresArray<CharArray>(slot, value, col);
		break;
	}
	case INT2ARRAYOID: {
		ConvertDuckToPostgresArray<Int2Array>(slot, value, col);
		break;
	}
	case INT4ARRAYOID: {
		ConvertDuckToPostgresArray<Int4Array>(slot, value, col);
		break;
	}
	case INT8ARRAYOID: {
		ConvertDuckToPostgresArray<Int8Array>(slot, value, col);
		break;
	}
	case BPCHARARRAYOID:
	case TEXTARRAYOID:
	case JSONARRAYOID:
	case VARCHARARRAYOID: {
		ConvertDuckToPostgresArray<TextArray>(slot, value, col);
		break;
	}
	case DATEARRAYOID: {
		ConvertDuckToPostgresArray<DateArray>(slot, value, col);
		break;
	}
	case TIMESTAMPARRAYOID: {
		ConvertDuckToPostgresArray<TimestampArray>(slot, value, col);
		break;
	}
	case TIMESTAMPTZARRAYOID: {
		ConvertDuckToPostgresArray<TimestampTzArray>(slot, value, col);
		break;
	}
	case INTERVALARRAYOID: {
		ConvertDuckToPostgresArray<IntervalArray>(slot, value, col);
		break;
	}
	case BITARRAYOID:
	case VARBITARRAYOID: {
		ConvertDuckToPostgresArray<BitArray>(slot, value, col);
		break;
	}
	case TIMEARRAYOID: {
		ConvertDuckToPostgresArray<TimeArray>(slot, value, col);
		break;
	}
	case TIMETZARRAYOID: {
		ConvertDuckToPostgresArray<TimeTzArray>(slot, value, col);
		break;
	}
	case FLOAT4ARRAYOID: {
		ConvertDuckToPostgresArray<Float4Array>(slot, value, col);
		break;
	}
	case FLOAT8ARRAYOID: {
		ConvertDuckToPostgresArray<Float8Array>(slot, value, col);
		break;
	}
	case NUMERICARRAYOID: {
		ConvertDuckToPostgresArray<NumericArray>(slot, value, col);
		break;
	}
	case UUIDARRAYOID: {
		ConvertDuckToPostgresArray<UUIDArray>(slot, value, col);
		break;
	}
	case BYTEAARRAYOID: {
		ConvertDuckToPostgresArray<ByteArray>(slot, value, col);
		break;
	}
	default: {
		// Non-built-in PG type: try registered hooks in registration order.
		for (auto fn : g_duck_value_to_pg_hooks) {
			if (fn(oid, value, slot, col)) {
				return true;
			}
		}
		elog(WARNING, "(PGDuckDB/ConvertDuckToPostgresValue) Unsupported type: %d", oid);
		return false;
	}
	}
	return true;
}

static inline int32
make_numeric_typmod(int precision, int scale) {
	return ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ;
}

static inline int
numeric_typmod_precision(int32 typmod) {
	return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}

static inline int
numeric_typmod_scale(int32 typmod) {
	return (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
}

static duckdb::LogicalType
ConvertPostgresToBaseDuckColumnType(Form_pg_attribute &attribute) {
	int32 type_modifier = attribute->atttypmod;
	Oid typoid = pgddb::pg::GetBaseTypeAndTypmod(attribute->atttypid, &type_modifier);
	switch (typoid) {
	case BOOLOID:
	case BOOLARRAYOID:
		return duckdb::LogicalTypeId::BOOLEAN;
	case CHAROID:
	case CHARARRAYOID:
		return duckdb::LogicalTypeId::TINYINT;
	case INT2OID:
	case INT2ARRAYOID:
		return duckdb::LogicalTypeId::SMALLINT;
	case INT4OID:
	case INT4ARRAYOID:
		return duckdb::LogicalTypeId::INTEGER;
	case INT8OID:
	case INT8ARRAYOID:
		return duckdb::LogicalTypeId::BIGINT;
	case BPCHAROID:
	case BPCHARARRAYOID:
	case TEXTOID:
	case TEXTARRAYOID:
	case VARCHAROID:
	case VARCHARARRAYOID:
		return duckdb::LogicalTypeId::VARCHAR;
	case DATEOID:
	case DATEARRAYOID:
		return duckdb::LogicalTypeId::DATE;
	case TIMESTAMPOID:
	case TIMESTAMPARRAYOID:
		return duckdb::LogicalTypeId::TIMESTAMP;
	case TIMESTAMPTZOID:
		return duckdb::LogicalTypeId::TIMESTAMP_TZ;
	case INTERVALOID:
	case INTERVALARRAYOID:
		return duckdb::LogicalTypeId::INTERVAL;
	case BITOID:
	case BITARRAYOID:
	case VARBITOID:
	case VARBITARRAYOID:
		return duckdb::LogicalTypeId::BIT;
	case TIMEOID:
	case TIMEARRAYOID:
		return duckdb::LogicalTypeId::TIME;
	case TIMETZOID:
	case TIMETZARRAYOID:
		return duckdb::LogicalTypeId::TIME_TZ;
	case FLOAT4OID:
	case FLOAT4ARRAYOID:
		return duckdb::LogicalTypeId::FLOAT;
	case FLOAT8OID:
	case FLOAT8ARRAYOID:
		return duckdb::LogicalTypeId::DOUBLE;
	case NUMERICOID:
	case NUMERICARRAYOID: {
		auto precision = numeric_typmod_precision(type_modifier);
		auto scale = numeric_typmod_scale(type_modifier);

		// DuckDB decimals support at most 38 digits; higher precision is lossy, so only DOUBLE conversion is offered.
		// https://duckdb.org/docs/stable/sql/data_types/numeric.html#fixed-point-decimals
		if (type_modifier == -1 || precision < 1 || precision > 38 || scale < 0 || scale > 38 || scale > precision) {
			if (convert_unsupported_numeric_to_double) {
				auto extra_type_info = duckdb::make_shared_ptr<NumericAsDouble>();
				return duckdb::LogicalType(duckdb::LogicalTypeId::DOUBLE, std::move(extra_type_info));
			}

			if (type_modifier == -1) {
				return CreateUnsupportedPostgresType(
				    "DuckDB requires the precision of a NUMERIC to be set. You can choose to convert these NUMERICs to "
				    "a DOUBLE by using 'SET duckdb.convert_unsupported_numeric_to_double = true'");
			} else if (precision < 1 || precision > 38) {
				return CreateUnsupportedPostgresType(
				    "DuckDB only supports NUMERIC with a precision of 1-38. You can choose to convert these NUMERICs "
				    "to a DOUBLE by using 'SET duckdb.convert_unsupported_numeric_to_double = true'");
			} else if (scale < 0 || scale > 38) {
				return CreateUnsupportedPostgresType(
				    "DuckDB only supports NUMERIC with a scale of 0-38. You can choose to convert these NUMERICs to a "
				    "DOUBLE by using 'SET duckdb.convert_unsupported_numeric_to_double = true'");
			} else {
				return CreateUnsupportedPostgresType(
				    "DuckDB does not support NUMERIC with a scale that is larger than the precision. You can choose to "
				    "convert these NUMERICs to a DOUBLE by using 'SET duckdb.convert_unsupported_numeric_to_double = "
				    "true'");
			}
		}

		return duckdb::LogicalType::DECIMAL(precision, scale);
	}
	case UUIDOID:
	case UUIDARRAYOID:
		return duckdb::LogicalTypeId::UUID;
	case JSONOID:
	case JSONARRAYOID:
	case JSONBOID:
	case JSONBARRAYOID:
		return duckdb::LogicalType::JSON();
	case REGCLASSOID:
	case REGCLASSARRAYOID:
		return duckdb::LogicalTypeId::UINTEGER;
	case BYTEAOID:
	case BYTEAARRAYOID:
		return duckdb::LogicalTypeId::BLOB;
	default:
		// Non-built-in PG type: try registered hooks in registration order.
		for (auto fn : g_pg_to_base_duck_hooks) {
			duckdb::LogicalType out;
			if (fn(typoid, out)) {
				return out;
			}
		}
		return CreateUnsupportedPostgresType("Oid=" + std::to_string(attribute->atttypid));
	}
}

duckdb::LogicalType
ConvertPostgresToDuckColumnType(Form_pg_attribute &attribute) {
	auto base_type = ConvertPostgresToBaseDuckColumnType(attribute);
	if (base_type.id() == duckdb::LogicalTypeId::INVALID) {
		return base_type;
	}

	if (!pgddb::pg::IsArrayType(attribute->atttypid)) {
		if (!pgddb::pg::IsArrayDomainType(attribute->atttypid)) {
			return base_type;
		}
	}

	auto dimensions = attribute->attndims;

	// PG multi-dim arrays and DuckDB nested lists differ; we map via attndims, which we trust as correct.
	// CTAS can leave attndims at 0 for array types; with no row to inspect we assume single-dimensional.
	if (dimensions == 0) {
		dimensions = 1;
	}

	for (int i = 0; i < dimensions; i++) {
		base_type = duckdb::LogicalType::LIST(base_type);
	}
	return base_type;
}

static Oid
GetPostgresArrayDuckDBType(const duckdb::LogicalType &type, bool throw_error) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
		return BOOLARRAYOID;
	case duckdb::LogicalTypeId::TINYINT:
		return INT2ARRAYOID;
	case duckdb::LogicalTypeId::SMALLINT:
		return INT2ARRAYOID;
	case duckdb::LogicalTypeId::INTEGER:
		return INT4ARRAYOID;
	case duckdb::LogicalTypeId::BIGINT:
		return INT8ARRAYOID;
	case duckdb::LogicalTypeId::HUGEINT:
		return NUMERICARRAYOID;
	case duckdb::LogicalTypeId::UTINYINT:
		return INT2ARRAYOID;
	case duckdb::LogicalTypeId::USMALLINT:
		return INT4ARRAYOID;
	case duckdb::LogicalTypeId::UINTEGER:
		return INT8ARRAYOID;
	case duckdb::LogicalTypeId::VARCHAR:
		return type.IsJSONType() ? JSONARRAYOID : TEXTARRAYOID;
	case duckdb::LogicalTypeId::GEOMETRY:
		return TEXTARRAYOID;
	case duckdb::LogicalTypeId::DATE:
		return DATEARRAYOID;
	case duckdb::LogicalTypeId::TIMESTAMP:
		return TIMESTAMPARRAYOID;
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		return TIMESTAMPTZARRAYOID;
	case duckdb::LogicalTypeId::INTERVAL:
		return INTERVALARRAYOID;
	case duckdb::LogicalTypeId::BIT:
		return VARBITARRAYOID;
	case duckdb::LogicalTypeId::TIME:
		return TIMEARRAYOID;
	case duckdb::LogicalTypeId::TIME_TZ:
		return TIMETZARRAYOID;
	case duckdb::LogicalTypeId::FLOAT:
		return FLOAT4ARRAYOID;
	case duckdb::LogicalTypeId::DOUBLE:
		return FLOAT8ARRAYOID;
	case duckdb::LogicalTypeId::DECIMAL:
		return NUMERICARRAYOID;
	case duckdb::LogicalTypeId::UUID:
		return UUIDARRAYOID;
	case duckdb::LogicalTypeId::BLOB:
		return BYTEAARRAYOID;
	case duckdb::LogicalTypeId::BIGNUM:
		return NUMERICARRAYOID;
	default: {
		// Non-built-in DuckDB type: try registered hooks in registration order.
		for (auto fn : g_duck_to_pg_array_hooks) {
			Oid out;
			if (fn(type, out)) {
				return out;
			}
		}
		if (throw_error) {
			throw duckdb::NotImplementedException("Unsupported DuckDB `LIST` subtype: " + type.ToString());
		} else {
			pd_log(WARNING, "Unsupported DuckDB `LIST` subtype: %s", type.ToString().c_str());
			return InvalidOid;
		}
	}
	}
}

void
CheckForUnsupportedPostgresType(duckdb::LogicalType type) {
	if (type.id() == duckdb::LogicalTypeId::INVALID && type.GetAlias() == "UnsupportedPostgresType") {
		auto info = type.GetExtensionInfo();
		if (info && info->modifiers.size() > 0) {
			// The first modifier carries the error message.
			auto modifier_value = info->modifiers[0];
			throw duckdb::NotImplementedException("Unsupported PostgreSQL type found in query: %s",
			                                      modifier_value.ToString());
		} else {
			throw duckdb::NotImplementedException("Unsupported PostgreSQL type found in query");
		}
	}
}

Oid
GetPostgresDuckDBType(const duckdb::LogicalType &type, bool throw_error) {
	CheckForUnsupportedPostgresType(type);
	switch (type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
		return BOOLOID;
	case duckdb::LogicalTypeId::TINYINT:
		return INT2OID;
	case duckdb::LogicalTypeId::SMALLINT:
		return INT2OID;
	case duckdb::LogicalTypeId::INTEGER:
		return INT4OID;
	case duckdb::LogicalTypeId::BIGINT:
		return INT8OID;
	case duckdb::LogicalTypeId::UBIGINT:
	case duckdb::LogicalTypeId::HUGEINT:
	case duckdb::LogicalTypeId::UHUGEINT:
		return NUMERICOID;
	case duckdb::LogicalTypeId::UTINYINT:
		return INT2OID;
	case duckdb::LogicalTypeId::USMALLINT:
		return INT4OID;
	case duckdb::LogicalTypeId::UINTEGER:
		return INT8OID;
	case duckdb::LogicalTypeId::VARCHAR:
		return type.IsJSONType() ? JSONOID : TEXTOID;
	case duckdb::LogicalTypeId::GEOMETRY:
		return TEXTOID;
	case duckdb::LogicalTypeId::DATE:
		return DATEOID;
	case duckdb::LogicalTypeId::TIMESTAMP:
	case duckdb::LogicalTypeId::TIMESTAMP_SEC:
	case duckdb::LogicalTypeId::TIMESTAMP_MS:
	case duckdb::LogicalTypeId::TIMESTAMP_NS:
		return TIMESTAMPOID;
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		return TIMESTAMPTZOID;
	case duckdb::LogicalTypeId::INTERVAL:
		return INTERVALOID;
	case duckdb::LogicalTypeId::BIT:
		return VARBITOID;
	case duckdb::LogicalTypeId::TIME:
		return TIMEOID;
	case duckdb::LogicalTypeId::TIME_TZ:
		return TIMETZOID;
	case duckdb::LogicalTypeId::FLOAT:
		return FLOAT4OID;
	case duckdb::LogicalTypeId::DOUBLE:
		return FLOAT8OID;
	case duckdb::LogicalTypeId::DECIMAL:
		return NUMERICOID;
	case duckdb::LogicalTypeId::UUID:
		return UUIDOID;
	case duckdb::LogicalTypeId::BIGNUM:
		return NUMERICOID;
	case duckdb::LogicalTypeId::LIST:
	case duckdb::LogicalTypeId::ARRAY: {
		const duckdb::LogicalType *duck_type = &type;
		while (IsNestedType(duck_type->id())) {
			auto &child_type = GetChildType(*duck_type);
			duck_type = &child_type;
		}
		return GetPostgresArrayDuckDBType(*duck_type, throw_error);
	}
	case duckdb::LogicalTypeId::BLOB:
		return BYTEAOID;
	case duckdb::LogicalTypeId::ENUM:
		return VARCHAROID;
	default: {
		// Non-built-in DuckDB type: try registered hooks in registration order.
		for (auto fn : g_duck_to_pg_hooks) {
			Oid out;
			if (fn(type, out)) {
				return out;
			}
		}
		if (throw_error) {
			throw duckdb::NotImplementedException("Could not convert DuckDB type: " + type.ToString() +
			                                      " to Postgres type");
		} else {
			pd_log(WARNING, "Could not convert DuckDB type: %s to Postgres type", type.ToString().c_str());
			return InvalidOid;
		}
	}
	}
}

int32
GetPostgresDuckDBTypemod(const duckdb::LogicalType &type) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::DECIMAL: {
		uint8_t width, scale;
		type.GetDecimalProperties(width, scale);
		return make_numeric_typmod(width, scale);
	}
	default:
		return -1;
	}
}

template <class T>
static void
Append(duckdb::Vector &result, T value, idx_t offset) {
	auto data = duckdb::FlatVector::GetData<T>(result);
	data[offset] = value;
}

template <class T>
static void
AppendDatum(duckdb::Vector &result, Datum value, idx_t offset) {
	Append<T>(result, DatumGet<T>(value), offset);
}

template <bool IS_BPCHAR>
static void
AppendString(duckdb::Vector &result, Datum value, idx_t offset) {
	bool should_free = false;
	Datum v = DetoastIfExternal(value, &should_free);
	void *ptr = DatumGetPointer(v);
	const char *text = VARDATA_ANY(ptr);
	/* Remove the padding of a BPCHAR type. DuckDB expects unpadded value. */
	auto len = IS_BPCHAR ? bpchartruelen(VARDATA_ANY(ptr), VARSIZE_ANY_EXHDR(ptr)) : VARSIZE_ANY_EXHDR(ptr);
	duckdb::string_t str(text, len);

	auto data = duckdb::FlatVector::GetData<duckdb::string_t>(result);
	data[offset] = duckdb::StringVector::AddString(result, str);
	if (should_free) {
		duckdb_free(reinterpret_cast<void *>(v));
	}
}

static void
AppendJsonb(duckdb::Vector &result, Datum value, idx_t offset) {
	alignas(int32) uint8_t buf[kShortRealignBufSize];
	bool should_free = false;
	Datum v = DetoastPostgresDatumInline(value, buf, &should_free);
	auto jsonb = DatumGetJsonbP(v);
	StringInfo str = PostgresFunctionGuard(makeStringInfo);
	auto json_str = PostgresFunctionGuard(JsonbToCString, str, &jsonb->root, VARSIZE(jsonb));
	auto data = duckdb::FlatVector::GetData<duckdb::string_t>(result);
	data[offset] = duckdb::StringVector::AddString(result, json_str, str->len);
	if (should_free) {
		duckdb_free(reinterpret_cast<void *>(v));
	}
}

static void
AppendBit(duckdb::Vector &result, Datum value, idx_t offset) {
	alignas(int32) uint8_t buf[kShortRealignBufSize];
	bool should_free = false;
	Datum v = DetoastPostgresDatumInline(value, buf, &should_free);
	Append<duckdb::bitstring_t>(result, duckdb::Bit::ToBit(DatumGetBitString(v)), offset);
	if (should_free) {
		duckdb_free(reinterpret_cast<void *>(v));
	}
}

static void
AppendBlob(duckdb::Vector &result, Datum value, idx_t offset) {
	bool should_free = false;
	Datum v = DetoastIfExternal(value, &should_free);
	void *ptr = DatumGetPointer(v);
	const char *bytea_data = VARDATA_ANY(ptr);
	size_t bytea_length = VARSIZE_ANY_EXHDR(ptr);
	const duckdb::string_t s(bytea_data, bytea_length);
	auto data = duckdb::FlatVector::GetData<duckdb::string_t>(result);
	data[offset] = duckdb::StringVector::AddStringOrBlob(result, s);
	if (should_free) {
		duckdb_free(reinterpret_cast<void *>(v));
	}
}

static void
AppendDate(duckdb::Vector &result, Datum value, idx_t offset) {
	auto date = DatumGetDateADT(value);
	if (date == DATEVAL_NOBEGIN) {
		// -infinity value is different between PG and duck
		Append<duckdb::date_t>(result, duckdb::date_t::ninfinity(), offset);
		return;
	}
	if (date == DATEVAL_NOEND) {
		Append<duckdb::date_t>(result, duckdb::date_t::infinity(), offset);
		return;
	}

	Append<duckdb::date_t>(result, duckdb::date_t(static_cast<int32_t>(date + PGDUCKDB_DUCK_DATE_OFFSET)), offset);
}

static void
AppendTimestamp(duckdb::Vector &result, Datum value, idx_t offset) {
	int64_t timestamp = static_cast<int64_t>(DatumGetTimestamp(value));
	if (timestamp == DT_NOBEGIN) {
		// -infinity value is different between PG and duck
		Append<duckdb::timestamp_t>(result, duckdb::timestamp_t::ninfinity(), offset);
		return;
	}
	if (timestamp == DT_NOEND) {
		Append<duckdb::timestamp_t>(result, duckdb::timestamp_t::infinity(), offset);
		return;
	}

	if (!ValidTimestampOrTimestampTz(timestamp + PGDUCKDB_DUCK_TIMESTAMP_OFFSET))
		throw duckdb::OutOfRangeException(
		    "The Timestamp value should be between min and max value (%s <-> %s)",
		    duckdb::Timestamp::ToString(static_cast<duckdb::timestamp_t>(PGDUCKDB_MIN_TIMESTAMP_VALUE)),
		    duckdb::Timestamp::ToString(static_cast<duckdb::timestamp_t>(PGDUCKDB_MAX_TIMESTAMP_VALUE)));

	Append<duckdb::timestamp_t>(result, duckdb::timestamp_t(timestamp + PGDUCKDB_DUCK_TIMESTAMP_OFFSET), offset);
}

static void
AppendTimestampTz(duckdb::Vector &result, Datum value, idx_t offset) {
	int64_t timestamp = static_cast<int64_t>(DatumGetTimestampTz(value));
	if (timestamp == DT_NOBEGIN) {
		// -infinity value is different between PG and duck
		Append<duckdb::timestamp_tz_t>(result, static_cast<duckdb::timestamp_tz_t>(duckdb::timestamp_t::ninfinity()),
		                               offset);
		return;
	}
	if (timestamp == DT_NOEND) {
		Append<duckdb::timestamp_tz_t>(result, static_cast<duckdb::timestamp_tz_t>(duckdb::timestamp_t::infinity()),
		                               offset);
		return;
	}

	if (!ValidTimestampOrTimestampTz(timestamp + PGDUCKDB_DUCK_TIMESTAMP_OFFSET))
		throw duckdb::OutOfRangeException(
		    "The TimestampTz value should be between min and max value (%s <-> %s)",
		    duckdb::Timestamp::ToString(static_cast<duckdb::timestamp_tz_t>(PGDUCKDB_MIN_TIMESTAMP_VALUE)),
		    duckdb::Timestamp::ToString(static_cast<duckdb::timestamp_tz_t>(PGDUCKDB_MAX_TIMESTAMP_VALUE)));

	Append<duckdb::timestamp_tz_t>(result, duckdb::timestamp_tz_t(timestamp + PGDUCKDB_DUCK_TIMESTAMP_OFFSET), offset);
}

template <class T, class OP = DecimalConversionInteger>
T
ConvertDecimal(const NumericVar &numeric) {
	auto scale_POWER = OP::GetPowerOfTen(numeric.dscale);

	if (numeric.ndigits == 0) {
		return 0;
	}

	T integral_part = 0, fractional_part = 0;
	if (numeric.weight >= 0) {
		int32_t digit_index = 0;
		integral_part = numeric.digits[digit_index++];
		for (; digit_index <= numeric.weight; digit_index++) {
			integral_part *= NBASE;
			if (digit_index < numeric.ndigits) {
				integral_part += numeric.digits[digit_index];
			}
		}
		integral_part *= scale_POWER;
	}

	// Reconcile the fractional part's power-of-ten against scale: NBASE multiplications rarely land exactly on
	// scale (and suppressed trailing zeros undershoot), so correct by the difference.
	if (numeric.ndigits > numeric.weight + 1) {
		auto fractional_power = (numeric.ndigits - numeric.weight - 1) * DEC_DIGITS;
		auto fractional_power_correction = fractional_power - numeric.dscale;
		D_ASSERT(fractional_power_correction < 20);
		fractional_part = 0;
		for (int32_t i = duckdb::MaxValue<int32_t>(0, numeric.weight + 1); i < numeric.ndigits; i++) {
			if (i + 1 < numeric.ndigits) {
				// more digits remain - no need to compensate yet
				fractional_part *= NBASE;
				fractional_part += numeric.digits[i];
			} else {
				// last digit, compensate
				T final_base = NBASE;
				T final_digit = numeric.digits[i];
				if (fractional_power_correction >= 0) {
					T compensation = OP::GetPowerOfTen(fractional_power_correction);
					final_base /= compensation;
					final_digit /= compensation;
				} else {
					T compensation = OP::GetPowerOfTen(-fractional_power_correction);
					final_base *= compensation;
					final_digit *= compensation;
				}
				fractional_part *= final_base;
				fractional_part += final_digit;
			}
		}
	}

	auto base_res = OP::Finalize(numeric, integral_part + fractional_part);
	return numeric.sign == NUMERIC_NEG ? -base_res : base_res;
}

template <class T>
static void
AppendDecimal(duckdb::Vector &result, Datum value, idx_t offset) {
	alignas(int32) uint8_t buf[kShortRealignBufSize];
	bool should_free = false;
	Datum v = DetoastPostgresDatumInline(value, buf, &should_free);
	auto numeric_var = FromNumeric(DatumGetNumeric(v));
	if constexpr (std::is_same_v<T, hugeint_t>) {
		Append<T>(result, ConvertDecimal<T, DecimalConversionHugeint>(numeric_var), offset);
	} else if constexpr (std::is_same_v<T, double>) {
		Append<T>(result, ConvertDecimal<T, DecimalConversionDouble>(numeric_var), offset);
	} else {
		Append<T>(result, ConvertDecimal<T>(numeric_var), offset);
	}
	if (should_free) {
		duckdb_free(reinterpret_cast<void *>(v));
	}
}

void
NumericToDecimalBytes(Datum value, int width_bytes, void *out) {
	alignas(int32) uint8_t buf[kShortRealignBufSize];
	bool should_free = false;
	Datum v = DetoastPostgresDatumInline(value, buf, &should_free);
	auto numeric_var = FromNumeric(DatumGetNumeric(v));
	switch (width_bytes) {
	case 4:
		*reinterpret_cast<int32_t *>(out) = ConvertDecimal<int32_t>(numeric_var);
		break;
	case 8:
		*reinterpret_cast<int64_t *>(out) = ConvertDecimal<int64_t>(numeric_var);
		break;
	case 16: {
		hugeint_t h = ConvertDecimal<hugeint_t, DecimalConversionHugeint>(numeric_var);
		// Arrow decimal128 is little-endian two's-complement: low 8 bytes then high.
		reinterpret_cast<uint64_t *>(out)[0] = h.lower;
		reinterpret_cast<int64_t *>(out)[1] = h.upper;
		break;
	}
	default:
		break;
	}
	if (should_free) {
		duckdb_free(reinterpret_cast<void *>(v));
	}
}

static void
AppendList(duckdb::Vector &result, Datum value, idx_t offset) {
	alignas(int32) uint8_t buf[kShortRealignBufSize];
	bool should_free = false;
	Datum detoasted_value = DetoastPostgresDatumInline(value, buf, &should_free);
	auto array = DatumGetArrayTypeP(detoasted_value);

	auto ndims = ARR_NDIM(array);
	int *dims = ARR_DIMS(array);
	auto elem_type = ARR_ELEMTYPE(array);

	int16 typlen;
	bool typbyval;
	char typalign;
	PostgresFunctionGuard(get_typlenbyvalalign, elem_type, &typlen, &typbyval, &typalign);

	int nelems;
	Datum *elems;
	bool *nulls;
	PostgresFunctionGuard(deconstruct_array, array, elem_type, typlen, typbyval, typalign, &elems, &nulls, &nelems);

	if (ndims == -1) {
		throw duckdb::InternalException("Array type has an ndims of -1, so it's actually not an array??");
	}
	duckdb::Vector *vec = &result;
	int write_offset = offset;
	for (int dim = 0; dim < ndims; dim++) {
		auto previous_dimension = dim ? dims[dim - 1] : 1;
		auto dimension = dims[dim];
		if (vec->GetType().id() != duckdb::LogicalTypeId::LIST) {
			throw duckdb::InvalidInputException(
			    "Dimensionality of the schema and the data does not match, data contains more dimensions than the "
			    "amount of dimensions specified by the schema");
		}
		auto child_offset = duckdb::ListVector::GetListSize(*vec);
		auto list_data = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*vec);
		for (int entry = 0; entry < previous_dimension; entry++) {
			// All lists in a PG row share one dimension (e.g. [[1,2],[2,3,4]] is rejected).
			list_data[write_offset + entry] = duckdb::list_entry_t(child_offset + (dimension * entry), dimension);
		}
		auto new_child_size = child_offset + (dimension * previous_dimension);
		duckdb::ListVector::Reserve(*vec, new_child_size);
		duckdb::ListVector::SetListSize(*vec, new_child_size);
		write_offset = child_offset;
		auto &child = duckdb::ListVector::GetEntry(*vec);
		vec = &child;
	}
	if (ndims == 0) {
		D_ASSERT(nelems == 0);
		auto child_offset = duckdb::ListVector::GetListSize(*vec);
		auto list_data = duckdb::FlatVector::GetData<duckdb::list_entry_t>(*vec);
		list_data[write_offset] = duckdb::list_entry_t(child_offset, 0);
		vec = &duckdb::ListVector::GetEntry(*vec);
	} else if (vec->GetType().id() == duckdb::LogicalTypeId::LIST) {
		throw duckdb::InvalidInputException(
		    "Dimensionality of the schema and the data does not match, data contains fewer dimensions than the "
		    "amount of dimensions specified by the schema");
	}

	for (int i = 0; i < nelems; i++) {
		idx_t dest_idx = write_offset + i;
		if (nulls[i]) {
			auto &array_mask = duckdb::FlatVector::Validity(*vec);
			array_mask.SetInvalid(dest_idx);
			continue;
		}
		ConvertPostgresToDuckValue(elem_type, elems[i], *vec, dest_idx);
	}
	if (should_free) {
		duckdb_free(reinterpret_cast<void *>(detoasted_value));
	}
}

// Converts a prepared-statement parameter Datum to a DuckDB Value. Does not detoast, so an on-disk Datum is UB.
duckdb::Value
ConvertPostgresParameterToDuckValue(Datum value, Oid postgres_type) {
	switch (postgres_type) {
	case BOOLOID:
		return duckdb::Value::BOOLEAN(DatumGetBool(value));
	case INT2OID:
		return duckdb::Value::SMALLINT(DatumGetInt16(value));
	case INT4OID:
		return duckdb::Value::INTEGER(DatumGetInt32(value));
	case INT8OID:
		return duckdb::Value::BIGINT(DatumGetInt64(value));
	case BPCHAROID:
	case TEXTOID:
	case JSONOID:
	case VARCHAROID: {
		// FIXME: TextDatumGetCString allocates and needs a guard, but it's a macro the guard template can't wrap.
		return duckdb::Value(TextDatumGetCString(value));
	}
	case DATEOID:
		return duckdb::Value::DATE(duckdb::date_t(DatumGetDateADT(value) + PGDUCKDB_DUCK_DATE_OFFSET));
	case TIMESTAMPOID:
		return duckdb::Value::TIMESTAMP(duckdb::timestamp_t(DatumGetTimestamp(value) + PGDUCKDB_DUCK_TIMESTAMP_OFFSET));
	case TIMESTAMPTZOID:
		return duckdb::Value::TIMESTAMPTZ(
		    duckdb::timestamp_tz_t(DatumGetTimestampTz(value) + PGDUCKDB_DUCK_TIMESTAMP_OFFSET));
	case INTERVALOID:
		return duckdb::Value::INTERVAL(DatumGetInterval(value));
	case BITOID:
	case VARBITOID:
		return duckdb::Value::BIT(DatumGetBitString(value));
	case TIMEOID:
		return duckdb::Value::TIME(DatumGetTime(value));
	case TIMETZOID:
		return duckdb::Value::TIMETZ(DatumGetTimeTz(value));
	case FLOAT4OID:
		return duckdb::Value::FLOAT(DatumGetFloat4(value));
	case FLOAT8OID:
		return duckdb::Value::DOUBLE(DatumGetFloat8(value));
	case UUIDOID:
		return duckdb::Value::UUID(DatumGetUUID(value));
	default:
		elog(ERROR, "Could not convert Postgres parameter of type: %d to DuckDB type", postgres_type);
	}
}

PostgresToDuckValueFn
GetPostgresToDuckValueFn(Oid attr_type, duckdb::Vector &result) {
	auto &type = result.GetType();
	switch (type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
		return &AppendDatum<bool>;
	case duckdb::LogicalTypeId::TINYINT:
	case duckdb::LogicalTypeId::SMALLINT:
		return &AppendDatum<int16_t>;
	case duckdb::LogicalTypeId::INTEGER:
		return &AppendDatum<int32_t>;
	case duckdb::LogicalTypeId::UINTEGER:
		return &AppendDatum<uint32_t>;
	case duckdb::LogicalTypeId::BIGINT:
		return &AppendDatum<int64_t>;
	case duckdb::LogicalTypeId::VARCHAR:
		// NOTE: This also handles JSON
		if (attr_type == JSONBOID) {
			return &AppendJsonb;
		} else if (attr_type == BPCHAROID) {
			return &AppendString<true>;
		} else {
			return &AppendString<false>;
		}
	case duckdb::LogicalTypeId::DATE:
		return &AppendDate;
	case duckdb::LogicalTypeId::TIMESTAMP_SEC:
	case duckdb::LogicalTypeId::TIMESTAMP_MS:
	case duckdb::LogicalTypeId::TIMESTAMP_NS:
	case duckdb::LogicalTypeId::TIMESTAMP:
		return &AppendTimestamp;
	case duckdb::LogicalTypeId::TIMESTAMP_TZ:
		return &AppendTimestampTz;
	case duckdb::LogicalTypeId::INTERVAL:
		return &AppendDatum<duckdb::interval_t>;
	case duckdb::LogicalTypeId::BIT:
		return &AppendBit;
	case duckdb::LogicalTypeId::TIME:
		return &AppendDatum<duckdb::dtime_t>;
	case duckdb::LogicalTypeId::TIME_TZ:
		return &AppendDatum<duckdb::dtime_tz_t>;
	case duckdb::LogicalTypeId::FLOAT:
		return &AppendDatum<float>;
	case duckdb::LogicalTypeId::DOUBLE: {
		auto aux_info = type.GetAuxInfoShrPtr();
		if (attr_type == NUMERICOID && aux_info && dynamic_cast<NumericAsDouble *>(aux_info.get())) {
			return &AppendDecimal<double>;
		}
		return &AppendDatum<double>;
	}
	case duckdb::LogicalTypeId::DECIMAL: {
		auto physical_type = type.InternalType();
		switch (physical_type) {
		case duckdb::PhysicalType::INT16:
			return &AppendDecimal<int16_t>;
		case duckdb::PhysicalType::INT32:
			return &AppendDecimal<int32_t>;
		case duckdb::PhysicalType::INT64:
			return &AppendDecimal<int64_t>;
		case duckdb::PhysicalType::INT128:
			return &AppendDecimal<hugeint_t>;
		default:
			throw duckdb::InternalException("Unrecognized physical type (%s) for DECIMAL value",
			                                duckdb::EnumUtil::ToString(physical_type));
		}
	}
	case duckdb::LogicalTypeId::UUID:
		return &AppendDatum<hugeint_t>;
	case duckdb::LogicalTypeId::BLOB:
		return &AppendBlob;
	case duckdb::LogicalTypeId::LIST:
		return &AppendList;
	default:
		// Non-built-in type: try registered hooks in registration order.
		for (auto fn : g_pg_to_duck_value_fn_hooks) {
			PostgresToDuckValueFn out = nullptr;
			if (fn(attr_type, result, out)) {
				return out;
			}
		}
		throw duckdb::NotImplementedException("(DuckDB/GetPostgresToDuckValueFn) Unsupported pgduckdb type: %s",
		                                      type.ToString().c_str());
	}
}

void
ConvertPostgresToDuckValue(Oid attr_type, Datum value, duckdb::Vector &result, idx_t offset) {
	auto fn = GetPostgresToDuckValueFn(attr_type, result);
	fn(result, value, offset);
}

// True if PG->DuckDB conversion needs no PG-specific functions or allocations (e.g. palloc), hence no lock.
static bool
IsThreadSafeTypeForPostgresToDuckDB(Oid attr_type, duckdb::LogicalTypeId duckdb_type) {
	if (duckdb_type == duckdb::LogicalTypeId::VARCHAR) {
		return attr_type != JSONBOID;
	}
	if (duckdb_type == duckdb::LogicalTypeId::LIST || duckdb_type == duckdb::LogicalTypeId::BIT) {
		return false;
	}

	return true;
}

// Inserts a batch of tuples into a chunk; deforms minimal slots in place. Unsafe types (JSONB/LIST/VARBIT) are
// converted under the global lock and a dedicated PG memory context.
void
InsertTuplesIntoChunk(duckdb::DataChunk &output, pgddb::PostgresScanLocalState &scan_local_state,
                      TupleTableSlot **slots, int num_slots) {
	if (num_slots == 0) {
		return;
	}

	// Deform all slots up front for the column-major loop; slot_getallattrs is allocation-free here, so no lock needed.
	for (int row = 0; row < num_slots; row++) {
		slot_getallattrs(slots[row]);
	}

	auto scan_global_state = scan_local_state.global_state;
	int natts = slots[0]->tts_tupleDescriptor->natts;
	D_ASSERT(!scan_global_state->count_tuples_only);

	for (int duckdb_output_index = 0; duckdb_output_index < natts; duckdb_output_index++) {
		auto &result = output.data[duckdb_output_index];
		auto attr = TupleDescAttr(slots[0]->tts_tupleDescriptor, duckdb_output_index);
		bool is_safe_type = IsThreadSafeTypeForPostgresToDuckDB(attr->atttypid, result.GetType().id());
		auto convert_fn = GetPostgresToDuckValueFn(attr->atttypid, result);
		auto &validity = duckdb::FlatVector::Validity(result);
		const idx_t base_offset = scan_local_state.output_vector_size;

		std::unique_ptr<std::lock_guard<std::recursive_mutex>> lock_guard;
		MemoryContext old_ctx = NULL;
		if (!is_safe_type) {
			lock_guard = std::make_unique<std::lock_guard<std::recursive_mutex>>(pgddb::GlobalProcessLock::GetLock());
			old_ctx = pgddb::pg::MemoryContextSwitchTo(scan_global_state->duckdb_scan_memory_ctx);
		}

		for (int row = 0; row < num_slots; row++) {
			if (slots[row]->tts_isnull[duckdb_output_index]) {
				validity.SetInvalid(base_offset + row);
			} else {
				convert_fn(result, slots[row]->tts_values[duckdb_output_index], base_offset + row);
			}
		}

		if (!is_safe_type) {
			pgddb::pg::MemoryContextSwitchTo(old_ctx);
			pgddb::pg::MemoryContextReset(scan_global_state->duckdb_scan_memory_ctx);
		}
	}

	scan_local_state.output_vector_size += num_slots;
	scan_global_state->total_row_count += num_slots;
}

NumericVar
FromNumeric(Numeric num) {
	NumericVar dest;
	dest.ndigits = NUMERIC_NDIGITS(num);
	dest.weight = NUMERIC_WEIGHT(num);
	dest.sign = NUMERIC_SIGN(num);
	dest.dscale = NUMERIC_DSCALE(num);
	dest.digits = NUMERIC_DIGITS(num);
	dest.buf = NULL; /* digits array is not palloc'd */
	return dest;
}
} // namespace pgddb
