// Vendored type conversion utilities from pg_duckdb
// Simplified to support only types needed for DuckLake metadata operations

#include "pgducklake/pgducklake_pg_types.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"

extern "C" {
#include "postgres.h"
#include "access/detoast.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/expandeddatum.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"
}

namespace pgducklake {

// DuckDB has date starting from 1/1/1970 while PG starts from 1/1/2000
constexpr int32_t DUCK_DATE_OFFSET = 10957;
constexpr int64_t DUCK_TIMESTAMP_OFFSET =
    static_cast<int64_t>(DUCK_DATE_OFFSET) * static_cast<int64_t>(86400000000);

//------------------------------------------------------------------------------
// Detoasting
//------------------------------------------------------------------------------

Datum DetoastPostgresDatum(struct varlena *attr, bool *should_free) {
  struct varlena *toasted_value = nullptr;
  *should_free = true;

  if (VARATT_IS_EXTERNAL_ONDISK(attr)) {
    // Fetch from toast table
    toasted_value = (struct varlena *)PG_DETOAST_DATUM(PointerGetDatum(attr));
  } else if (VARATT_IS_EXTERNAL_EXPANDED(attr)) {
    // Expanded object
    ExpandedObjectHeader *eoh = DatumGetEOHP(PointerGetDatum(attr));
    Size resultsize = EOH_get_flat_size(eoh);
    toasted_value = (struct varlena *)palloc(resultsize);
    EOH_flatten_into(eoh, (void *)toasted_value, resultsize);
  } else if (VARATT_IS_COMPRESSED(attr)) {
    // Compressed value
    toasted_value = (struct varlena *)PG_DETOAST_DATUM(PointerGetDatum(attr));
  } else if (VARATT_IS_SHORT(attr)) {
    // Short varlena - expand to normal format
    Size data_size = VARSIZE_SHORT(attr) - VARHDRSZ_SHORT;
    Size new_size = data_size + VARHDRSZ;
    toasted_value = (struct varlena *)palloc(new_size);
    SET_VARSIZE(toasted_value, new_size);
    memcpy(VARDATA(toasted_value), VARDATA_SHORT(attr), data_size);
  } else {
    // Already detoasted
    toasted_value = attr;
    *should_free = false;
  }

  return PointerGetDatum(toasted_value);
}

//------------------------------------------------------------------------------
// Type conversion - PostgreSQL to DuckDB
//------------------------------------------------------------------------------

static duckdb::LogicalType ConvertPostgresToBaseDuckType(Oid typid) {
  switch (typid) {
  // Boolean
  case BOOLOID:
    return duckdb::LogicalType::BOOLEAN;

  // Integer types
  case INT2OID:
    return duckdb::LogicalType::SMALLINT;
  case INT4OID:
    return duckdb::LogicalType::INTEGER;
  case INT8OID:
    return duckdb::LogicalType::BIGINT;

  // Floating point
  case FLOAT4OID:
    return duckdb::LogicalType::FLOAT;
  case FLOAT8OID:
    return duckdb::LogicalType::DOUBLE;

  // Numeric/Decimal
  case NUMERICOID:
    return duckdb::LogicalType::DOUBLE; // Simplified - convert numeric to double

  // String types
  case TEXTOID:
  case VARCHAROID:
  case BPCHAROID:
  case NAMEOID:
    return duckdb::LogicalType::VARCHAR;

  // Date/Time types
  case DATEOID:
    return duckdb::LogicalType::DATE;
  case TIMESTAMPOID:
    return duckdb::LogicalType::TIMESTAMP;
  case TIMESTAMPTZOID:
    return duckdb::LogicalType::TIMESTAMP_TZ;
  case TIMEOID:
    return duckdb::LogicalType::TIME;

  // UUID
  case UUIDOID:
    return duckdb::LogicalType::UUID;

  // JSON
  case JSONOID:
  case JSONBOID:
    return duckdb::LogicalType::JSON();

  default:
    return duckdb::LogicalType::SQLNULL; // Unsupported type
  }
}

duckdb::LogicalType ConvertPostgresToDuckColumnType(Form_pg_attribute &attribute) {
  // Check array types first: array OIDs (e.g. 3807 for jsonb[]) are not in the
  // base-type switch, but their element type (e.g. 3802 for jsonb) is.
  Oid elem_type = get_element_type(attribute->atttypid);
  if (elem_type != InvalidOid) {
    auto elem_base_type = ConvertPostgresToBaseDuckType(elem_type);
    if (elem_base_type.id() == duckdb::LogicalTypeId::SQLNULL) {
      elog(WARNING, "Unsupported array element type OID: %u, using VARCHAR", elem_type);
      return duckdb::LogicalType::VARCHAR;
    }

    // DuckDB uses LIST for arrays
    int dimensions = attribute->attndims;
    if (dimensions == 0) {
      dimensions = 1; // Default to 1D array
    }

    duckdb::LogicalType list_type = elem_base_type;
    for (int i = 0; i < dimensions; i++) {
      list_type = duckdb::LogicalType::LIST(list_type);
    }
    return list_type;
  }

  // Not an array — try direct type conversion
  auto base_type = ConvertPostgresToBaseDuckType(attribute->atttypid);
  if (base_type.id() == duckdb::LogicalTypeId::SQLNULL) {
    elog(WARNING, "Unsupported PostgreSQL type OID: %u, using VARCHAR", attribute->atttypid);
    return duckdb::LogicalType::VARCHAR;
  }

  return base_type;
}

//------------------------------------------------------------------------------
// Value conversion - PostgreSQL Datum to DuckDB Vector
//------------------------------------------------------------------------------

void ConvertPostgresToDuckValue(Oid attr_type, Datum value, duckdb::Vector &result, uint64_t offset) {
  switch (attr_type) {
  case BOOLOID:
    duckdb::FlatVector::GetData<bool>(result)[offset] = DatumGetBool(value);
    break;

  case INT2OID:
    duckdb::FlatVector::GetData<int16_t>(result)[offset] = DatumGetInt16(value);
    break;

  case INT4OID:
    duckdb::FlatVector::GetData<int32_t>(result)[offset] = DatumGetInt32(value);
    break;

  case INT8OID:
    duckdb::FlatVector::GetData<int64_t>(result)[offset] = DatumGetInt64(value);
    break;

  case FLOAT4OID:
    duckdb::FlatVector::GetData<float>(result)[offset] = DatumGetFloat4(value);
    break;

  case FLOAT8OID:
    duckdb::FlatVector::GetData<double>(result)[offset] = DatumGetFloat8(value);
    break;

  case NUMERICOID: {
    // Convert numeric to double
    duckdb::FlatVector::GetData<double>(result)[offset] =
        DatumGetFloat8(DirectFunctionCall1(numeric_float8, value));
    break;
  }

  case TEXTOID:
  case VARCHAROID:
  case BPCHAROID:
  case NAMEOID: {
    text *txt = DatumGetTextP(value);
    char *str = VARDATA_ANY(txt);
    size_t len = VARSIZE_ANY_EXHDR(txt);
    duckdb::string_t duck_str(str, len);
    duckdb::FlatVector::GetData<duckdb::string_t>(result)[offset] =
        duckdb::StringVector::AddString(result, duck_str);
    break;
  }

  case DATEOID: {
    DateADT pg_date = DatumGetDateADT(value);
    // PostgreSQL dates are days since 2000-01-01
    // DuckDB dates are days since 1970-01-01
    duckdb::FlatVector::GetData<duckdb::date_t>(result)[offset] =
        duckdb::date_t(pg_date + DUCK_DATE_OFFSET);
    break;
  }

  case TIMESTAMPOID: {
    Timestamp pg_ts = DatumGetTimestamp(value);
    // PostgreSQL timestamps are microseconds since 2000-01-01
    // DuckDB timestamps are microseconds since 1970-01-01
    duckdb::FlatVector::GetData<duckdb::timestamp_t>(result)[offset] =
        duckdb::timestamp_t(pg_ts + DUCK_TIMESTAMP_OFFSET);
    break;
  }

  case TIMESTAMPTZOID: {
    TimestampTz pg_ts = DatumGetTimestampTz(value);
    duckdb::FlatVector::GetData<duckdb::timestamp_t>(result)[offset] =
        duckdb::timestamp_t(pg_ts + DUCK_TIMESTAMP_OFFSET);
    break;
  }

  case UUIDOID: {
    pg_uuid_t *pg_uuid = DatumGetUUIDP(value);
    duckdb::hugeint_t uuid_value;
    // Copy UUID bytes
    memcpy(&uuid_value, pg_uuid->data, 16);
    duckdb::FlatVector::GetData<duckdb::hugeint_t>(result)[offset] = uuid_value;
    break;
  }

  case JSONOID:
  case JSONBOID: {
    // Convert JSONB to string
    // jsonb_out returns cstring
    Datum json_datum = DirectFunctionCall1(jsonb_out, value);
    char *json_str = DatumGetCString(json_datum);
    duckdb::string_t duck_str(json_str, strlen(json_str));
    duckdb::FlatVector::GetData<duckdb::string_t>(result)[offset] =
        duckdb::StringVector::AddString(result, duck_str);
    pfree(json_str);
    break;
  }

  default: {
    // Unsupported type - convert to string representation
    Oid typoutput;
    bool typisvarlena;
    getTypeOutputInfo(attr_type, &typoutput, &typisvarlena);
    char *str = OidOutputFunctionCall(typoutput, value);
    duckdb::string_t duck_str(str, strlen(str));
    duckdb::FlatVector::GetData<duckdb::string_t>(result)[offset] =
        duckdb::StringVector::AddString(result, duck_str);
    pfree(str);
    elog(WARNING, "Converting unsupported type OID %u to string", attr_type);
    break;
  }
  }
}

} // namespace pgducklake
