#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "pgddb/pg/declarations.hpp"

#include "pgddb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgddb {

struct PostgresScanGlobalState;
struct PostgresScanLocalState;

// DuckDB has date starting from 1/1/1970 while PG starts from 1/1/2000
constexpr int32_t PGDUCKDB_DUCK_DATE_OFFSET = 10957;
constexpr int64_t PGDUCKDB_DUCK_TIMESTAMP_OFFSET =
    static_cast<int64_t>(PGDUCKDB_DUCK_DATE_OFFSET) * static_cast<int64_t>(86400000000) /* USECS_PER_DAY */;

// Check from regress/sql/date.sql
#define PG_MINYEAR  (-4713)
#define PG_MINMONTH (11)
#define PG_MINDAY   (24)
#define PG_MAXYEAR  (5874897)
#define PG_MAXMONTH (12)
#define PG_MAXDAY   (31)

const duckdb::date_t PGDUCKDB_PG_MIN_DATE_VALUE = duckdb::Date::FromDate(PG_MINYEAR, PG_MINMONTH, PG_MINDAY);
const duckdb::date_t PGDUCKDB_PG_MAX_DATE_VALUE = duckdb::Date::FromDate(PG_MAXYEAR, PG_MAXMONTH, PG_MAXDAY);

// Check ValidTimestampOrTimestampTz() for the logic, These values are counted from 1/1/1970
constexpr int64_t PGDUCKDB_MAX_TIMESTAMP_VALUE = 9223371244800000000;
constexpr int64_t PGDUCKDB_MIN_TIMESTAMP_VALUE = -210866803200000000;

// Type conversion between Postgres and DuckDB; the kernel implements all built-in PG types.
void CheckForUnsupportedPostgresType(duckdb::LogicalType type);
duckdb::LogicalType ConvertPostgresToDuckColumnType(Form_pg_attribute &attribute);
Oid GetPostgresDuckDBType(const duckdb::LogicalType &type, bool throw_error = false);
int32_t GetPostgresDuckDBTypemod(const duckdb::LogicalType &type);
duckdb::Value ConvertPostgresParameterToDuckValue(Datum value, Oid postgres_type);
void ConvertPostgresToDuckValue(Oid attr_type, Datum value, duckdb::Vector &result, uint64_t offset);
bool ConvertDuckToPostgresValue(TupleTableSlot *slot, duckdb::Value &value, uint64_t col);
void InsertTuplesIntoChunk(duckdb::DataChunk &output, PostgresScanLocalState &scan_local_state, TupleTableSlot **slots,
                           int num_slots);

// Detoast + convert a PG numeric Datum to its DuckDB/Arrow DECIMAL scaled-integer
// representation, written little-endian into `out` (width_bytes is 4, 8, or 16). The
// value's own scale must equal the column scale (PG enforces this for typmod'd
// columns), matching the DuckDB/Arrow decimal scale.
void NumericToDecimalBytes(Datum value, int width_bytes, void *out);

// Public helpers exposed for consumer-side hook implementations.
bool IsNestedType(duckdb::LogicalTypeId type_id);
const duckdb::LogicalType &GetChildType(const duckdb::LogicalType &type);
Datum ConvertToStringDatum(const duckdb::Value &value);

// Per-column converter, picked once per column (by attr Oid + result type) so the type
// switch happens once per batch rather than per cell.
using PostgresToDuckValueFn = void (*)(duckdb::Vector &result, Datum value, duckdb::idx_t offset);

PostgresToDuckValueFn GetPostgresToDuckValueFn(Oid attr_type, duckdb::Vector &result);

// Type extension hooks. The kernel handles built-in PG types first, then tries each
// registered hook in registration order until one returns true; consumers register via
// the Register_* functions in _PG_init. Register_* are exported (extern "C", default
// visibility) so a future shared libpgddb can collect hooks from independently-linked
// extensions instead of each bundling its own copy.

// PG Oid -> DuckDB base (non-array) type. Fill out and return true if handled.
typedef bool (*ConvertPostgresToBaseDuckColumnType_hook_t)(Oid pg_oid, duckdb::LogicalType &out);
extern "C" __attribute__((visibility("default"))) void
Register_ConvertPostgresToBaseDuckColumnType(ConvertPostgresToBaseDuckColumnType_hook_t fn);

// DuckDB LogicalType -> PG Oid (scalar form). Fill out and return true if handled.
typedef bool (*GetPostgresDuckDBType_hook_t)(const duckdb::LogicalType &type, Oid &out);
extern "C" __attribute__((visibility("default"))) void Register_GetPostgresDuckDBType(GetPostgresDuckDBType_hook_t fn);

// DuckDB element LogicalType -> PG array Oid. Fill out and return true if handled.
typedef bool (*GetPostgresArrayDuckDBType_hook_t)(const duckdb::LogicalType &type, Oid &out);
extern "C" __attribute__((visibility("default"))) void
Register_GetPostgresArrayDuckDBType(GetPostgresArrayDuckDBType_hook_t fn);

// DuckDB Value -> PG Datum stored into slot->tts_values[col]. Return true if handled.
typedef bool (*ConvertDuckToPostgresValue_hook_t)(Oid pg_oid, duckdb::Value &value, TupleTableSlot *slot, uint64_t col);
extern "C" __attribute__((visibility("default"))) void
Register_ConvertDuckToPostgresValue(ConvertDuckToPostgresValue_hook_t fn);

// PG attr Oid + result vector -> the per-column converter for that column. Set out and
// return true if handled (the kernel calls out() for every row of the column).
typedef bool (*GetPostgresToDuckValueFn_hook_t)(Oid attr_type, duckdb::Vector &result, PostgresToDuckValueFn &out);
extern "C" __attribute__((visibility("default"))) void
Register_GetPostgresToDuckValueFn(GetPostgresToDuckValueFn_hook_t fn);

// When true, an unsupported-precision NUMERIC maps to DOUBLE instead of being
// rejected as an UnsupportedPostgresType. Default false; per-consumer copy.
extern bool convert_unsupported_numeric_to_double;

} // namespace pgddb
