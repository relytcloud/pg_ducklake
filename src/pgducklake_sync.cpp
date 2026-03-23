/*
 * pgducklake_sync.cpp -- Reverse sync framework: DuckDB metadata -> PG catalog.
 *
 * @scope backend: syncing_from_metadata guard bool, handler registration
 * @scope duckdb-instance: ducklake_snapshot_trigger, SyncNewTables,
 *   SyncDroppedTables, DuckLakeTypeToPgType
 *
 * ducklake_snapshot_trigger fires on INSERT into ducklake.ducklake_snapshot.
 * It syncs newly created/dropped tables (always), then calls registered
 * per-object-type sync handlers for sort keys, partitions, etc.
 *
 * Per-object-type sync handlers (e.g. SyncSortKeys) are listed in the
 * static sync_handlers initializer -- add new handlers there.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_sync.hpp"

#include <duckdb/common/error_data.hpp> /* must precede postgres.h (FATAL macro) */

#include "pgducklake/pgducklake_sorted_by.hpp"

#include "pgducklake/utility/cpp_wrapper.hpp"

#include <string>
#include <vector>

#include <duckdb/common/string_util.hpp>

#include "common/ducklake_types.hpp"

extern "C" {
#include "postgres.h"

#include "catalog/namespace.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
}

namespace pgducklake {

bool syncing_from_metadata = false;

/* Per-object-type sync handlers, called after SyncNewTables/SyncDroppedTables.
 * Add new handlers here. */
static SyncHandler sync_handlers[] = {
    SyncSortKeys,
};

} // namespace pgducklake

/* ================================================================
 * Static helpers
 * ================================================================ */

namespace {

/*
 * Map DuckLake type strings to PostgreSQL type strings.
 * Reuses DuckLakeTypes::FromString() to parse the type, then maps the
 * resulting LogicalTypeId to a PG type OID and formats it canonically.
 * Falls back to "text" for unrecognized or complex types.
 */
static std::string DuckLakeTypeToPgType(const char *dl_type) {
  try {
    auto logical_type = duckdb::DuckLakeTypes::FromString(dl_type);
    Oid pg_type = InvalidOid;
    int32_t typemod = -1;

    switch (logical_type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      pg_type = BOOLOID;
      break;
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::UTINYINT:
      pg_type = INT2OID;
      break;
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::USMALLINT:
      pg_type = INT4OID;
      break;
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::UINTEGER:
      pg_type = INT8OID;
      break;
    case duckdb::LogicalTypeId::HUGEINT:
    case duckdb::LogicalTypeId::UBIGINT:
    case duckdb::LogicalTypeId::UHUGEINT:
      pg_type = NUMERICOID;
      break;
    case duckdb::LogicalTypeId::FLOAT:
      pg_type = FLOAT4OID;
      break;
    case duckdb::LogicalTypeId::DOUBLE:
      pg_type = FLOAT8OID;
      break;
    case duckdb::LogicalTypeId::DECIMAL: {
      pg_type = NUMERICOID;
      uint8_t width, scale;
      logical_type.GetDecimalProperties(width, scale);
      typemod = ((width << 16) | (scale & 0x7ff)) + VARHDRSZ;
      break;
    }
    case duckdb::LogicalTypeId::VARCHAR:
      pg_type = TEXTOID;
      break;
    case duckdb::LogicalTypeId::BLOB:
      pg_type = BYTEAOID;
      break;
    case duckdb::LogicalTypeId::UUID:
      pg_type = UUIDOID;
      break;
    case duckdb::LogicalTypeId::DATE:
      pg_type = DATEOID;
      break;
    case duckdb::LogicalTypeId::TIME:
      pg_type = TIMEOID;
      break;
    case duckdb::LogicalTypeId::TIME_TZ:
      pg_type = TIMETZOID;
      break;
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_MS:
    case duckdb::LogicalTypeId::TIMESTAMP_NS:
    case duckdb::LogicalTypeId::TIMESTAMP_SEC:
      pg_type = TIMESTAMPOID;
      break;
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      pg_type = TIMESTAMPTZOID;
      break;
    case duckdb::LogicalTypeId::INTERVAL:
      pg_type = INTERVALOID;
      break;
    case duckdb::LogicalTypeId::VARIANT:
      return "ducklake.variant";
    default:
      return "text";
    }

    return format_type_with_typemod(pg_type, typemod);
  } catch (...) {
    return "text";
  }
}

/*
 * Sync newly created tables from DuckLake metadata into pg_class.
 * Queries ducklake_table/ducklake_column for tables with begin_snapshot = sid
 * and emits CREATE TABLE ... USING ducklake for each one not already present.
 * Caller must have an active SPI connection.
 */
static void SyncNewTables(const char *sid) {
  std::string query = duckdb::StringUtil::Format(R"(
		SELECT s.schema_name, t.table_name,
		       c.column_name, c.column_type, c.nulls_allowed
		FROM ducklake.ducklake_table t
		JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
		LEFT JOIN ducklake.ducklake_column c ON t.table_id = c.table_id
		  AND c.parent_column IS NULL
		  AND c.end_snapshot IS NULL
		WHERE t.begin_snapshot = %s
		  AND s.end_snapshot IS NULL
		ORDER BY t.table_id, c.column_order
		)",
                                                 sid);

  int ret = SPI_exec(query.c_str(), 0);
  if (ret != SPI_OK_SELECT)
    elog(ERROR, "SPI_exec failed: %s", SPI_result_code_string(ret));

  /* Save results -- SPI_exec invalidates previous tuptable */
  struct ColInfo {
    std::string schema_name, table_name, col_name, col_type;
    bool not_null, has_col;
  };
  std::vector<ColInfo> cols;
  for (uint64_t i = 0; i < SPI_processed; ++i) {
    HeapTuple tup = SPI_tuptable->vals[i];
    TupleDesc td = SPI_tuptable->tupdesc;
    ColInfo ci;
    char *v;
    v = SPI_getvalue(tup, td, 1);
    ci.schema_name = v ? v : "";
    v = SPI_getvalue(tup, td, 2);
    ci.table_name = v ? v : "";
    v = SPI_getvalue(tup, td, 3);
    ci.has_col = (v != nullptr);
    ci.col_name = v ? v : "";
    v = SPI_getvalue(tup, td, 4);
    ci.col_type = v ? v : "";
    v = SPI_getvalue(tup, td, 5);
    ci.not_null = (v && strcmp(v, "f") == 0);
    cols.push_back(std::move(ci));
  }

  /* Group by table and emit CREATE TABLE DDL */
  std::string prev_schema, prev_table, ddl;
  bool first_col = true;
  bool skip_table = false;

  auto emit_ddl = [&]() {
    if (!ddl.empty() && !skip_table) {
      ddl += ") USING ducklake";
      elog(DEBUG1, "Metadata sync: %s", ddl.c_str());
      ret = SPI_exec(ddl.c_str(), 0);
      if (ret != SPI_OK_UTILITY)
        elog(ERROR, "SPI_exec CREATE TABLE failed: %s",
             SPI_result_code_string(ret));
    }
    ddl.clear();
    skip_table = false;
  };

  for (auto &ci : cols) {
    if (ci.schema_name != prev_schema || ci.table_name != prev_table) {
      emit_ddl();

      /* Skip if table already exists in pg_class */
      Oid nsp_oid = get_namespace_oid(ci.schema_name.c_str(), true);
      if (OidIsValid(nsp_oid) &&
          OidIsValid(get_relname_relid(ci.table_name.c_str(), nsp_oid))) {
        skip_table = true;
        prev_schema = ci.schema_name;
        prev_table = ci.table_name;
        continue;
      }

      /* Create schema if it doesn't exist yet */
      if (ci.schema_name != "public") {
        std::string cs = "CREATE SCHEMA IF NOT EXISTS ";
        cs += quote_identifier(ci.schema_name.c_str());
        ret = SPI_exec(cs.c_str(), 0);
        if (ret != SPI_OK_UTILITY)
          elog(ERROR, "SPI_exec CREATE SCHEMA failed: %s",
               SPI_result_code_string(ret));
      }

      ddl = "CREATE TABLE ";
      ddl += quote_identifier(ci.schema_name.c_str());
      ddl += ".";
      ddl += quote_identifier(ci.table_name.c_str());
      ddl += " (";
      first_col = true;
      prev_schema = ci.schema_name;
      prev_table = ci.table_name;
    }

    if (skip_table || !ci.has_col)
      continue;

    if (!first_col)
      ddl += ", ";
    ddl += quote_identifier(ci.col_name.c_str());
    ddl += " ";
    ddl += DuckLakeTypeToPgType(ci.col_type.c_str());
    if (ci.not_null)
      ddl += " NOT NULL";
    first_col = false;
  }
  emit_ddl();
}

/*
 * Sync dropped tables from DuckLake metadata: drop pg_class entries for
 * tables whose end_snapshot = sid.
 * Caller must have an active SPI connection.
 */
static void SyncDroppedTables(const char *sid) {
  std::string query = duckdb::StringUtil::Format(R"(
		SELECT s.schema_name, t.table_name
		FROM ducklake.ducklake_table t
		JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
		WHERE t.end_snapshot = %s
		)",
                                                 sid);

  int ret = SPI_exec(query.c_str(), 0);
  if (ret != SPI_OK_SELECT)
    elog(ERROR, "SPI_exec failed: %s", SPI_result_code_string(ret));

  struct DropInfo {
    std::string schema_name, table_name;
  };
  std::vector<DropInfo> drops;
  for (uint64_t i = 0; i < SPI_processed; ++i) {
    HeapTuple tup = SPI_tuptable->vals[i];
    TupleDesc td = SPI_tuptable->tupdesc;
    DropInfo di;
    char *v;
    v = SPI_getvalue(tup, td, 1);
    di.schema_name = v ? v : "";
    v = SPI_getvalue(tup, td, 2);
    di.table_name = v ? v : "";
    drops.push_back(std::move(di));
  }

  for (auto &di : drops) {
    Oid nsp_oid = get_namespace_oid(di.schema_name.c_str(), true);
    if (!OidIsValid(nsp_oid))
      continue;
    if (!OidIsValid(get_relname_relid(di.table_name.c_str(), nsp_oid)))
      continue;

    std::string drop_ddl = "DROP TABLE ";
    drop_ddl += quote_identifier(di.schema_name.c_str());
    drop_ddl += ".";
    drop_ddl += quote_identifier(di.table_name.c_str());
    elog(DEBUG1, "Metadata sync: %s", drop_ddl.c_str());
    ret = SPI_exec(drop_ddl.c_str(), 0);
    if (ret != SPI_OK_UTILITY)
      elog(ERROR, "SPI_exec DROP TABLE failed: %s",
           SPI_result_code_string(ret));
  }
}

} // anonymous namespace

/* ================================================================
 * Snapshot trigger
 * ================================================================ */

extern "C" {

/*
 * ducklake_snapshot_trigger -- AFTER INSERT trigger on ducklake.ducklake_snapshot.
 *
 * Detects tables created/dropped by external DuckDB clients (which write
 * directly to the ducklake metadata tables) and creates/drops corresponding
 * pg_class entries so the tables become visible from PostgreSQL.
 * After table sync, calls registered per-object-type sync handlers.
 */
DECLARE_PG_FUNCTION(ducklake_snapshot_trigger) {
  if (!CALLED_AS_TRIGGER(fcinfo))
    elog(ERROR, "not fired by trigger manager");

  TriggerData *trigdata = (TriggerData *)fcinfo->context;

  /* Extract snapshot_id from the NEW row */
  bool isnull;
  int64 snapshot_id = DatumGetInt64(SPI_getbinval(
      trigdata->tg_trigtuple, trigdata->tg_relation->rd_att, 1, &isnull));
  if (isnull)
    elog(ERROR, "snapshot_id is NULL");

  SPI_connect();

  auto save_nestlevel = NewGUCNestLevel();
  SetConfigOption("duckdb.force_execution", "false", PGC_USERSET,
                  PGC_S_SESSION);

  pgducklake::syncing_from_metadata = true;

  PG_TRY();
  {
    std::string sid = std::to_string(snapshot_id);
    SyncNewTables(sid.c_str());
    SyncDroppedTables(sid.c_str());

    for (auto handler : pgducklake::sync_handlers)
      handler(sid.c_str());
  }
  PG_FINALLY();
  {
    pgducklake::syncing_from_metadata = false;
  }
  PG_END_TRY();

  AtEOXact_GUC(false, save_nestlevel);
  SPI_finish();
  return PointerGetDatum(trigdata->tg_trigtuple);
}
}
