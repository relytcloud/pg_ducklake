/*
 * pgducklake_ddl.cpp — DDL synchronization between PostgreSQL and DuckLake.
 *
 * Forward (PG→DuckDB): event triggers capture PostgreSQL DDL and replay it
 * in DuckDB via duckdb.raw_query().
 *
 * Reverse (DuckDB→PG): an AFTER INSERT trigger on ducklake_snapshot detects
 * tables created/dropped by external DuckDB clients and creates/drops
 * corresponding pg_class entries so they become visible from PostgreSQL.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"
#include "pgducklake/utility/cpp_wrapper.hpp"

#include <string>
#include <vector>

#include <duckdb/common/string_util.hpp>
#include <duckdb/parser/keyword_helper.hpp>

#include "common/ducklake_types.hpp"

extern "C" {
#include "postgres.h"

#include "access/relation.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/extension.h" // creating_extension
#include "commands/trigger.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "nodes/value.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
}

/* Guard to prevent circular triggers during metadata→PG catalog sync */
static bool syncing_from_metadata = false;

namespace pgducklake {

static Oid GetRawQueryFuncOid() {
  static Oid cached = InvalidOid;
  if (!OidIsValid(cached)) {
    List *funcname = list_make2(makeString(pstrdup("duckdb")),
                                makeString(pstrdup("raw_query")));
    Oid argtypes[] = {TEXTOID};
    cached = LookupFuncName(funcname, 1, argtypes, false);
    list_free(funcname);
  }
  return cached;
}

static void DuckdbRawQuery(const char *query) {
  OidFunctionCall1(GetRawQueryFuncOid(), CStringGetTextDatum(query));
}

/*
 * Execute a DuckDB query via pg_duckdb's duckdb.raw_query() UDF.
 * Ensures the DuckLake catalog is attached first.
 *
 * Returns 0 on success, 1 on error.
 * On error, sets *errmsg_out to the error message (if non-null).
 */
int ExecuteDuckDBQuery(const char *query, const char **errmsg_out) {
  static thread_local std::string last_error;

  // Volatile to survive PG_CATCH longjmp
  volatile int result = 0;
  MemoryContext saved_context = CurrentMemoryContext;

  // Suppress NOTICE messages from duckdb.raw_query() which unconditionally
  // emits "result: ..." via elog(NOTICE).
  //
  // FIXME: should we modify pg_duckdb?
  auto save_nestlevel = NewGUCNestLevel();
  SetConfigOption("client_min_messages", "warning", PGC_USERSET, PGC_S_SESSION);

  PG_TRY();
  {
    DuckdbRawQuery(query);
  }
  PG_CATCH();
  {
    MemoryContextSwitchTo(saved_context);
    ErrorData *edata = CopyErrorData();
    FlushErrorState();

    last_error = edata->message ? edata->message : "unknown error";
    FreeErrorData(edata);

    if (errmsg_out)
      *errmsg_out = last_error.c_str();
    result = 1;
  }
  PG_END_TRY();

  AtEOXact_GUC(false, save_nestlevel);

  return result;
}

} // namespace pgducklake

extern "C" {

DECLARE_PG_FUNCTION(ducklake_initialize) {
  elog(LOG, "ducklake_initialize() called");

  if (!creating_extension) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("ducklake_initialize() can only be called during "
                           "CREATE EXTENSION")));
  }

  if (pgducklake::PgDuckLakeMetadataManager::IsInitialized()) {
    ereport(
        ERROR,
        (errcode(ERRCODE_DUPLICATE_SCHEMA),
         errmsg("DuckLake reserved schema \"ducklake\" is already in use")));
  }

  // Force DuckDB initialization (no-op if already alive).
  // On first CREATE: this triggers DuckDBManager::Initialize() which
  //   calls ducklake_load_extension() -> ducklake_attach_catalog().
  // On DROP+CREATE: DuckDB is already alive, the catalog was detached
  //   by the utility hook during DROP, so we re-attach it here.
  bool duckdb_already_initialized = (ducklake_get_duckdb_database() != nullptr);

  const char *init_errmsg = nullptr;
  int ret = pgducklake::ExecuteDuckDBQuery("SELECT 1", &init_errmsg);
  if (ret != 0) {
    ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("failed to initialize DuckDB: %s",
                    init_errmsg ? init_errmsg : "unknown error")));
  }

  if (duckdb_already_initialized) {
    ducklake_attach_catalog();
  }

  PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_create_table_trigger) {
  if (syncing_from_metadata)
    PG_RETURN_NULL();

  if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
    elog(ERROR, "not fired by event trigger manager");

  EventTriggerData *trigger_data = (EventTriggerData *)fcinfo->context;
  Node *parsetree = trigger_data->parsetree;

  SPI_connect();

  auto save_nestlevel = NewGUCNestLevel();
  SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET,
                  PGC_S_SESSION);
  SetConfigOption("duckdb.force_execution", "false", PGC_USERSET,
                  PGC_S_SESSION);

  int ret = SPI_exec(R"(
		SELECT DISTINCT objid AS relid, pg_class.relpersistence = 't' AS is_temporary
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN pg_catalog.pg_class
		ON cmds.objid = pg_class.oid
		WHERE cmds.object_type = 'table'
		AND pg_class.relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake')
		)",
                     0);

  if (ret != SPI_OK_SELECT)
    elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

  /* if we selected a row it was a duckdb table */
  auto is_ducklake_table = SPI_processed > 0;
  if (!is_ducklake_table) {
    /* No DuckDB tables were created so we don't need to do anything */
    AtEOXact_GUC(false, save_nestlevel);
    SPI_finish();
    PG_RETURN_NULL();
  }

  if (SPI_processed != 1) {
    elog(ERROR, "Expected single table to be created, but found %llu",
         static_cast<unsigned long long>(SPI_processed));
  }

  if (!IsA(parsetree, CreateStmt) && !IsA(parsetree, CreateTableAsStmt)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                    errmsg("Cannot create a DuckLake table this way, use "
                           "CREATE TABLE or CREATE TABLE AS")));
  }

  HeapTuple tuple = SPI_tuptable->vals[0];
  bool isnull;
  Datum relid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
  if (isnull) {
    elog(ERROR, "Expected relid to be returned, but found NULL");
  }

  Datum is_temporary_datum =
      SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &isnull);
  if (isnull) {
    elog(ERROR, "Expected temporary boolean to be returned, but found NULL");
  }

  Oid relid = DatumGetObjectId(relid_datum);
  bool is_temporary = DatumGetBool(is_temporary_datum);

  if (is_temporary) {
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("temporary tables are not supported with ducklake "
                           "access method")));
  }

  AtEOXact_GUC(false, save_nestlevel);
  SPI_finish();

  // Generate CREATE TABLE DDL for DuckDB
  std::string create_table_ddl(pgduckdb_get_tabledef(relid));
  elog(DEBUG1, "Creating DuckLake table: %s", create_table_ddl.c_str());

  // Execute CREATE TABLE in DuckDB via raw_query
  const char *error_msg = nullptr;
  int result =
      pgducklake::ExecuteDuckDBQuery(create_table_ddl.c_str(), &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to create DuckLake table: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  // Handle CREATE TABLE AS (CTAS) - populate data via DuckDB
  if (IsA(parsetree, CreateTableAsStmt) && !pgducklake::ctas_skip_data) {
    auto ctas_stmt = castNode(CreateTableAsStmt, parsetree);
    auto ctas_query = (Query *)ctas_stmt->query;
    const char *ctas_query_string = pgduckdb_get_querydef(ctas_query);
    std::string insert_string = std::string("INSERT INTO ") +
                                pgduckdb_relation_name(relid) + " " +
                                ctas_query_string;

    elog(DEBUG1, "CTAS data population: %s", insert_string.c_str());

    const char *insert_error_msg = nullptr;
    int insert_result = pgducklake::ExecuteDuckDBQuery(insert_string.c_str(),
                                                       &insert_error_msg);
    if (insert_result != 0) {
      ereport(ERROR,
              (errcode(ERRCODE_INTERNAL_ERROR),
               errmsg("failed to populate DuckLake table via CTAS: %s",
                      insert_error_msg ? insert_error_msg : "unknown error")));
    }
  }

  PG_RETURN_NULL();
}

DECLARE_PG_FUNCTION(ducklake_drop_trigger) {
  if (syncing_from_metadata)
    PG_RETURN_NULL();

  if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
    elog(ERROR, "not fired by event trigger manager");

  SPI_connect();

  auto save_nestlevel = NewGUCNestLevel();
  SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET,
                  PGC_S_SESSION);

  // Check if any tables were dropped
  int ret = SPI_exec(R"(
		SELECT 1
		FROM pg_catalog.pg_event_trigger_dropped_objects()
		WHERE object_type = 'table'
	)",
                     0);

  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));
  }

  if (SPI_processed == 0) {
    AtEOXact_GUC(false, save_nestlevel);
    SPI_finish();
    PG_RETURN_NULL();
  }

  /*
   * Query DuckLake metadata to find tables that need to be dropped.
   * We can't use pg_class here since the tables are already dropped.
   */
  ret = SPI_exec(R"(
		SELECT cmds.schema_name, cmds.object_name
		FROM pg_catalog.pg_event_trigger_dropped_objects() cmds
		JOIN ducklake.ducklake_table AS tbl
		  ON cmds.object_name = tbl.table_name
		JOIN ducklake.ducklake_schema AS schema
		  ON cmds.schema_name = schema.schema_name
		  AND tbl.schema_id = schema.schema_id
		WHERE cmds.object_type = 'table'
		  AND tbl.end_snapshot IS NULL
		  AND schema.end_snapshot IS NULL
		)",
                 0);

  if (ret != SPI_OK_SELECT) {
    elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));
  }

  // Drop corresponding DuckDB tables
  for (uint64_t proc = 0; proc < SPI_processed; ++proc) {
    HeapTuple tuple = SPI_tuptable->vals[proc];

    char *schema_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
    char *table_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2);

    std::string drop_ddl = duckdb::StringUtil::Format(
        "DROP TABLE IF EXISTS " PGDUCKLAKE_DUCKDB_CATALOG ".%s.%s", schema_name,
        table_name);

    elog(DEBUG1, "Dropping DuckLake table: %s", drop_ddl.c_str());

    const char *error_msg = nullptr;
    int result = pgducklake::ExecuteDuckDBQuery(drop_ddl.c_str(), &error_msg);
    if (result != 0) {
      // Log warning but don't fail - table might already be gone
      elog(WARNING, "failed to drop DuckLake table %s.%s: %s", schema_name,
           table_name, error_msg ? error_msg : "unknown error");
    }
  }

  AtEOXact_GUC(false, save_nestlevel);
  SPI_finish();

  PG_RETURN_NULL();
}


DECLARE_PG_FUNCTION(ducklake_set_option) {
  if (PG_ARGISNULL(0)) {
    elog(ERROR, "Option name cannot be NULL");
  }

  char *option_name = text_to_cstring(PG_GETARG_TEXT_PP(0));

  if (pg_strcasecmp(option_name, "data_inlining_row_limit") == 0) {
    // supported
  } else {
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("invalid option name \"%s\"", option_name),
             errdetail(
                 "Only \"data_inlining_row_limit\" is supported currently.")));
  }

  // Handle value argument which is VARIADIC "any"
  // We need to convert the datum to a string representation for the SQL query
  Oid value_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
  Datum value_datum = PG_GETARG_DATUM(1);
  bool value_isnull = PG_ARGISNULL(1);

  std::string value_str;
  if (value_isnull) {
    value_str = "NULL";
  } else {
    Oid typoutput;
    bool typisvarlena;
    getTypeOutputInfo(value_type, &typoutput, &typisvarlena);
    char *val_str = OidOutputFunctionCall(typoutput, value_datum);

    // For numeric/boolean types, we don't want to quote them if we can avoid
    // it, but since we are constructing a SQL string, passing them as string
    // literals is usually safest and DuckDB is good at casting strings to
    // appropriate types. However, for ANY parameter, DuckDB might treat '50' as
    // STRING and 50 as INTEGER. Let's try to be smart based on OID.
    switch (value_type) {
    case BOOLOID:
    case INT2OID:
    case INT4OID:
    case INT8OID:
    case FLOAT4OID:
    case FLOAT8OID:
    case NUMERICOID:
      // These types can be passed directly without quotes (mostly)
      // But OidOutputFunctionCall returns string representation.
      // e.g. "50".
      value_str = val_str;
      break;
    default:
      // Quote strings, dates, etc.
      value_str = duckdb::KeywordHelper::WriteQuoted(val_str);
      break;
    }
    pfree(val_str);
  }

  // Use ducklake_set_option('catalog', 'option', value, ...) directly
  auto query = duckdb::StringUtil::Format(
      "CALL " PGDUCKLAKE_DUCKDB_CATALOG ".set_option(%s, %s",
      duckdb::KeywordHelper::WriteQuoted(option_name).c_str(),
      value_str.c_str());

  // Optional scope (regclass)
  if (PG_NARGS() > 2 && !PG_ARGISNULL(2)) {
    Oid relid = PG_GETARG_OID(2);

    char *table_name = get_rel_name(relid);
    if (!table_name) {
      elog(ERROR, "Could not find relation with OID %u", relid);
    }
    char *schema_name = get_namespace_name(get_rel_namespace(relid));
    if (!schema_name) {
      elog(ERROR, "Could not find namespace for relation with OID %u", relid);
    }

    query +=
        ", table_name => " + duckdb::KeywordHelper::WriteQuoted(table_name);
    query += ", schema => " + duckdb::KeywordHelper::WriteQuoted(schema_name);
  }

  query += ")";

  elog(DEBUG2, "[PGDuckDB] Executing set_option: %s", query.c_str());

  const char *error_msg = nullptr;
  int result = pgducklake::ExecuteDuckDBQuery(query.c_str(), &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to set DuckLake option: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  PG_RETURN_VOID();
}

static void EnsureDuckLakeTable(Oid relid) {
  static Oid ducklake_am_oid = InvalidOid;
  if (!OidIsValid(ducklake_am_oid))
    ducklake_am_oid = get_am_oid("ducklake", false);

  Relation rel = relation_open(relid, AccessShareLock);
  Oid am_oid = rel->rd_rel->relam;
  relation_close(rel, AccessShareLock);

  if (am_oid != ducklake_am_oid)
    ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("table \"%s\" is not a DuckLake table",
                           get_rel_name(relid))));
}

DECLARE_PG_FUNCTION(ducklake_set_partition) {
  if (PG_ARGISNULL(0))
    elog(ERROR, "table cannot be NULL");
  if (PG_ARGISNULL(1))
    elog(ERROR, "partition_by cannot be NULL");

  Oid relid = PG_GETARG_OID(0);
  EnsureDuckLakeTable(relid);

  ArrayType *arr = PG_GETARG_ARRAYTYPE_P(1);
  if (ARR_NDIM(arr) == 0)
    elog(ERROR, "partition_by cannot be empty");

  int nelems;
  Datum *elems;
  bool *nulls;
  deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT, &elems, &nulls,
                    &nelems);

  if (nelems == 0)
    elog(ERROR, "partition_by cannot be empty");

  std::string spec;
  for (int i = 0; i < nelems; i++) {
    if (nulls[i])
      elog(ERROR, "partition key cannot be NULL");
    if (i > 0)
      spec += ", ";
    spec += text_to_cstring(DatumGetTextPP(elems[i]));
  }

  std::string query = std::string("ALTER TABLE ") +
                       pgduckdb_relation_name(relid) +
                       " SET PARTITIONED BY (" + spec + ")";

  const char *error_msg = nullptr;
  int result = pgducklake::ExecuteDuckDBQuery(query.c_str(), &error_msg);
  if (result != 0)
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to set partition: %s",
                           error_msg ? error_msg : "unknown error")));

  PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_reset_partition) {
  if (PG_ARGISNULL(0))
    elog(ERROR, "table cannot be NULL");

  Oid relid = PG_GETARG_OID(0);
  EnsureDuckLakeTable(relid);

  std::string query = std::string("ALTER TABLE ") +
                       pgduckdb_relation_name(relid) +
                       " RESET PARTITIONED BY";

  const char *error_msg = nullptr;
  int result = pgducklake::ExecuteDuckDBQuery(query.c_str(), &error_msg);
  if (result != 0)
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to reset partition: %s",
                           error_msg ? error_msg : "unknown error")));

  PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_alter_table_trigger) {
  if (syncing_from_metadata)
    PG_RETURN_NULL();

  if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
    elog(ERROR, "not fired by event trigger manager");

  EventTriggerData *trigger_data = (EventTriggerData *)fcinfo->context;
  Node *parsetree = trigger_data->parsetree;

  SPI_connect();

  auto save_nestlevel = NewGUCNestLevel();
  SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET,
                  PGC_S_SESSION);
  SetConfigOption("duckdb.force_execution", "false", PGC_USERSET,
                  PGC_S_SESSION);

  int ret = SPI_exec(R"(
		SELECT DISTINCT objid AS relid
		FROM pg_catalog.pg_event_trigger_ddl_commands() cmds
		JOIN pg_catalog.pg_class
		ON cmds.objid = pg_class.oid
		WHERE cmds.object_type IN ('table', 'table column')
		AND pg_class.relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake')
		)",
                     0);

  if (ret != SPI_OK_SELECT)
    elog(ERROR, "SPI_exec failed: error code %s", SPI_result_code_string(ret));

  if (SPI_processed == 0) {
    AtEOXact_GUC(false, save_nestlevel);
    SPI_finish();
    PG_RETURN_NULL();
  }

  HeapTuple tuple = SPI_tuptable->vals[0];
  bool isnull;
  Datum relid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
  if (isnull)
    elog(ERROR, "Expected relid to be returned, but found NULL");

  Oid relid = DatumGetObjectId(relid_datum);

  AtEOXact_GUC(false, save_nestlevel);
  SPI_finish();

  /* Generate DDL using pg_duckdb's ruleutils functions */
  char *ddl_str;
  if (IsA(parsetree, RenameStmt)) {
    ddl_str = pgduckdb_get_rename_relationdef(relid, (RenameStmt *)parsetree);
  } else if (IsA(parsetree, AlterTableStmt)) {
    ddl_str = pgduckdb_get_alter_tabledef(relid, (AlterTableStmt *)parsetree);
  } else {
    elog(ERROR, "Unexpected parsetree type in ALTER TABLE trigger: %d",
         nodeTag(parsetree));
  }

  elog(DEBUG1, "ALTER TABLE DDL for DuckLake: %s", ddl_str);

  const char *error_msg = nullptr;
  int result = pgducklake::ExecuteDuckDBQuery(ddl_str, &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to alter DuckLake table: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  PG_RETURN_NULL();
}

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
    default:
      return "text";
    }

    return format_type_with_typemod(pg_type, typemod);
  } catch (...) {
    return "text";
  }
}

/*
 * ducklake_snapshot_trigger — AFTER INSERT trigger on ducklake.ducklake_snapshot.
 *
 * Detects tables created/dropped by external DuckDB clients (which write
 * directly to the ducklake metadata tables) and creates/drops corresponding
 * pg_class entries so the tables become visible from PostgreSQL.
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

  syncing_from_metadata = true;

  PG_TRY();
  {
    std::string sid = std::to_string(snapshot_id);

    /* ---- Find newly created tables ---- */
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
                                                   sid.c_str());

    int ret = SPI_exec(query.c_str(), 0);
    if (ret != SPI_OK_SELECT)
      elog(ERROR, "SPI_exec failed: %s", SPI_result_code_string(ret));

    /* Save results — SPI_exec invalidates previous tuptable */
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
            OidIsValid(
                get_relname_relid(ci.table_name.c_str(), nsp_oid))) {
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

    /* ---- Find dropped tables ---- */
    query = duckdb::StringUtil::Format(R"(
		SELECT s.schema_name, t.table_name
		FROM ducklake.ducklake_table t
		JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
		WHERE t.end_snapshot = %s
		)",
                                       sid.c_str());

    ret = SPI_exec(query.c_str(), 0);
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

      std::string drop_ddl = "DROP TABLE IF EXISTS ";
      drop_ddl += quote_identifier(di.schema_name.c_str());
      drop_ddl += ".";
      drop_ddl += quote_identifier(di.table_name.c_str());
      elog(DEBUG1, "Metadata sync: %s", drop_ddl.c_str());
      ret = SPI_exec(drop_ddl.c_str(), 0);
      if (ret != SPI_OK_UTILITY)
        elog(ERROR, "SPI_exec DROP TABLE failed: %s",
             SPI_result_code_string(ret));
    }

    syncing_from_metadata = false;
  }
  PG_CATCH();
  {
    syncing_from_metadata = false;
    PG_RE_THROW();
  }
  PG_END_TRY();

  AtEOXact_GUC(false, save_nestlevel);
  SPI_finish();
  return PointerGetDatum(trigdata->tg_trigtuple);
}
}
