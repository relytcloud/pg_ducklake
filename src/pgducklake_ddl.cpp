/*
 * pgducklake_ddl.cpp — Event-trigger based DuckLake DDL synchronization.
 *
 * Captures PostgreSQL DDL and executes equivalent DuckDB statements via
 * duckdb.raw_query() for DuckLake-managed relations.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"
#include "pgducklake/utility/cpp_wrapper.hpp"

#include <duckdb/common/string_util.hpp>
#include <duckdb/parser/keyword_helper.hpp>

extern "C" {
#include "postgres.h"

#include "commands/event_trigger.h"
#include "commands/extension.h" // creating_extension
#include "executor/spi.h"
#include "fmgr.h"
#include "nodes/value.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
}

/*
 * Look up the OID of duckdb.raw_query(text), cached per backend.
 */
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

/*
 * Call duckdb.raw_query()
 */
static inline void DuckdbRawQuery(const char *query) {
  OidFunctionCall1(GetRawQueryFuncOid(), CStringGetTextDatum(query));
}

/*
 * Execute a DuckDB query via pg_duckdb's duckdb.raw_query() UDF.
 * Ensures the DuckLake catalog is attached first.
 *
 * Returns 0 on success, 1 on error.
 * On error, sets *errmsg_out to the error message (if non-null).
 */
extern "C" int ExecuteDuckDBQuery(const char *query, const char **errmsg_out) {
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
  // force creating DuckDB instance
  ExecuteDuckDBQuery("SELECT 1", NULL);

  PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_create_table_trigger) {
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
  int result = ExecuteDuckDBQuery(create_table_ddl.c_str(), &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to create DuckLake table: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  // Handle CREATE TABLE AS (CTAS) - populate data via DuckDB
  if (IsA(parsetree, CreateTableAsStmt) && !pg_ducklake::ctas_skip_data) {
    auto ctas_stmt = castNode(CreateTableAsStmt, parsetree);
    auto ctas_query = (Query *)ctas_stmt->query;
    const char *ctas_query_string = pgduckdb_get_querydef(ctas_query);
    std::string insert_string = std::string("INSERT INTO ") +
                                pgduckdb_relation_name(relid) + " " +
                                ctas_query_string;

    elog(DEBUG1, "CTAS data population: %s", insert_string.c_str());

    const char *insert_error_msg = nullptr;
    int insert_result =
        ExecuteDuckDBQuery(insert_string.c_str(), &insert_error_msg);
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
    int result = ExecuteDuckDBQuery(drop_ddl.c_str(), &error_msg);
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

/*
 * ducklake_cleanup(older_than) - Clean up old files in the DuckLake database.
 *
 * Parameters:
 *   older_than - PostgreSQL interval (e.g., '24 hours'::interval, '7
 * days'::interval). If NULL, all scheduled files will be cleaned up.
 *
 * Returns the number of files cleaned up.
 */
DECLARE_PG_FUNCTION(ducklake_cleanup_old_files) {
  std::string query;

  if (PG_ARGISNULL(0)) {
    query = "SELECT count(*) FROM "
            "ducklake_cleanup_old_files(" PGDUCKLAKE_DUCKDB_CATALOG_QUOTED ", "
            "cleanup_all => true)";
  } else {
    Interval *interval = PG_GETARG_INTERVAL_P(0);
    char *interval_str = DatumGetCString(
        DirectFunctionCall1(interval_out, IntervalPGetDatum(interval)));

    query = duckdb::StringUtil::Format(
        "SELECT count(*) FROM "
        "ducklake_cleanup_old_files(" PGDUCKLAKE_DUCKDB_CATALOG_QUOTED ", "
        "older_than => now() - INTERVAL '%s')",
        interval_str);
  }

  const char *error_msg = nullptr;
  int result = ExecuteDuckDBQuery(query.c_str(), &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to clean up old files: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  /* TODO: parse result count from DuckDB query result */
  PG_RETURN_INT64(0);
}

DECLARE_PG_FUNCTION(ducklake_flush_inlined_data) {
  std::string query =
      "CALL ducklake_flush_inlined_data(" PGDUCKLAKE_DUCKDB_CATALOG_QUOTED;

  if (PG_NARGS() > 0 && !PG_ARGISNULL(0)) {
    Oid relid = PG_GETARG_OID(0);
    if (!OidIsValid(relid)) {
      elog(ERROR, "invalid table OID");
    }

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
    query +=
        ", schema_name => " + duckdb::KeywordHelper::WriteQuoted(schema_name);
  }

  query += ")";

  const char *error_msg = nullptr;
  int result = ExecuteDuckDBQuery(query.c_str(), &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to flush inlined data: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  PG_RETURN_VOID();
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
  int result = ExecuteDuckDBQuery(query.c_str(), &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to set DuckLake option: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_alter_table_trigger) {
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
  int result = ExecuteDuckDBQuery(ddl_str, &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to alter DuckLake table: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  PG_RETURN_NULL();
}
}
