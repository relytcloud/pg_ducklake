#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"
#include "pgducklake/utility/cpp_wrapper.hpp"

#include <duckdb/main/database.hpp>
#include <filesystem>

extern "C" {
#include "postgres.h"

#include "access/relation.h"
#include "catalog/pg_class.h"
#include "commands/event_trigger.h"
#include "commands/extension.h" // creating_extension
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"

#include "pgducklake/pgduckdb.h"
}

namespace pgduckdb {
bool ducklake_ctas_skip_data = false;
} // namespace pgduckdb

extern "C" {

DECLARE_PG_FUNCTION(ducklake_initialize) {
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

  /* Create the data directory for DuckLake parquet files */
  auto data_path = duckdb::StringUtil::Format("%s/pg_ducklake", DataDir);
  try {
    std::filesystem::create_directory(data_path);
  } catch (const std::filesystem::filesystem_error &e) {
    ereport(ERROR,
            (errcode(ERRCODE_IO_ERROR),
             errmsg("failed to create DuckLake data directory \"%s\": %s",
                    data_path.c_str(), e.what())));
  }

  /* Initialize DuckDB and attach the DuckLake catalog */
  try {
    auto &duckdb = pgducklake::DuckLakeManager::GetDatabase();
    // pgduckdb::DuckDBQueryOrThrow(*connection,
    //                              "ATTACH 'ducklake:pgducklake:' AS pgducklake
    //                              "
    //                              "(METADATA_SCHEMA 'ducklake', DATA_PATH '" +
    //                                  data_path + "')");
    // /* Reset DuckDB instance to apply initialization settings */
    // pgduckdb::DuckDBManager::Reset();
  } catch (const std::exception &e) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to initialize DuckLake: %s", e.what())));
  }

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
    elog(ERROR, "Expected single table to be created, but found %ulld" ,
         static_cast<uint64_t>(SPI_processed));
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
    elog(ERROR, "TODO: create a DuckLake table as a temporary table is not "
                "supported yet");
  }

  // std::string create_table_string(pgducklake_get_tabledef(relid));
  std::string create_table_string;

  elog(DEBUG1, "create_table_string: %s", create_table_string.c_str());

  AtEOXact_GUC(false, save_nestlevel);
  SPI_finish();

  // auto connection = pgduckdb::DuckDBManager::GetConnection(false);

  std::string set_table_path_string;
  // auto &table_path = pgduckdb::ducklake_default_table_path;
  char *table_path;
  if (table_path == nullptr || strlen(table_path) == 0) {
    // If path is empty or null, reset the DuckDB variable
    set_table_path_string = "RESET ducklake_default_table_path";
    elog(DEBUG2,
         "[PGDuckDB] Reset DuckDB option: 'ducklake_default_table_path'");
  } else {
    set_table_path_string = "SET ducklake_default_table_path=" +
                            duckdb::KeywordHelper::WriteQuoted(table_path);
    elog(DEBUG2,
         "[PGDuckDB] Set DuckDB option: 'ducklake_default_table_path'=%s",
         table_path);
  }

  // pgduckdb::DuckDBQueryOrThrow(*connection, set_table_path_string + ";" +
  //                                               create_table_string);
  // if (IsA(parsetree, CreateTableAsStmt) &&
  // !pgduckdb::ducklake_ctas_skip_data) {
  //   auto ctas_stmt = castNode(CreateTableAsStmt, parsetree);
  //   auto ctas_query = (Query *)ctas_stmt->query;
  //   const char *ctas_query_string = pgduckdb_get_querydef(ctas_query);
  //   std::string insert_string = std::string("INSERT INTO ") +
  //                               pgduckdb_relation_name(relid) + " " +
  //                               ctas_query_string;
  //   pgduckdb::DuckDBQueryOrThrow(*connection, insert_string);
  // }

  PG_RETURN_NULL();
}

DECLARE_PG_FUNCTION(ducklake_drop_trigger) {
  if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
    elog(ERROR, "not fired by event trigger manager");

  SPI_connect();

  auto save_nestlevel = NewGUCNestLevel();
  SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET,
                  PGC_S_SESSION);
  SetConfigOption("duckdb.force_execution", "false", PGC_USERSET,
                  PGC_S_SESSION);

  // We only care about ducklake table drop
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
   * We cannot see dropped objects in pg_class at this point,
   * so we directly query ducklake metadata.
   * Could be buggy.
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

  // auto connection = pgduckdb::DuckDBManager::GetConnection(true);

  for (uint64_t proc = 0; proc < SPI_processed; ++proc) {
    HeapTuple tuple = SPI_tuptable->vals[proc];

    char *schema_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
    char *table_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2);

    char *drop_query =
        psprintf("DROP TABLE %s.%s.%s", pgducklake::PGDUCKLAKE_DB_NAME,
                 schema_name, table_name);
    elog(DEBUG1, "drop query: %s", drop_query);
    // pgduckdb::DuckDBQueryOrThrow(*connection, drop_query);
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
  // auto connection = pgduckdb::DuckDBManager::GetConnection();

  char *cleanup_query;
  if (PG_ARGISNULL(0)) {
    /* Clean up all scheduled files */
    elog(INFO, "Cleaning up all scheduled files");
    cleanup_query =
        psprintf("SELECT count(*) FROM ducklake_cleanup_old_files('%s', "
                 "cleanup_all => true)",
                 pgducklake::PGDUCKLAKE_DB_NAME);
  } else {
    /* Convert interval to string using PostgreSQL's interval_out */
    Interval *interval = PG_GETARG_INTERVAL_P(0);
    char *interval_str = DatumGetCString(
        DirectFunctionCall1(interval_out, IntervalPGetDatum(interval)));

    /* Clean up files older than specified interval */
    cleanup_query =
        psprintf("SELECT count(*) FROM ducklake_cleanup_old_files('%s', "
                 "older_than => now() - INTERVAL '%s')",
                 pgducklake::PGDUCKLAKE_DB_NAME, interval_str);
  }

  // auto result = pgduckdb::DuckDBQueryOrThrow(*connection, cleanup_query);
  // auto chunk = result->Fetch();

  // int64_t files_cleaned = 0;
  // if (chunk && chunk->size() > 0) {
  //   files_cleaned = chunk->GetValue(0, 0).GetValue<int64_t>();
  // }

  // PG_RETURN_INT64(files_cleaned);
  PG_RETURN_INT64(0);
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
      "CALL %s.set_option(%s, %s", pgducklake::PGDUCKLAKE_DB_NAME,
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

  // pgduckdb::DuckDBQueryOrThrow(query);

  PG_RETURN_VOID();
}
}
