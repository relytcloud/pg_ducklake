#include "pgduckdb/ducklake/pgducklake_ddl.hpp"
#include "pgduckdb/ducklake/pgducklake_defs.hpp"
#include "pgduckdb/ducklake/pgducklake_metadata_manager.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_guc.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_table_am.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

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
#include "pgduckdb/vendor/pg_ruleutils.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"

#include "pgduckdb/pgduckdb_ruleutils.h"
}

namespace pgduckdb {
bool ducklake_ctas_skip_data = false;
} // namespace pgduckdb

static char *
pgducklake_get_tabledef(Oid relation_oid) {
	Relation relation = relation_open(relation_oid, AccessShareLock);
	const char *relation_name = pgduckdb_relation_name(relation_oid);
	const char *postgres_schema_name = get_namespace_name_or_temp(relation->rd_rel->relnamespace);
	const char *duckdb_table_am_name = pgduckdb::DuckdbTableAmGetName(relation->rd_tableam);
	const char *db_and_schema = pgduckdb_db_and_schema_string(postgres_schema_name, duckdb_table_am_name);

	StringInfoData buffer;
	initStringInfo(&buffer);

	if (relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("Using duckdb as a table access method on a partitioned table is not supported")));
	} else if (relation->rd_rel->relkind != RELKIND_RELATION) {
		elog(ERROR, "Only regular tables are supported in DuckDB");
	}

	if (relation->rd_rel->relispartition) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("DuckDB tables cannot be used as a partition")));
	}

	appendStringInfo(&buffer, "CREATE SCHEMA IF NOT EXISTS %s; ", db_and_schema);

	appendStringInfoString(&buffer, "CREATE ");

#if 0
	if (relation->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
		// allowed
	} else if (relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT) {
		elog(ERROR, "Only TEMP and non-UNLOGGED tables are supported in DuckDB");
	} else if (relation->rd_rel->relowner != pgduckdb::MotherDuckPostgresUserOid()) {
		elog(ERROR, "MotherDuck tables must be owned by the duckb.postgres_role");
	}
#endif

	appendStringInfo(&buffer, "TABLE %s (", relation_name);

	if (list_length(RelationGetFKeyList(relation)) > 0) {
		elog(ERROR, "DuckDB tables do not support foreign keys");
	}

	List *relation_context = pgduckdb_deparse_context_for(relation_name, relation_oid);

	/*
	 * Iterate over the table's columns. If a particular column is not dropped
	 * and is not inherited from another table, print the column's name and
	 * its formatted type.
	 */
	TupleDesc tuple_descriptor = RelationGetDescr(relation);
	TupleConstr *tuple_constraints = tuple_descriptor->constr;
	AttrDefault *default_value_list = tuple_constraints ? tuple_constraints->defval : NULL;

	bool first_column_printed = false;
	AttrNumber default_value_index = 0;
	for (int i = 0; i < tuple_descriptor->natts; i++) {
		Form_pg_attribute column = TupleDescAttr(tuple_descriptor, i);

		if (column->attisdropped) {
			continue;
		}

		const char *column_name = NameStr(column->attname);

		/*
		 * Check that this type is known by DuckDB, and throw the appropriate
		 * error otherwise. This is particularly important for NUMERIC without
		 * precision specified. Because that means something very different in
		 * Postgres
		 */
		auto duck_type = pgduckdb::ConvertPostgresToDuckColumnType(column);
		pgduckdb::GetPostgresDuckDBType(duck_type, true);

		const char *column_type_name = format_type_with_typemod(column->atttypid, column->atttypmod);

		if (first_column_printed) {
			appendStringInfoString(&buffer, ", ");
		}
		first_column_printed = true;

		appendStringInfo(&buffer, "%s ", quote_identifier(column_name));
		appendStringInfoString(&buffer, column_type_name);

		if (column->attcompression) {
			elog(ERROR, "Column compression is not supported in DuckDB");
		}

		if (column->attidentity) {
			elog(ERROR, "Identity columns are not supported in DuckDB");
		}

		/* if this column has a default value, append the default value */
		if (column->atthasdef) {
			Assert(tuple_constraints != NULL);
			Assert(default_value_list != NULL);

			AttrDefault *default_value = &(default_value_list[default_value_index]);
			default_value_index++;

			Assert(default_value->adnum == (i + 1));
			Assert(default_value_index <= tuple_constraints->num_defval);

			/*
			 * convert expression to node tree, and prepare deparse
			 * context
			 */
			Node *default_node = (Node *)stringToNode(default_value->adbin);

			/* deparse default value string */
			char *default_string = pgduckdb_deparse_expression(default_node, relation_context, false, false);

			/*
			 * DuckDB does not support STORED generated columns, it does
			 * support VIRTUAL generated columns though. Howevever, Postgres
			 * currently does not support those, so for now there's no overlap
			 * in generated column support between the two databases.
			 */
			if (!column->attgenerated) {
				appendStringInfo(&buffer, " DEFAULT %s", default_string);
			} else if (column->attgenerated == ATTRIBUTE_GENERATED_STORED) {
				elog(ERROR, "DuckDB does not support STORED generated columns");
			} else {
				elog(ERROR, "Unkown generated column type");
			}
		}

		/* if this column has a not null constraint, append the constraint */
		if (column->attnotnull) {
			appendStringInfoString(&buffer, " NOT NULL");
		}

		/*
		 * XXX: default collation is actually probably not supported by
		 * DuckDB, unless it's C or POSIX. But failing unless people
		 * provide C or POSIX seems pretty annoying. How should we handle
		 * this?
		 */
#if 0
			Oid collation = column->attcollation;
		if (collation != InvalidOid && collation != DEFAULT_COLLATION_OID && !pgduckdb::pg::IsCLocale(collation)) {
			elog(ERROR, "DuckDB does not support column collations");
		}
#endif
	}

	/*
	 * Now check if the table has any constraints. If it does, set the number
	 * of check constraints here. Then iterate over all check constraints and
	 * print them.
	 */
	AttrNumber constraint_count = tuple_constraints ? tuple_constraints->num_check : 0;
	ConstrCheck *check_constraint_list = tuple_constraints ? tuple_constraints->check : NULL;

	for (AttrNumber i = 0; i < constraint_count; i++) {
		ConstrCheck *check_constraint = &(check_constraint_list[i]);

		/* convert expression to node tree, and prepare deparse context */
		Node *check_node = (Node *)stringToNode(check_constraint->ccbin);

		/* deparse check constraint string */
		char *check_string = pgduckdb_deparse_expression(check_node, relation_context, false, false);

		/* if an attribute or constraint has been printed, format properly */
		if (first_column_printed || i > 0) {
			appendStringInfoString(&buffer, ", ");
		}

		appendStringInfo(&buffer, "CONSTRAINT %s CHECK ", quote_identifier(check_constraint->ccname));

		appendStringInfoString(&buffer, "(");
		appendStringInfoString(&buffer, check_string);
		appendStringInfoString(&buffer, ")");
	}

	/* close create table's outer parentheses */
	appendStringInfoString(&buffer, ")");

#if 0
	if (!pgduckdb::IsDuckdbTableAm(relation->rd_tableam)) {
		/* Shouldn't happen but seems good to check anyway */
		elog(ERROR, "Only a table with the DuckDB can be stored in DuckDB, %d %d", relation->rd_rel->relam,
		     pgduckdb::DuckdbTableAmOid());
	}
#endif

	if (relation->rd_options) {
		elog(ERROR, "Storage options are not supported in DuckDB");
	}

	relation_close(relation, AccessShareLock);

	return buffer.data;
}

extern "C" {

DECLARE_PG_FUNCTION(ducklake_initialize) {
	if (!creating_extension) {
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("ducklake_initialize() can only be called during CREATE EXTENSION")));
	}

	if (pgduckdb::PgDuckLakeMetadataManager::IsInitialized()) {
		ereport(ERROR,
		        (errcode(ERRCODE_DUPLICATE_SCHEMA), errmsg("DuckLake reserved schema \"ducklake\" is already in use")));
	}

	/* Create the data directory for DuckLake parquet files */
	auto data_path = duckdb::StringUtil::Format("%s/pg_ducklake", DataDir);
	try {
		std::filesystem::create_directory(data_path);
	} catch (const std::filesystem::filesystem_error &e) {
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
		                errmsg("failed to create DuckLake data directory \"%s\": %s", data_path.c_str(), e.what())));
	}

	/* Initialize DuckDB and attach the DuckLake catalog */
	try {
		auto connection = pgduckdb::DuckDBManager::GetConnection();
		pgduckdb::DuckDBQueryOrThrow(*connection, "ATTACH 'ducklake:pgducklake:' AS pgducklake "
		                                          "(METADATA_SCHEMA 'ducklake', DATA_PATH '" +
		                                              data_path + "')");
		/* Reset DuckDB instance to apply initialization settings */
		pgduckdb::DuckDBManager::Reset();
	} catch (const std::exception &e) {
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("failed to initialize DuckLake: %s", e.what())));
	}

	PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_create_table_trigger) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	if (!pgduckdb::IsExtensionRegistered()) {
		/*
		 * We're not installed, so don't mess with the query. Normally this
		 * shouldn't happen, but better safe than sorry.
		 */
		PG_RETURN_NULL();
	}

	EventTriggerData *trigger_data = (EventTriggerData *)fcinfo->context;
	Node *parsetree = trigger_data->parsetree;

	SPI_connect();

	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);

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
		elog(ERROR, "Expected single table to be created, but found %" PRIu64, static_cast<uint64_t>(SPI_processed));
	}

	if (!IsA(parsetree, CreateStmt) && !IsA(parsetree, CreateTableAsStmt)) {
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
		                errmsg("Cannot create a DuckLake table this way, use CREATE TABLE or CREATE TABLE AS")));
	}

	HeapTuple tuple = SPI_tuptable->vals[0];
	bool isnull;
	Datum relid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull) {
		elog(ERROR, "Expected relid to be returned, but found NULL");
	}

	Datum is_temporary_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 2, &isnull);
	if (isnull) {
		elog(ERROR, "Expected temporary boolean to be returned, but found NULL");
	}

	Oid relid = DatumGetObjectId(relid_datum);
	bool is_temporary = DatumGetBool(is_temporary_datum);

	if (is_temporary) {
		elog(ERROR, "TODO: create a DuckLake table as a temporary table is not supported yet");
	}

	std::string create_table_string(pgducklake_get_tabledef(relid));

	elog(DEBUG1, "create_table_string: %s", create_table_string.c_str());

	AtEOXact_GUC(false, save_nestlevel);
	SPI_finish();

	auto connection = pgduckdb::DuckDBManager::GetConnection(false);

	std::string set_table_path_string;
	auto &table_path = pgduckdb::ducklake_default_table_path;
	if (table_path == nullptr || strlen(table_path) == 0) {
		// If path is empty or null, reset the DuckDB variable
		set_table_path_string = "RESET ducklake_default_table_path";
		elog(DEBUG2, "[PGDuckDB] Reset DuckDB option: 'ducklake_default_table_path'");
	} else {
		set_table_path_string = "SET ducklake_default_table_path=" + duckdb::KeywordHelper::WriteQuoted(table_path);
		elog(DEBUG2, "[PGDuckDB] Set DuckDB option: 'ducklake_default_table_path'=%s", table_path);
	}

	pgduckdb::DuckDBQueryOrThrow(*connection, set_table_path_string + ";" + create_table_string);
	if (IsA(parsetree, CreateTableAsStmt) && !pgduckdb::ducklake_ctas_skip_data) {
		auto ctas_stmt = castNode(CreateTableAsStmt, parsetree);
		auto ctas_query = (Query *)ctas_stmt->query;
		const char *ctas_query_string = pgduckdb_get_querydef(ctas_query);
		std::string insert_string =
		    std::string("INSERT INTO ") + pgduckdb_relation_name(relid) + " " + ctas_query_string;
		pgduckdb::DuckDBQueryOrThrow(*connection, insert_string);
	}

	PG_RETURN_NULL();
}

DECLARE_PG_FUNCTION(ducklake_drop_trigger) {
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) /* internal error */
		elog(ERROR, "not fired by event trigger manager");

	if (!pgduckdb::IsExtensionRegistered()) {
		/*
		 * We're not installed, so don't mess with the query. Normally this
		 * shouldn't happen, but better safe than sorry.
		 */
		PG_RETURN_NULL();
	}

	SPI_connect();

	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("search_path", "pg_catalog, pg_temp", PGC_USERSET, PGC_S_SESSION);
	SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);

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

	auto connection = pgduckdb::DuckDBManager::GetConnection(true);

	for (uint64_t proc = 0; proc < SPI_processed; ++proc) {
		HeapTuple tuple = SPI_tuptable->vals[proc];

		char *schema_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 1);
		char *table_name = SPI_getvalue(tuple, SPI_tuptable->tupdesc, 2);

		char *drop_query = psprintf("DROP TABLE %s.%s.%s", pgduckdb::PGDUCKLAKE_DB_NAME, schema_name, table_name);
		elog(DEBUG1, "drop query: %s", drop_query);
		pgduckdb::DuckDBQueryOrThrow(*connection, drop_query);
	}

	AtEOXact_GUC(false, save_nestlevel);
	SPI_finish();

	PG_RETURN_NULL();
}

/*
 * ducklake_cleanup(older_than) - Clean up old files in the DuckLake database.
 *
 * Parameters:
 *   older_than - PostgreSQL interval (e.g., '24 hours'::interval, '7 days'::interval).
 *                If NULL, all scheduled files will be cleaned up.
 *
 * Returns the number of files cleaned up.
 */
DECLARE_PG_FUNCTION(ducklake_cleanup_old_files) {
	auto connection = pgduckdb::DuckDBManager::GetConnection();

	char *cleanup_query;
	if (PG_ARGISNULL(0)) {
		/* Clean up all scheduled files */
		elog(INFO, "Cleaning up all scheduled files");
		cleanup_query = psprintf("SELECT count(*) FROM ducklake_cleanup_old_files('%s', cleanup_all => true)",
		                         pgduckdb::PGDUCKLAKE_DB_NAME);
	} else {
		/* Convert interval to string using PostgreSQL's interval_out */
		Interval *interval = PG_GETARG_INTERVAL_P(0);
		char *interval_str = DatumGetCString(DirectFunctionCall1(interval_out, IntervalPGetDatum(interval)));

		/* Clean up files older than specified interval */
		cleanup_query =
		    psprintf("SELECT count(*) FROM ducklake_cleanup_old_files('%s', older_than => now() - INTERVAL '%s')",
		             pgduckdb::PGDUCKLAKE_DB_NAME, interval_str);
	}

	auto result = pgduckdb::DuckDBQueryOrThrow(*connection, cleanup_query);
	auto chunk = result->Fetch();

	int64_t files_cleaned = 0;
	if (chunk && chunk->size() > 0) {
		files_cleaned = chunk->GetValue(0, 0).GetValue<int64_t>();
	}

	PG_RETURN_INT64(files_cleaned);
}
}
