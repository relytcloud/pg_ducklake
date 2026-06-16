/*
 * Foreign Data Wrapper for DuckLake tables. Two modes: regular
 * (PostgreSQL-backed metadata, full DML) and frozen (static .ducklake snapshot
 * over HTTP, read-only). Queries never run through the FDW scan callbacks: the
 * planner routes them whole to DuckDB.
 */

#include "pgducklake/duckdb_manager.hpp"
#include "pgducklake/ducklake_fdw.hpp"
#include "pgducklake/ducklake_secret.hpp"

#include "pgddb/pgddb_types.hpp"

#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/prepared_statement.hpp>

#include "pgddb/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"

#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "pgddb/pgddb_ruleutils.h"
}

struct DucklakeFdwOption {
	const char *optname;
	Oid context;
};

static const DucklakeFdwOption valid_server_options[] = {
    {"dbname", ForeignServerRelationId},          {"connection_string", ForeignServerRelationId},
    {"metadata_schema", ForeignServerRelationId}, {"frozen_url", ForeignServerRelationId},
    {"updatable", ForeignServerRelationId},       {nullptr, InvalidOid}};

static const DucklakeFdwOption valid_table_options[] = {{"schema_name", ForeignTableRelationId},
                                                        {"table_name", ForeignTableRelationId},
                                                        {"updatable", ForeignTableRelationId},
                                                        {nullptr, InvalidOid}};

static bool
IsValidOption(const char *name, Oid context) {
	for (auto *opt = valid_server_options; opt->optname; opt++) {
		if (opt->context == context && strcmp(name, opt->optname) == 0)
			return true;
	}
	for (auto *opt = valid_table_options; opt->optname; opt++) {
		if (opt->context == context && strcmp(name, opt->optname) == 0)
			return true;
	}
	return false;
}

static const char *
GetOptionValue(List *options, const char *name) {
	ListCell *lc;
	foreach (lc, options) {
		DefElem *def = (DefElem *)lfirst(lc);
		if (strcmp(def->defname, name) == 0)
			return defGetString(def);
	}
	return nullptr;
}

static const char *FDW_NAME = "ducklake_fdw";

static Oid
GetDucklakeFdwOid() {
	static Oid cached_oid = InvalidOid;
	if (cached_oid != InvalidOid)
		return cached_oid;
	HeapTuple tup = SearchSysCache1(FOREIGNDATAWRAPPERNAME, CStringGetDatum(FDW_NAME));
	if (!HeapTupleIsValid(tup))
		return InvalidOid;
	Form_pg_foreign_data_wrapper fdw = (Form_pg_foreign_data_wrapper)GETSTRUCT(tup);
	cached_oid = fdw->oid;
	ReleaseSysCache(tup);
	return cached_oid;
}

static bool
IsFrozenServer(ForeignServer *server) {
	return GetOptionValue(server->options, "frozen_url") != nullptr;
}

/* Table option overrides server; frozen defaults false, else true (postgres_fdw convention). */
static bool
IsUpdatable(ForeignTable *ft, ForeignServer *server) {
	const char *table_val = GetOptionValue(ft->options, "updatable");
	if (table_val)
		return pg_strcasecmp(table_val, "true") == 0;
	const char *server_val = GetOptionValue(server->options, "updatable");
	if (server_val)
		return pg_strcasecmp(server_val, "true") == 0;
	return !IsFrozenServer(server);
}

static bool
IsDucklakeForeignTable(Oid relid) {
	if (get_rel_relkind(relid) != RELKIND_FOREIGN_TABLE)
		return false;
	ForeignTable *ft = GetForeignTable(relid);
	ForeignServer *server = GetForeignServer(ft->serverid);
	return server->fdwid == GetDucklakeFdwOid();
}

static duckdb::string
GetDatabaseAlias(ForeignServer *server) {
	if (IsFrozenServer(server))
		return server->servername;
	const char *connstr = GetOptionValue(server->options, "connection_string");
	if (connstr)
		return duckdb::StringUtil::Format("fdw_db_%s", server->servername);
	const char *dbname = GetOptionValue(server->options, "dbname");
	if (!dbname)
		dbname = get_database_name(MyDatabaseId);
	return duckdb::StringUtil::Format("fdw_db_%s", dbname);
}

static duckdb::string
BuildAttachQuery(ForeignServer *server, const char *db_alias, bool if_not_exists) {
	const char *exists_clause = if_not_exists ? " IF NOT EXISTS" : "";

	if (IsFrozenServer(server)) {
		const char *url = GetOptionValue(server->options, "frozen_url");
		return duckdb::StringUtil::Format("ATTACH%s 'ducklake:%s' AS \"%s\"", exists_clause, url, db_alias);
	}

	const char *schema = GetOptionValue(server->options, "metadata_schema");
	if (!schema)
		schema = "ducklake";

	const char *connstr = GetOptionValue(server->options, "connection_string");
	if (connstr) {
		return duckdb::StringUtil::Format("ATTACH%s 'postgres:%s' "
		                                  "AS \"%s\" (TYPE DUCKLAKE, METADATA_SCHEMA '%s')",
		                                  exists_clause, connstr, db_alias, schema);
	}

	const char *dbname = GetOptionValue(server->options, "dbname");
	if (!dbname)
		dbname = get_database_name(MyDatabaseId);

	const char *user = GetUserNameFromId(GetUserId(), false);
	return duckdb::StringUtil::Format("ATTACH%s 'postgres:dbname=%s user=%s' "
	                                  "AS \"%s\" (TYPE DUCKLAKE, METADATA_SCHEMA '%s')",
	                                  exists_clause, dbname, user, db_alias, schema);
}

static char *
GetDucklakeForeignTableName(Oid relid) {
	if (get_rel_relkind(relid) != RELKIND_FOREIGN_TABLE)
		return nullptr;

	ForeignTable *ft = GetForeignTable(relid);
	ForeignServer *server = GetForeignServer(ft->serverid);
	if (server->fdwid != GetDucklakeFdwOid())
		return nullptr;

	const char *schema_name = GetOptionValue(ft->options, "schema_name");
	if (!schema_name)
		schema_name = "public";

	const char *table_name = GetOptionValue(ft->options, "table_name");
	if (!table_name)
		table_name = get_rel_name(relid);

	duckdb::string db_alias = GetDatabaseAlias(server);

	return psprintf("%s.%s.%s", quote_identifier(db_alias.c_str()), quote_identifier(schema_name),
	                quote_identifier(table_name));
}

static void
AttachDucklakeDatabase(ForeignServer *server) {
	duckdb::string db_alias = GetDatabaseAlias(server);
	duckdb::string query = BuildAttachQuery(server, db_alias.c_str(), true);

	elog(DEBUG1, "ducklake_fdw: %s", query.c_str());

	try {
		pgducklake::DuckDBQueryOrThrow(query);
	} catch (const std::exception &e) {
		elog(ERROR, "ducklake_fdw: ATTACH failed: %s", pgducklake::DuckDBErrorMessage(e).c_str());
	}
}

// Lets the planner hook route foreign-table queries through DuckDB rather than
// the FDW direct-scan stub (which errors out by design).
bool
pgducklake::QueryReferencesDucklakeForeignTable(Query *query) {
	if (!query)
		return false;

	Oid fdw_oid = GetDucklakeFdwOid();
	if (fdw_oid == InvalidOid)
		return false;

	ListCell *lc;
	foreach (lc, query->rtable) {
		RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
		if (rte->rtekind == RTE_SUBQUERY && rte->subquery) {
			if (pgducklake::QueryReferencesDucklakeForeignTable(rte->subquery))
				return true;
			continue;
		}
		if (rte->relid == InvalidOid)
			continue;
		if (get_rel_relkind(rte->relid) != RELKIND_FOREIGN_TABLE)
			continue;
		ForeignTable *ft = GetForeignTable(rte->relid);
		ForeignServer *server = GetForeignServer(ft->serverid);
		if (server->fdwid == fdw_oid)
			return true;
	}
	foreach (lc, query->cteList) {
		CommonTableExpr *cte = (CommonTableExpr *)lfirst(lc);
		if (IsA(cte->ctequery, Query) &&
		    pgducklake::QueryReferencesDucklakeForeignTable(castNode(Query, cte->ctequery)))
			return true;
	}
	return false;
}

void
pgducklake::RegisterForeignTablesInQuery(Query *query) {
	if (!query || !query->rtable)
		return;

	Oid fdw_oid = GetDucklakeFdwOid();
	if (fdw_oid == InvalidOid)
		return;

	ListCell *lc;
	foreach (lc, query->rtable) {
		RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
		if (rte->rtekind == RTE_SUBQUERY && rte->subquery)
			pgducklake::RegisterForeignTablesInQuery(rte->subquery);

		if (rte->relid == InvalidOid)
			continue;
		if (get_rel_relkind(rte->relid) != RELKIND_FOREIGN_TABLE)
			continue;

		ForeignTable *ft = GetForeignTable(rte->relid);
		ForeignServer *server = GetForeignServer(ft->serverid);
		if (server->fdwid != fdw_oid)
			continue;

		AttachDucklakeDatabase(server);
	}

	foreach (lc, query->cteList) {
		CommonTableExpr *cte = (CommonTableExpr *)lfirst(lc);
		if (IsA(cte->ctequery, Query))
			pgducklake::RegisterForeignTablesInQuery(castNode(Query, cte->ctequery));
	}

	/* Block DML on non-updatable FDW tables; writable ones go through pg_duckdb's planner. */
	if (query->commandType != CMD_SELECT && query->resultRelation > 0) {
		RangeTblEntry *result_rte = list_nth_node(RangeTblEntry, query->rtable, query->resultRelation - 1);
		if (result_rte->relid != InvalidOid && IsDucklakeForeignTable(result_rte->relid)) {
			ForeignTable *ft = GetForeignTable(result_rte->relid);
			ForeignServer *server = GetForeignServer(ft->serverid);
			if (!IsUpdatable(ft, server)) {
				const char *op = "Unknown";
				switch (query->commandType) {
				case CMD_INSERT:
					op = "INSERT";
					break;
				case CMD_UPDATE:
					op = "UPDATE";
					break;
				case CMD_DELETE:
					op = "DELETE";
					break;
				default:
					break;
				}
				ereport(ERROR,
				        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("ducklake_fdw foreign table is not updatable"),
				         errhint("%s is not supported when the \"updatable\" option is false.", op)));
			}
		}
	}
}

/* Reuses the runtime alias with IF NOT EXISTS to avoid file-handle conflicts
 * when the catalog is already ATTACHed (frozen .ducklake files). */
static List *
InferForeignTableColumns(CreateForeignTableStmt *stmt) {
	if (!pgducklake::DuckDBManager::IsInitialized()) {
		try {
			pgducklake::DuckDBQueryOrThrow("SELECT 1");
		} catch (const std::exception &) {
			// Best-effort init; a real failure surfaces on the ATTACH below.
		}
	}

	const char *server_name = stmt->servername;
	ForeignServer *server = GetForeignServerByName(server_name, false);

	const char *schema_name = GetOptionValue(stmt->options, "schema_name");
	if (!schema_name)
		schema_name = "public";

	const char *table_name = GetOptionValue(stmt->options, "table_name");
	if (!table_name)
		table_name = stmt->base.relation->relname;

	auto conn = pgducklake::DuckDBManager::Get().GetConnection();

	duckdb::string db_alias = GetDatabaseAlias(server);
	duckdb::string attach_query = BuildAttachQuery(server, db_alias.c_str(), true);

	auto attach_result = conn->Query(attach_query);
	if (attach_result->HasError()) {
		elog(ERROR, "ducklake_fdw: column inference ATTACH failed: %s", attach_result->GetError().c_str());
	}

	duckdb::string select_query =
	    duckdb::StringUtil::Format("SELECT * FROM \"%s\".%s.%s LIMIT 0", db_alias.c_str(),
	                               duckdb::KeywordHelper::WriteOptionallyQuoted(schema_name).c_str(),
	                               duckdb::KeywordHelper::WriteOptionallyQuoted(table_name).c_str());

	auto prepared = conn->Prepare(select_query);
	if (prepared->HasError()) {
		elog(ERROR, "ducklake_fdw: cannot read table \"%s\".\"%s\": %s", schema_name, table_name,
		     prepared->error.Message().c_str());
	}

	List *columns = NIL;
	auto &names = prepared->GetNames();
	auto &types = prepared->GetTypes();

	for (size_t i = 0; i < names.size(); i++) {
		Oid pg_type = pgddb::GetPostgresDuckDBType(types[i]);
		int32_t typemod = pgddb::GetPostgresDuckDBTypemod(types[i]);

		ColumnDef *col = makeColumnDef(names[i].c_str(), pg_type, typemod, InvalidOid);
		columns = lappend(columns, col);
	}

	return columns;
}

static List *
DucklakeImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid) {
	if (!pgducklake::DuckDBManager::IsInitialized()) {
		try {
			pgducklake::DuckDBQueryOrThrow("SELECT 1");
		} catch (const std::exception &) {
			// Best-effort init; a real failure surfaces on the ATTACH below.
		}
	}

	ForeignServer *server = GetForeignServer(serverOid);

	auto conn = pgducklake::DuckDBManager::Get().GetConnection();
	duckdb::string db_alias = GetDatabaseAlias(server);
	duckdb::string attach_query = BuildAttachQuery(server, db_alias.c_str(), true);

	auto attach_result = conn->Query(attach_query);
	if (attach_result->HasError())
		elog(ERROR, "ducklake_fdw: IMPORT FOREIGN SCHEMA ATTACH failed: %s", attach_result->GetError().c_str());

	List *commands = NIL;

	{
		/* duckdb_tables(): information_schema is unavailable on ATTACHed DuckLake databases. */
		duckdb::string list_query = duckdb::StringUtil::Format(
		    "SELECT table_name FROM duckdb_tables() "
		    "WHERE database_name = '%s' AND schema_name = '%s' "
		    "ORDER BY table_name",
		    db_alias.c_str(), duckdb::KeywordHelper::WriteOptionallyQuoted(stmt->remote_schema).c_str());

		auto list_result = conn->Query(list_query);
		if (list_result->HasError())
			elog(ERROR, "ducklake_fdw: cannot list tables in schema \"%s\": %s", stmt->remote_schema,
			     list_result->GetError().c_str());

		duckdb::vector<duckdb::string> table_names;
		for (idx_t row = 0; row < list_result->RowCount(); row++) {
			duckdb::string tname = list_result->GetValue(0, row).ToString();

			if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO) {
				bool found = false;
				ListCell *lc;
				foreach (lc, stmt->table_list) {
					RangeVar *rv = (RangeVar *)lfirst(lc);
					if (strcmp(rv->relname, tname.c_str()) == 0) {
						found = true;
						break;
					}
				}
				if (!found)
					continue;
			} else if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT) {
				bool excluded = false;
				ListCell *lc;
				foreach (lc, stmt->table_list) {
					RangeVar *rv = (RangeVar *)lfirst(lc);
					if (strcmp(rv->relname, tname.c_str()) == 0) {
						excluded = true;
						break;
					}
				}
				if (excluded)
					continue;
			}

			table_names.push_back(tname);
		}

		for (auto &tname : table_names) {
			duckdb::string select_query =
			    duckdb::StringUtil::Format("SELECT * FROM \"%s\".%s.%s LIMIT 0", db_alias.c_str(),
			                               duckdb::KeywordHelper::WriteOptionallyQuoted(stmt->remote_schema).c_str(),
			                               duckdb::KeywordHelper::WriteOptionallyQuoted(tname).c_str());

			auto prepared = conn->Prepare(select_query);
			if (prepared->HasError())
				elog(ERROR, "ducklake_fdw: cannot read table \"%s\".\"%s\": %s", stmt->remote_schema, tname.c_str(),
				     prepared->error.Message().c_str());

			auto &names = prepared->GetNames();
			auto &types = prepared->GetTypes();

			StringInfoData col_buf;
			initStringInfo(&col_buf);
			for (size_t i = 0; i < names.size(); i++) {
				Oid pg_type = pgddb::GetPostgresDuckDBType(types[i]);
				int32_t typemod = pgddb::GetPostgresDuckDBTypemod(types[i]);
				char *type_name = format_type_with_typemod(pg_type, typemod);

				if (i > 0)
					appendStringInfoString(&col_buf, ", ");
				appendStringInfo(&col_buf, "%s %s", quote_identifier(names[i].c_str()), type_name);
			}

			StringInfoData sql;
			initStringInfo(&sql);
			appendStringInfo(&sql,
			                 "CREATE FOREIGN TABLE %s.%s (%s) SERVER %s OPTIONS (schema_name '%s', table_name '%s')",
			                 quote_identifier(stmt->local_schema), quote_identifier(tname.c_str()), col_buf.data,
			                 quote_identifier(server->servername), stmt->remote_schema, tname.c_str());

			commands = lappend(commands, pstrdup(sql.data));
			pfree(col_buf.data);
			pfree(sql.data);
		}
	}

	return commands;
}

static ProcessUtility_hook_type prev_fdw_process_utility_hook = NULL;

static void
DucklakeFdwUtilityHook(PlannedStmt *pstmt, const char *query_string, bool read_only_tree, ProcessUtilityContext context,
                       ParamListInfo params, struct QueryEnvironment *query_env, DestReceiver *dest,
                       QueryCompletion *qc) {
	if (pstmt->utilityStmt && IsA(pstmt->utilityStmt, CreateForeignTableStmt)) {
		auto *cft = castNode(CreateForeignTableStmt, pstmt->utilityStmt);

		ForeignDataWrapper *fdw = GetForeignDataWrapperByName(FDW_NAME, true);
		if (fdw) {
			ForeignServer *server = GetForeignServerByName(cft->servername, true);
			if (server && server->fdwid == fdw->fdwid) {
				if (cft->base.tableElts == NIL) {
					if (read_only_tree) {
						pstmt = (PlannedStmt *)copyObjectImpl(pstmt);
						cft = castNode(CreateForeignTableStmt, pstmt->utilityStmt);
						read_only_tree = false;
					}

					cft->base.tableElts = InferForeignTableColumns(cft);
				}
			}
		}
	}

	// Stash the target server's type/oid before the DDL runs, so the
	// ducklake_secret FDW validator can reach them (it only receives options).
	if (pstmt->utilityStmt) {
		pgducklake::CaptureSecretServer(pstmt->utilityStmt);
	}

	prev_fdw_process_utility_hook(pstmt, query_string, read_only_tree, context, params, query_env, dest, qc);
}

/* Minimal FDW callbacks; execution goes through DuckDB. */

static void
DucklakeGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
	baserel->rows = 1000;
}

static void
DucklakeGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
	add_path(baserel, (Path *)create_foreignscan_path(root, baserel, NULL, baserel->rows,
#if PG_VERSION_NUM >= 180000
	                                                  0, /* disabled_nodes */
#endif
	                                                  10, 1000, NIL, NULL, NULL,
#if PG_VERSION_NUM >= 170000
	                                                  NIL, /* fdw_restrictinfo */
#endif
	                                                  NIL));
}

static ForeignScan *
DucklakeGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist,
                       List *scan_clauses, Plan *outer_plan) {
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, NIL, NIL, NIL, outer_plan);
}

static void
DucklakeBeginForeignScan(ForeignScanState *node, int eflags) {
	elog(ERROR, "ducklake_fdw: direct scan should not be reached; "
	            "query should be routed through DuckDB");
}

static TupleTableSlot *
DucklakeIterateForeignScan(ForeignScanState *node) {
	elog(ERROR, "ducklake_fdw: direct scan should not be reached; "
	            "query should be routed through DuckDB");
	return nullptr;
}

static void
DucklakeReScanForeignScan(ForeignScanState *node) {
	elog(ERROR, "ducklake_fdw: direct scan should not be reached; "
	            "query should be routed through DuckDB");
}

static void
DucklakeEndForeignScan(ForeignScanState *node) {
}

extern "C" {

DECLARE_PG_FUNCTION(ducklake_fdw_handler) {
	FdwRoutine *routine = makeNode(FdwRoutine);
	routine->GetForeignRelSize = DucklakeGetForeignRelSize;
	routine->GetForeignPaths = DucklakeGetForeignPaths;
	routine->GetForeignPlan = DucklakeGetForeignPlan;
	routine->BeginForeignScan = DucklakeBeginForeignScan;
	routine->IterateForeignScan = DucklakeIterateForeignScan;
	routine->ReScanForeignScan = DucklakeReScanForeignScan;
	routine->EndForeignScan = DucklakeEndForeignScan;
	routine->ImportForeignSchema = DucklakeImportForeignSchema;
	PG_RETURN_POINTER(routine);
}

DECLARE_PG_FUNCTION(ducklake_fdw_validator) {
	List *options = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);

	ListCell *lc;
	foreach (lc, options) {
		DefElem *def = (DefElem *)lfirst(lc);
		if (!IsValidOption(def->defname, catalog)) {
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			                errmsg("invalid ducklake_fdw option \"%s\"", def->defname)));
		}
	}

	if (catalog == ForeignServerRelationId) {
		bool has_frozen = GetOptionValue(options, "frozen_url") != nullptr;
		bool has_dbname = GetOptionValue(options, "dbname") != nullptr;
		bool has_connstr = GetOptionValue(options, "connection_string") != nullptr;
		bool has_schema = GetOptionValue(options, "metadata_schema") != nullptr;

		if (has_frozen && has_dbname)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			                errmsg("\"frozen_url\" and \"dbname\" are mutually exclusive")));
		if (has_frozen && has_schema)
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME), errmsg("\"frozen_url\" and \"metadata_schema\" are "
			                                                          "mutually exclusive")));
		if (has_frozen && has_connstr)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			                errmsg("\"frozen_url\" and \"connection_string\" are mutually exclusive")));
		if (has_connstr && has_dbname)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			                errmsg("\"connection_string\" and \"dbname\" are mutually exclusive")));

		const char *updatable_val = GetOptionValue(options, "updatable");
		if (updatable_val && pg_strcasecmp(updatable_val, "true") != 0 && pg_strcasecmp(updatable_val, "false") != 0)
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME), errmsg("\"updatable\" must be \"true\" or \"false\"")));
		if (has_frozen && updatable_val && pg_strcasecmp(updatable_val, "true") == 0)
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			                errmsg("\"frozen_url\" and \"updatable 'true'\" are mutually exclusive")));
	}

	if (catalog == ForeignTableRelationId) {
		const char *updatable_val = GetOptionValue(options, "updatable");
		if (updatable_val && pg_strcasecmp(updatable_val, "true") != 0 && pg_strcasecmp(updatable_val, "false") != 0)
			ereport(ERROR,
			        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME), errmsg("\"updatable\" must be \"true\" or \"false\"")));
	}

	PG_RETURN_VOID();
}

} // extern "C"

namespace pgducklake {

void
InitFDW() {
	// libpgddb's deparser uses this hook for the DuckDB-qualified name of
	// ducklake_fdw foreign tables (servers are ATTACHed as fdw_db_* aliases).
	Register_pgddb_relation_name(GetDucklakeForeignTableName);

	prev_fdw_process_utility_hook = ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = DucklakeFdwUtilityHook;
}

} // namespace pgducklake
