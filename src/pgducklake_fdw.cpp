/*
 * pgducklake_fdw.cpp — Foreign Data Wrapper for DuckLake tables
 *
 * Enables read-only access to DuckLake tables from other databases.
 * Instead of implementing a full scan path, it registers tables with DuckDB
 * and leverages the existing pg_duckdb query execution infrastructure.
 *
 * Include order: DuckDB → DuckLake → Local → PostgreSQL (see CLAUDE.md)
 */

#include "pgducklake/pgducklake_defs.hpp"

#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/prepared_statement.hpp>

#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"

#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

/* pg_duckdb exported C symbols */
extern "C" {
Oid DuckdbLogicalTypeToPostgresOid(const void *logical_type, bool throw_error);
int32_t DuckdbLogicalTypeToPostgresTypemod(const void *logical_type);
}

/* pg_ducklake's own ExecuteDuckDBQuery */
extern "C" int ExecuteDuckDBQuery(const char *query, const char **errmsg_out);

#define DUCKLAKE_FDW_NAME "ducklake_fdw"

/* ----------------------------------------------------------------
 * Option validation
 * ---------------------------------------------------------------- */

struct DucklakeFdwOption {
	const char *optname;
	Oid context;
	bool required;
};

static const struct DucklakeFdwOption valid_server_options[] = {
    {"dbname", ForeignServerRelationId, false},
    {"metadata_schema", ForeignServerRelationId, false},
    {NULL, InvalidOid, false}
};

static const struct DucklakeFdwOption valid_table_options[] = {
    {"schema_name", ForeignTableRelationId, true},
    {"table_name", ForeignTableRelationId, true},
    {NULL, InvalidOid, false}
};

static bool
IsValidDucklakeFdwOption(const char *optname, Oid context) {
	const struct DucklakeFdwOption *options = NULL;

	if (context == ForeignServerRelationId)
		options = valid_server_options;
	else if (context == ForeignTableRelationId)
		options = valid_table_options;
	else
		return false;

	for (const struct DucklakeFdwOption *opt = options; opt->optname != NULL; ++opt) {
		if (strcmp(optname, opt->optname) == 0)
			return true;
	}
	return false;
}

static void
ValidateRequiredOptions(List *options_list, Oid context) {
	const struct DucklakeFdwOption *options = NULL;

	if (context == ForeignServerRelationId)
		options = valid_server_options;
	else if (context == ForeignTableRelationId)
		options = valid_table_options;
	else
		return;

	for (const struct DucklakeFdwOption *opt = options; opt->optname != NULL; ++opt) {
		if (!opt->required)
			continue;

		bool found = false;
		ListCell *lc_opt;
		foreach (lc_opt, options_list) {
			DefElem *def = lfirst_node(DefElem, lc_opt);
			if (strcmp(def->defname, opt->optname) == 0) {
				found = true;
				break;
			}
		}

		if (!found) {
			ereport(ERROR, (errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
			                errmsg("required option \"%s\" is missing", opt->optname)));
		}
	}
}

static const char *
GetOptionValue(List *options, const char *optname, const char *default_value = nullptr) {
	ListCell *lc_opt;
	foreach (lc_opt, options) {
		DefElem *def = lfirst_node(DefElem, lc_opt);
		if (strcmp(def->defname, optname) == 0)
			return defGetString(def);
	}
	return default_value;
}

/* ----------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------- */

static const char *
GetCurrentDatabaseName(void) {
	const char *dbname = get_database_name(MyDatabaseId);
	if (!dbname) {
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
		                errmsg("could not determine current database name")));
	}
	return dbname;
}

static std::string
EscapeDuckDBStringLiteral(const char *value) {
	return duckdb::StringUtil::Replace(std::string(value), "'", "''");
}

/* ----------------------------------------------------------------
 * Foreign table registration (ATTACH database in DuckDB session)
 * ---------------------------------------------------------------- */

static void
RegisterDucklakeForeignTable_Cpp(const char *dbname, const char *username,
                                 const char *metadata_schema, const char *attach_as) {
	auto escaped_dbname = EscapeDuckDBStringLiteral(dbname);
	auto escaped_username = EscapeDuckDBStringLiteral(username);
	auto escaped_metadata_schema = EscapeDuckDBStringLiteral(metadata_schema);

	auto attach_query = duckdb::StringUtil::Format(
	    "ATTACH IF NOT EXISTS 'postgres:dbname=%s user=%s' AS %s (TYPE DUCKLAKE, METADATA_SCHEMA '%s')",
	    escaped_dbname.c_str(), escaped_username.c_str(), attach_as, escaped_metadata_schema.c_str());

	elog(DEBUG1, "(DuckLake FDW) Attaching database as '%s': %s", attach_as, attach_query.c_str());

	const char *error_msg = nullptr;
	int result = ExecuteDuckDBQuery(attach_query.c_str(), &error_msg);
	if (result != 0) {
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
		                errmsg("failed to attach DuckLake FDW database: %s",
		                       error_msg ? error_msg : "unknown error")));
	}
}

static void
RegisterDucklakeForeignTable(Oid foreign_table_oid) {
	ForeignTable *ft = GetForeignTable(foreign_table_oid);
	ForeignServer *server = GetForeignServer(ft->serverid);

	const char *metadata_schema = GetOptionValue(server->options, "metadata_schema");
	const char *dbname = GetOptionValue(server->options, "dbname");

	if (!metadata_schema)
		metadata_schema = "ducklake";
	if (!dbname)
		dbname = GetCurrentDatabaseName();

	const char *username = GetUserNameFromId(GetUserId(), false);
	auto attach_as = psprintf("fdw_db_%s", dbname);
	InvokeCPPFunc(RegisterDucklakeForeignTable_Cpp, dbname, username, metadata_schema, attach_as);
}

/* ----------------------------------------------------------------
 * Foreign table identification
 * ---------------------------------------------------------------- */

static bool
IsDucklakeForeignTableInternal(Oid relid) {
	ForeignTable *ft = GetForeignTable(relid);
	ForeignServer *server = GetForeignServer(ft->serverid);
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);
	return (strcmp(fdw->fdwname, DUCKLAKE_FDW_NAME) == 0);
}

/*
 * Check if a relation is a DuckLake foreign table.
 * Used as callback for RegisterDuckdbExternalTableCheck.
 */
static bool
IsDucklakeForeignTable(Oid relid) {
	if (!OidIsValid(relid))
		return false;
	Relation rel = table_open(relid, NoLock);
	bool result = false;

	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		result = IsDucklakeForeignTableInternal(relid);

	table_close(rel, NoLock);
	return result;
}

/*
 * Get the qualified DuckDB name for a DuckLake foreign table.
 * Used as callback for RegisterDuckdbRelationNameCallback.
 * Returns NULL if the relation is not a DuckLake foreign table.
 */
static char *
GetDucklakeForeignTableName(Oid foreign_table_oid) {
	/* Quick check: is this a foreign table at all? */
	char relkind = get_rel_relkind(foreign_table_oid);
	if (relkind != RELKIND_FOREIGN_TABLE)
		return NULL;

	if (!IsDucklakeForeignTableInternal(foreign_table_oid))
		return NULL;

	ForeignTable *ft = GetForeignTable(foreign_table_oid);
	ForeignServer *server = GetForeignServer(ft->serverid);

	const char *schema_name = GetOptionValue(ft->options, "schema_name");
	const char *table_name = GetOptionValue(ft->options, "table_name");
	const char *dbname = GetOptionValue(server->options, "dbname", GetCurrentDatabaseName());

	return psprintf("fdw_db_%s.%s.%s", dbname,
	                quote_identifier(schema_name), quote_identifier(table_name));
}

/* ----------------------------------------------------------------
 * Column inference for CREATE FOREIGN TABLE
 * ---------------------------------------------------------------- */

static void
InferAndPopulateForeignTableColumns_Cpp(CreateForeignTableStmt *stmt,
                                        const char *schema_name, const char *table_name,
                                        const char *dbname, const char *username,
                                        const char *metadata_schema) {
	const char *attach_name = "fdw_probe_ddl";
	auto escaped_dbname = EscapeDuckDBStringLiteral(dbname);
	auto escaped_username = EscapeDuckDBStringLiteral(username);
	auto escaped_metadata_schema = EscapeDuckDBStringLiteral(metadata_schema);

	auto attach_query = duckdb::StringUtil::Format(
	    "ATTACH IF NOT EXISTS 'postgres:dbname=%s user=%s' AS %s (TYPE DUCKLAKE, METADATA_SCHEMA '%s')",
	    escaped_dbname.c_str(), escaped_username.c_str(), attach_name, escaped_metadata_schema.c_str());

	auto probe_query = duckdb::StringUtil::Format("SELECT * FROM %s.%s.%s LIMIT 0", attach_name,
	                                              quote_identifier(schema_name), quote_identifier(table_name));
	auto detach_query = duckdb::StringUtil::Format("DETACH DATABASE IF EXISTS %s", attach_name);

	/* Get a DuckDB connection for probing */
	auto *db = static_cast<duckdb::DuckDB *>(ducklake_get_duckdb_database());
	auto conn = duckdb::make_uniq<duckdb::Connection>(*db);

	/* Attach the target database */
	auto attach_result = conn->Query(attach_query);
	if (attach_result->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::CATALOG,
		    duckdb::StringUtil::Format("Failed to attach database for column inference: %s",
		                               attach_result->GetError().c_str()));
	}

	elog(DEBUG2, "(DuckLake FDW) Probing schema: %s", probe_query.c_str());
	auto prepared = conn->context->Prepare(probe_query);

	if (prepared->HasError()) {
		/* Clean up probe attachment */
		try { conn->Query(detach_query); } catch (...) {}

		auto error_msg = duckdb::StringUtil::Format(
		    "Cannot create foreign table: DuckLake table \"%s.%s\" in database \"%s\" is not accessible.\n"
		    "HINT: Verify that:\n"
		    "  1. The table exists in the DuckLake catalog\n"
		    "  2. The schema_name and table_name options are correct\n"
		    "  3. The metadata_schema option points to the correct schema (default: 'ducklake')\n"
		    "  4. You have permission to access the table",
		    schema_name, table_name, dbname);

		throw duckdb::Exception(duckdb::ExceptionType::CATALOG, error_msg);
	}

	auto &result_types = prepared->GetTypes();
	auto &result_names = prepared->GetNames();

	for (size_t i = 0; i < result_types.size(); i++) {
		auto &col_name = result_names[i];
		auto &duckdb_type = result_types[i];

		Oid pg_type_oid = DuckdbLogicalTypeToPostgresOid(&duckdb_type, false);
		int32 typmod = DuckdbLogicalTypeToPostgresTypemod(&duckdb_type);

		ColumnDef *coldef = makeNode(ColumnDef);
		coldef->colname = pstrdup(col_name.c_str());
		coldef->inhcount = 0;
		coldef->is_local = true;
		coldef->is_not_null = false;
		coldef->is_from_type = false;
		coldef->storage = 0;
		coldef->raw_default = NULL;
		coldef->cooked_default = NULL;
		coldef->collClause = NULL;
		coldef->collOid = InvalidOid;
		coldef->constraints = NIL;
		coldef->fdwoptions = NIL;
		coldef->location = -1;
		coldef->typeName = makeTypeNameFromOid(pg_type_oid, typmod);
		stmt->base.tableElts = lappend(stmt->base.tableElts, coldef);

		elog(DEBUG2, "(DuckLake FDW) Added column: %s %s", coldef->colname,
		     format_type_with_typemod(pg_type_oid, typmod));
	}

	auto detach_result = conn->Query(detach_query);
	if (detach_result->HasError()) {
		elog(WARNING, "(DuckLake FDW) Failed to detach probe database: %s",
		     detach_result->GetError().c_str());
	}

	elog(DEBUG2, "Inferred %zu columns for foreign table from DuckLake metadata",
	     result_types.size());
}

static void
InferAndPopulateForeignTableColumns(CreateForeignTableStmt *stmt) {
	char *server_name = stmt->servername;
	ForeignServer *server = GetForeignServerByName(server_name, false);

	const char *schema_name = NULL;
	const char *table_name = NULL;
	ListCell *lc;
	foreach (lc, stmt->options) {
		DefElem *def = (DefElem *)lfirst(lc);
		if (strcmp(def->defname, "schema_name") == 0)
			schema_name = defGetString(def);
		else if (strcmp(def->defname, "table_name") == 0)
			table_name = defGetString(def);
	}

	if (!schema_name || !table_name) {
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("schema_name and table_name options are required")));
	}

	const char *metadata_schema = GetOptionValue(server->options, "metadata_schema", "ducklake");
	const char *dbname = GetOptionValue(server->options, "dbname", GetCurrentDatabaseName());
	const char *username = GetUserNameFromId(GetUserId(), false);
	InvokeCPPFunc(InferAndPopulateForeignTableColumns_Cpp, stmt, schema_name, table_name,
	              dbname, username, metadata_schema);
}

/* ----------------------------------------------------------------
 * Query tree walking — register foreign tables before DuckDB PREPARE
 * ---------------------------------------------------------------- */

static void RegisterForeignTablesInQueryExprs(Query *query);

/* Forward declaration */
extern "C" void ducklake_register_foreign_tables_in_query(Query *query);

extern "C" {
static bool
RegisterForeignTablesInExpr(Node *node, void *context) {
	if (!node)
		return false;

	if (IsA(node, SubLink)) {
		SubLink *sublink = (SubLink *)node;
		if (IsA(sublink->subselect, Query))
			ducklake_register_foreign_tables_in_query((Query *)sublink->subselect);
	}

#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, RegisterForeignTablesInExpr, context);
#else
	return expression_tree_walker(node, (bool (*)())((void *)RegisterForeignTablesInExpr), context);
#endif
}
} // extern "C"

static void
RegisterForeignTablesInQueryExprs(Query *query) {
	if (!query)
		return;

	if (query->jointree && query->jointree->quals)
		RegisterForeignTablesInExpr((Node *)query->jointree->quals, nullptr);

	if (query->havingQual)
		RegisterForeignTablesInExpr((Node *)query->havingQual, nullptr);

	ListCell *lc;
	foreach (lc, query->targetList) {
		TargetEntry *tle = (TargetEntry *)lfirst(lc);
		if (tle->expr)
			RegisterForeignTablesInExpr((Node *)tle->expr, nullptr);
	}

	if (query->limitCount)
		RegisterForeignTablesInExpr((Node *)query->limitCount, nullptr);
	if (query->limitOffset)
		RegisterForeignTablesInExpr((Node *)query->limitOffset, nullptr);

	foreach (lc, query->windowClause) {
		WindowClause *wc = (WindowClause *)lfirst(lc);
		if (wc->startOffset)
			RegisterForeignTablesInExpr((Node *)wc->startOffset, nullptr);
		if (wc->endOffset)
			RegisterForeignTablesInExpr((Node *)wc->endOffset, nullptr);
	}
}

/*
 * Public function called from the pre-prepare callback.
 * Registers all DuckLake foreign tables in the query with DuckDB.
 */
extern "C" void
ducklake_register_foreign_tables_in_query(Query *query) {
	if (!query)
		return;

	/* Enforce read-only: block DML on foreign tables */
	if (query->commandType == CMD_INSERT ||
	    query->commandType == CMD_UPDATE ||
	    query->commandType == CMD_DELETE) {
		ListCell *lc;
		foreach (lc, query->rtable) {
			RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
			if (rte->rtekind == RTE_RELATION) {
				Relation rel = table_open(rte->relid, NoLock);
				if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE &&
				    IsDucklakeForeignTableInternal(rte->relid)) {
					const char *relname = RelationGetRelationName(rel);
					table_close(rel, NoLock);
					ereport(ERROR,
					        (errcode(ERRCODE_FDW_ERROR),
					         errmsg("cannot %s foreign table \"%s\"",
					                query->commandType == CMD_INSERT ? "insert into" :
					                query->commandType == CMD_UPDATE ? "update" : "delete from",
					                relname),
					         errhint("DuckLake foreign tables are read-only")));
				}
				table_close(rel, NoLock);
			}
		}
	}

	ListCell *lc;
	foreach (lc, query->rtable) {
		RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);

		if (rte->rtekind == RTE_RELATION) {
			Relation rel = table_open(rte->relid, NoLock);
			if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE &&
			    IsDucklakeForeignTableInternal(rte->relid)) {
				RegisterDucklakeForeignTable(rte->relid);
			}
			table_close(rel, NoLock);
		} else if (rte->rtekind == RTE_SUBQUERY && rte->subquery) {
			ducklake_register_foreign_tables_in_query(rte->subquery);
		}
	}

	foreach (lc, query->cteList) {
		CommonTableExpr *cte = (CommonTableExpr *)lfirst(lc);
		if (IsA(cte->ctequery, Query))
			ducklake_register_foreign_tables_in_query((Query *)cte->ctequery);
	}

	RegisterForeignTablesInQueryExprs(query);
}

/* ----------------------------------------------------------------
 * Pre-prepare callback (registered with pg_duckdb)
 * ---------------------------------------------------------------- */

static void
DucklakePrePrepareCallback(const void *query_ptr) {
	ducklake_register_foreign_tables_in_query((Query *)query_ptr);
}

/* ----------------------------------------------------------------
 * ProcessUtility hook for CREATE FOREIGN TABLE column inference
 * ---------------------------------------------------------------- */

static ProcessUtility_hook_type prev_process_utility_hook = NULL;

static bool
IsDucklakeFdw(const char *server_name) {
	ForeignServer *server = GetForeignServerByName(server_name, true);
	if (!server)
		return false;
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);
	return (strcmp(fdw->fdwname, DUCKLAKE_FDW_NAME) == 0);
}

static void
DucklakeProcessUtilityHook(PlannedStmt *pstmt, const char *queryString,
                           bool readOnlyTree,
                           ProcessUtilityContext context,
                           ParamListInfo params,
                           QueryEnvironment *queryEnv,
                           DestReceiver *dest,
                           QueryCompletion *qc) {
	Node *parsetree = pstmt->utilityStmt;

	if (IsA(parsetree, CreateForeignTableStmt)) {
		CreateForeignTableStmt *stmt = (CreateForeignTableStmt *)parsetree;

		if (IsDucklakeFdw(stmt->servername)) {
			/* If columns are specified, error out */
			if (list_length(stmt->base.tableElts) != 0) {
				ereport(ERROR,
				        (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				         errmsg("cannot specify column definitions for DuckLake foreign table"),
				         errhint("Leave the column list empty - column definitions are automatically inferred")));
			}

			/* Auto-infer columns from DuckLake metadata */
			InferAndPopulateForeignTableColumns(stmt);
		}
	}

	/* Chain to the next hook */
	if (prev_process_utility_hook)
		prev_process_utility_hook(pstmt, queryString, readOnlyTree,
		                          context, params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
		                        context, params, queryEnv, dest, qc);
}

/* ----------------------------------------------------------------
 * FDW handler and validator (exported PG functions)
 * ---------------------------------------------------------------- */

static void
DucklakeFdwGetForeignRelSize(PlannerInfo * /*root*/, RelOptInfo *baserel, Oid /*foreigntableid*/) {
	baserel->rows = 1000.0;
	baserel->tuples = 1000.0;
}

static void
DucklakeFdwGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid /*foreigntableid*/) {
	Path *path;
	path = (Path *)create_foreignscan_path(root, baserel, NULL,
	                                       baserel->rows,
#if PG_VERSION_NUM >= 180000
	                                       0, /* disabled_nodes */
#endif
	                                       1,    /* startup_cost */
	                                       1,    /* total_cost */
	                                       NIL,  /* no pathkeys */
	                                       NULL, /* no required outer */
	                                       NULL, /* no fdw_outerpath */
#if PG_VERSION_NUM >= 170000
	                                       NIL, /* fdw_restrictinfo */
#endif
	                                       NIL); /* no fdw_private */
	add_path(baserel, path);
}

static ForeignScan *
DucklakeFdwGetForeignPlan(PlannerInfo * /*root*/, RelOptInfo *baserel, Oid /*foreigntableid*/,
                          ForeignPath * /*best_path*/, List *tlist, List * /*scan_clauses*/,
                          Plan *outer_plan) {
	return make_foreignscan(tlist, NIL, baserel->relid, NIL, NIL, NIL, NIL, outer_plan);
}

static void
DucklakeFdwBeginForeignScan(ForeignScanState * /*node*/, int /*eflags*/) {
	elog(ERROR, "DuckLake foreign table scan should not reach BeginForeignScan - "
	            "execution should go through DuckDB planner");
}

static TupleTableSlot *
DucklakeFdwIterateForeignScan(ForeignScanState * /*node*/) {
	elog(ERROR, "DuckLake foreign table scan should not reach IterateForeignScan - "
	            "execution should go through DuckDB planner");
	return nullptr;
}

static void
DucklakeFdwEndForeignScan(ForeignScanState * /*node*/) {
}

extern "C" {

PG_FUNCTION_INFO_V1(ducklake_fdw_handler);
Datum
ducklake_fdw_handler(PG_FUNCTION_ARGS) {
	(void)fcinfo;
	FdwRoutine *routine = makeNode(FdwRoutine);

	routine->GetForeignRelSize = DucklakeFdwGetForeignRelSize;
	routine->GetForeignPaths = DucklakeFdwGetForeignPaths;
	routine->GetForeignPlan = DucklakeFdwGetForeignPlan;

	routine->BeginForeignScan = DucklakeFdwBeginForeignScan;
	routine->IterateForeignScan = DucklakeFdwIterateForeignScan;
	routine->EndForeignScan = DucklakeFdwEndForeignScan;

	routine->AddForeignUpdateTargets = NULL;
	routine->PlanForeignModify = NULL;
	routine->BeginForeignModify = NULL;
	routine->ExecForeignInsert = NULL;
	routine->ExecForeignUpdate = NULL;
	routine->ExecForeignDelete = NULL;
	routine->EndForeignModify = NULL;

	PG_RETURN_POINTER(routine);
}

PG_FUNCTION_INFO_V1(ducklake_fdw_validator);
Datum
ducklake_fdw_validator(PG_FUNCTION_ARGS) {
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);

	if (catalog == ForeignDataWrapperRelationId)
		PG_RETURN_VOID();

	if (catalog != ForeignServerRelationId && catalog != ForeignTableRelationId) {
		ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
		                errmsg("ducklake_fdw only supports SERVER and FOREIGN TABLE objects")));
	}

	ListCell *lc_v;
	foreach (lc_v, options_list) {
		DefElem *def = lfirst_node(DefElem, lc_v);
		if (!IsValidDucklakeFdwOption(def->defname, catalog)) {
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			                errmsg("invalid option \"%s\"", def->defname)));
		}
	}

	ValidateRequiredOptions(options_list, catalog);

	PG_RETURN_VOID();
}

} // extern "C"

/* ----------------------------------------------------------------
 * Initialization — called from _PG_init()
 * ---------------------------------------------------------------- */

/* pg_duckdb hook registration functions */
extern "C" {
typedef bool (*DuckdbExternalTableCheck)(Oid relid);
void RegisterDuckdbExternalTableCheck(DuckdbExternalTableCheck callback);

typedef void (*DuckdbPrePrepareCallback_t)(const void *query);
void RegisterDuckdbPrePrepareCallback(DuckdbPrePrepareCallback_t callback);

typedef char *(*DuckdbRelationNameCallback)(Oid relid);
void RegisterDuckdbRelationNameCallback(DuckdbRelationNameCallback callback);

typedef bool (*DuckdbForcedAliasCheck)(Oid relid);
void RegisterDuckdbForcedAliasCheck(DuckdbForcedAliasCheck callback);
}

extern "C" void
ducklake_fdw_init(void) {
	/* Register external table check: makes IsDuckdbTable() return true for FDW tables */
	RegisterDuckdbExternalTableCheck(IsDucklakeForeignTable);

	/* Register pre-prepare callback: ATTACHes foreign databases before query prep */
	RegisterDuckdbPrePrepareCallback(DucklakePrePrepareCallback);

	/* Register relation name callback: provides DuckDB-qualified names for FDW tables */
	RegisterDuckdbRelationNameCallback(GetDucklakeForeignTableName);

	/* Register forced alias check: ensures alias is printed for FDW tables */
	RegisterDuckdbForcedAliasCheck(IsDucklakeForeignTable);

	/* Install ProcessUtility hook for CREATE FOREIGN TABLE column inference */
	prev_process_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = DucklakeProcessUtilityHook;
}
