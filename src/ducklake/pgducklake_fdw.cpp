/*
 * pgducklake_fdw.cpp
 *
 * Foreign Data Wrapper for DuckLake tables
 *
 * This FDW enables read-only access to DuckLake tables from other databases.
 * Unlike traditional FDWs, this doesn't implement a full scan path. Instead,
 * it registers tables with DuckDB and leverages the existing pg_ducklake
 * query execution infrastructure.
 */

#include "duckdb.hpp"

#include "pgduckdb/pgduckdb_utils.hpp"

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
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "nodes/nodeFuncs.h"

#include "pgduckdb/vendor/pg_list.hpp"
}

#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/ducklake/pgducklake_ddl.hpp"
#include "pgduckdb/ducklake/pgducklake_metadata_manager.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

// Forward declarations to avoid including cpp_only headers
namespace pgduckdb {
bool IsExtensionRegistered();
Oid GetPostgresDuckDBType(const duckdb::LogicalType &type, bool throw_error = false);
int32_t GetPostgresDuckDBTypemod(const duckdb::LogicalType &type);
} // namespace pgduckdb

namespace pgduckdb {

/* Constants for FDW implementation */
constexpr const char *FDW_PROBE_ATTACH_NAME = "fdw_probe_ddl";
constexpr double DEFAULT_FOREIGN_TABLE_ROWS = 1000.0;

struct DucklakeFdwOption {
	const char *optname;
	Oid context;
	bool required;
};

static const struct DucklakeFdwOption valid_server_options[] = {
    {"dbname", ForeignServerRelationId, false},          // Target database name
    {"metadata_schema", ForeignServerRelationId, false}, // DuckLake metadata schema (default: ducklake)
    {NULL, InvalidOid, false}                            // Sentinel
};

static const struct DucklakeFdwOption valid_table_options[] = {
    {"schema_name", ForeignTableRelationId, true}, // DuckLake schema name
    {"table_name", ForeignTableRelationId, true},  // DuckLake table name
    {NULL, InvalidOid, false}                      // Sentinel
};

static bool
IsValidDucklakeFdwOption(const char *optname, Oid context) {
	const struct DucklakeFdwOption *options = NULL;

	if (context == ForeignServerRelationId) {
		options = valid_server_options;
	} else if (context == ForeignTableRelationId) {
		options = valid_table_options;
	} else {
		return false;
	}

	for (const struct DucklakeFdwOption *opt = options; opt->optname != NULL; ++opt) {
		if (strcmp(optname, opt->optname) == 0) {
			return true;
		}
	}
	return false;
}

static void
ValidateRequiredOptions(List *options_list, Oid context) {
	const struct DucklakeFdwOption *options = NULL;

	if (context == ForeignServerRelationId) {
		options = valid_server_options;
	} else if (context == ForeignTableRelationId) {
		options = valid_table_options;
	} else {
		return;
	}

	for (const struct DucklakeFdwOption *opt = options; opt->optname != NULL; ++opt) {
		if (!opt->required) {
			continue;
		}

		bool found = false;
		foreach_node(DefElem, def, options_list) {
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
	foreach_node(DefElem, def, options) {
		if (strcmp(def->defname, optname) == 0) {
			return defGetString(def);
		}
	}
	return default_value;
}

/*
 * Get the current database name. Error out if the database name cannot be determined.
 */
static const char *
GetCurrentDatabaseName(void) {
	const char *dbname = get_database_name(MyDatabaseId);
	if (!dbname) {
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("could not determine current database name")));
	}
	return dbname;
}

/*
 * Escape a string value for use in a DuckDB SQL string literal.
 * This doubles single quotes to prevent SQL injection.
 */
static std::string
EscapeDuckDBStringLiteral(const char *value) {
	return duckdb::StringUtil::Replace(std::string(value), "'", "''");
}

static void
RegisterDucklakeForeignTable_Cpp(const char *dbname, const char *username, const char *metadata_schema,
                                 const char *attach_as) {
	/* Escape string values to prevent SQL injection */
	auto escaped_dbname = EscapeDuckDBStringLiteral(dbname);
	auto escaped_username = EscapeDuckDBStringLiteral(username);
	auto escaped_metadata_schema = EscapeDuckDBStringLiteral(metadata_schema);

	auto attach_query = duckdb::StringUtil::Format(
	    "ATTACH IF NOT EXISTS 'postgres:dbname=%s user=%s' AS %s (TYPE DUCKLAKE, METADATA_SCHEMA '%s')",
	    escaped_dbname.c_str(), escaped_username.c_str(), attach_as, escaped_metadata_schema.c_str());

	elog(DEBUG1, "(DuckLake FDW) Attaching database as '%s': %s", attach_as, attach_query.c_str());
	pgduckdb::DuckDBQueryOrThrow(attach_query);
}

void
RegisterDucklakeForeignTable(Oid foreign_table_oid) {
	/* Extract data before C++ context to avoid elog(ERROR) with C++ objects on stack */
	ForeignTable *ft = GetForeignTable(foreign_table_oid);
	ForeignServer *server = GetForeignServer(ft->serverid);

	const char *metadata_schema = GetOptionValue(server->options, "metadata_schema");
	const char *dbname = GetOptionValue(server->options, "dbname");

	if (!metadata_schema) {
		metadata_schema = "ducklake";
	}

	if (!dbname) {
		dbname = GetCurrentDatabaseName();
	}

	const char *username = GetUserNameFromId(GetUserId(), false);
	auto attach_as = psprintf("fdw_db_%s", dbname);
	InvokeCPPFunc(RegisterDucklakeForeignTable_Cpp, dbname, username, metadata_schema, attach_as);
}

static void
InferAndPopulateForeignTableColumns_Cpp(CreateForeignTableStmt *stmt, const char *schema_name, const char *table_name,
                                        const char *dbname, const char *username, const char *metadata_schema) {
	const char *attach_name = FDW_PROBE_ATTACH_NAME;
	auto probe_query = duckdb::StringUtil::Format("SELECT * FROM %s.%s.%s LIMIT 0", attach_name,
	                                              quote_identifier(schema_name), quote_identifier(table_name));
	auto detach_query = duckdb::StringUtil::Format("DETACH DATABASE IF EXISTS %s", attach_name);

	RegisterDucklakeForeignTable_Cpp(dbname, username, metadata_schema, attach_name);
	elog(DEBUG2, "(DuckLake FDW) Probing schema: %s", probe_query.c_str());
	auto conn = pgduckdb::DuckDBManager::GetConnection();
	auto prepared = conn->context->Prepare(probe_query);

	if (prepared->HasError()) {
		// Try to clean up the probe attachment before throwing error
		try {
			conn->Query(detach_query);
		} catch (const duckdb::Exception &ex) {
			elog(WARNING, "(DuckLake FDW) Failed to detach probe database: %s", ex.what());
		} catch (const std::exception &ex) {
			elog(WARNING, "(DuckLake FDW) Failed to detach probe database: %s", ex.what());
		}

		// Provide a helpful error message with context
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

		Oid pg_type_oid = pgduckdb::GetPostgresDuckDBType(duckdb_type);
		int32 typmod = pgduckdb::GetPostgresDuckDBTypemod(duckdb_type);

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
		elog(WARNING, "(DuckLake FDW) Failed to detach probe database: %s", detach_result->GetError().c_str());
	}

	elog(DEBUG2, "Automatically inferred %zu columns for foreign table from DuckLake metadata", result_types.size());
}

void
InferAndPopulateForeignTableColumns(CreateForeignTableStmt *stmt) {
	char *server_name = stmt->servername;
	ForeignServer *server = GetForeignServerByName(server_name, false);

	const char *schema_name = NULL;
	const char *table_name = NULL;
	ListCell *lc;
	foreach (lc, stmt->options) {
		DefElem *def = (DefElem *)lfirst(lc);
		if (strcmp(def->defname, "schema_name") == 0) {
			schema_name = defGetString(def);
		} else if (strcmp(def->defname, "table_name") == 0) {
			table_name = defGetString(def);
		}
	}

	if (!schema_name || !table_name) {
		ereport(ERROR,
		        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("schema_name and table_name options are required")));
	}

	const char *metadata_schema = GetOptionValue(server->options, "metadata_schema", "ducklake");
	const char *dbname = GetOptionValue(server->options, "dbname", GetCurrentDatabaseName());
	const char *username = GetUserNameFromId(GetUserId(), false);
	InvokeCPPFunc(InferAndPopulateForeignTableColumns_Cpp, stmt, schema_name, table_name, dbname, username,
	              metadata_schema);
}

/*
 * Check if a relation is a DuckLake foreign table by its OID.
 * This version is optimized when we already know the relkind is RELKIND_FOREIGN_TABLE.
 */
static bool
IsDucklakeForeignTableInternal(Oid relid) {
	ForeignTable *ft = GetForeignTable(relid);
	ForeignServer *server = GetForeignServer(ft->serverid);
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

	return (strcmp(fdw->fdwname, DUCKLAKE_FDW_NAME) == 0);
}

bool
IsDucklakeForeignTable(Oid relid) {
	Relation rel = table_open(relid, NoLock);
	bool result = false;

	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
		result = IsDucklakeForeignTableInternal(relid);
	}

	table_close(rel, NoLock);
	return result;
}

/*
 * Get the qualified DuckDB name for a DuckLake foreign table
 */
char *
GetDucklakeForeignTableName(Oid foreign_table_oid) {
	ForeignTable *ft = GetForeignTable(foreign_table_oid);
	ForeignServer *server = GetForeignServer(ft->serverid);

	const char *schema_name = GetOptionValue(ft->options, "schema_name");
	const char *table_name = GetOptionValue(ft->options, "table_name");
	const char *dbname = GetOptionValue(server->options, "dbname", GetCurrentDatabaseName());

	return psprintf("fdw_db_%s.%s.%s", dbname, quote_identifier(schema_name), quote_identifier(table_name));
}

void
DucklakeFdwGetForeignRelSize(PlannerInfo * /*root*/, RelOptInfo *baserel, Oid /*foreigntableid*/) {
	baserel->rows = DEFAULT_FOREIGN_TABLE_ROWS;
	baserel->tuples = DEFAULT_FOREIGN_TABLE_ROWS;
}

/*
 * Dummy path to let UPDATE/DELETE pass planning and fail properly in executor
 * SELECT queries are handled by planner hook, not this path
 */
void
DucklakeFdwGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid /*foreigntableid*/) {
	Path *path;
#if PG_VERSION_NUM >= 180000
	path = (Path *)create_foreignscan_path(root, baserel, NULL, /* default pathtarget */
	                                       baserel->rows,       /* rows */
	                                       0,                   /* disabled_nodes */
	                                       1,                   /* startup_cost (dummy) */
	                                       1,                   /* total_cost (dummy) */
	                                       NIL,                 /* no pathkeys */
	                                       NULL,                /* no required outer */
	                                       NULL,                /* no fdw_outerpath */
	                                       NIL,                 /* fdw_restrictinfo */
	                                       NIL);                /* no fdw_private */
#else
	path = (Path *)create_foreignscan_path(root, baserel, NULL, /* default pathtarget */
	                                       baserel->rows,       /* rows */
	                                       1,                   /* startup_cost (dummy) */
	                                       1,                   /* total_cost (dummy) */
	                                       NIL,                 /* no pathkeys */
	                                       NULL,                /* no required outer */
	                                       NULL,                /* no fdw_outerpath */
	                                       NIL,                 /* fdw_restrictinfo */
	                                       NIL);                /* no fdw_private */
#endif
	add_path(baserel, path);
}

ForeignScan *
DucklakeFdwGetForeignPlan(PlannerInfo * /*root*/, RelOptInfo *baserel, Oid /*foreigntableid*/,
                          ForeignPath * /*best_path*/, List *tlist, List * /*scan_clauses*/, Plan *outer_plan) {
	return make_foreignscan(tlist, NIL, baserel->relid, NIL, /* fdw_exprs */
	                        NIL,                             /* fdw_private */
	                        NIL,                             /* fdw_scan_tlist */
	                        NIL,                             /* fdw_recheck_quals */
	                        outer_plan);
}

void
DucklakeFdwBeginForeignScan(ForeignScanState * /*node*/, int /*eflags*/) {
	elog(ERROR, "DuckLake foreign table scan should not reach BeginForeignScan - "
	            "execution should go through DuckDB planner");
}

TupleTableSlot *
DucklakeFdwIterateForeignScan(ForeignScanState * /*node*/) {
	elog(ERROR, "DuckLake foreign table scan should not reach IterateForeignScan - "
	            "execution should go through DuckDB planner");
	return nullptr;
}

void
DucklakeFdwEndForeignScan(ForeignScanState * /*node*/) {
}

/*
 * Walk through join conditions, WHERE clauses, and other expressions to find subqueries.
 * This function must be defined before RegisterForeignTablesInQueryExprs which calls it.
 */
static void RegisterForeignTablesInQueryExprs(Query *query);

/* Forward declaration - defined later in this file */
void RegisterForeignTablesInQuery(Query *query);

/*
 * Tree walker callback for finding subqueries in expression trees.
 * Must have C linkage because expression_tree_walker is a C function that
 * stores and calls this callback - without extern "C", C++ name mangling
 * would cause linker errors.
 */
extern "C" {
static bool
RegisterForeignTablesInExpr(Node *node, void *context) {
	if (!node) {
		return false;
	}

	if (IsA(node, SubLink)) {
		SubLink *sublink = (SubLink *)node;
		if (IsA(sublink->subselect, Query)) {
			pgduckdb::RegisterForeignTablesInQuery((Query *)sublink->subselect);
		}
	}

	/* Continue traversing the expression tree */
#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, RegisterForeignTablesInExpr, context);
#else
	return expression_tree_walker(node, (bool (*)())((void *)RegisterForeignTablesInExpr), context);
#endif
}
} // extern "C"

/*
 * Walk through join conditions, WHERE clauses, and other expressions to find subqueries
 */
static void
RegisterForeignTablesInQueryExprs(Query *query) {
	if (!query) {
		return;
	}

	/* Check jointree conditions (FROM clause joins and WHERE clause) */
	if (query->jointree && query->jointree->quals) {
		RegisterForeignTablesInExpr((Node *)query->jointree->quals, nullptr);
	}

	/* Check HAVING clause */
	if (query->havingQual) {
		RegisterForeignTablesInExpr((Node *)query->havingQual, nullptr);
	}

	/* Check targetList (SELECT clause) for subqueries */
	ListCell *lc;
	foreach (lc, query->targetList) {
		TargetEntry *tle = (TargetEntry *)lfirst(lc);
		if (tle->expr) {
			RegisterForeignTablesInExpr((Node *)tle->expr, nullptr);
		}
	}

	/* Check limitCount and limitOffset (LIMIT/OFFSET) */
	if (query->limitCount) {
		RegisterForeignTablesInExpr((Node *)query->limitCount, nullptr);
	}
	if (query->limitOffset) {
		RegisterForeignTablesInExpr((Node *)query->limitOffset, nullptr);
	}

	/* Check windowClause (WINDOW functions) */
	foreach (lc, query->windowClause) {
		WindowClause *wc = (WindowClause *)lfirst(lc);
		if (wc->startOffset) {
			RegisterForeignTablesInExpr((Node *)wc->startOffset, nullptr);
		}
		if (wc->endOffset) {
			RegisterForeignTablesInExpr((Node *)wc->endOffset, nullptr);
		}
	}
}

/*
 * RegisterForeignTablesInQuery - Register any DuckLake foreign tables in the query
 * with the current DuckDB session before preparing the query
 *
 * This function walks through:
 * 1. Range table entries (tables/views in FROM clause)
 * 2. CTEs (WITH clauses)
 * 3. Subqueries in WHERE, HAVING, SELECT, and other expressions
 */
void
RegisterForeignTablesInQuery(Query *query) {
	if (!query) {
		return;
	}

	/* Walk through all range table entries */
	ListCell *lc;
	foreach (lc, query->rtable) {
		RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);

		if (rte->rtekind == RTE_RELATION) {
			Relation rel = table_open(rte->relid, NoLock);

			/* Use IsDucklakeForeignTableInternal since we already checked relkind */
			if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE && IsDucklakeForeignTableInternal(rte->relid)) {
				/* Register this foreign table with DuckDB */
				RegisterDucklakeForeignTable(rte->relid);
			}

			table_close(rel, NoLock);
		} else if (rte->rtekind == RTE_SUBQUERY && rte->subquery) {
			/* Handle subqueries in FROM clause */
			RegisterForeignTablesInQuery(rte->subquery);
		}
	}

	/* Recursively handle CTEs */
	foreach (lc, query->cteList) {
		CommonTableExpr *cte = (CommonTableExpr *)lfirst(lc);
		if (IsA(cte->ctequery, Query)) {
			RegisterForeignTablesInQuery((Query *)cte->ctequery);
		}
	}

	/* Handle subqueries in expressions (WHERE, HAVING, SELECT, etc.) */
	RegisterForeignTablesInQueryExprs(query);
}

} // namespace pgduckdb

extern "C" {

PG_FUNCTION_INFO_V1(ducklake_fdw_handler);
Datum
ducklake_fdw_handler(PG_FUNCTION_ARGS) {
	(void)fcinfo;
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Planner functions */
	routine->GetForeignRelSize = pgduckdb::DucklakeFdwGetForeignRelSize;
	routine->GetForeignPaths = pgduckdb::DucklakeFdwGetForeignPaths;
	routine->GetForeignPlan = pgduckdb::DucklakeFdwGetForeignPlan;

	/* Executor functions */
	routine->BeginForeignScan = pgduckdb::DucklakeFdwBeginForeignScan;
	routine->IterateForeignScan = pgduckdb::DucklakeFdwIterateForeignScan;
	routine->EndForeignScan = pgduckdb::DucklakeFdwEndForeignScan;

	/* No support for writes */
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

	if (catalog == ForeignDataWrapperRelationId) {
		PG_RETURN_VOID();
	}

	if (catalog != ForeignServerRelationId && catalog != ForeignTableRelationId) {
		ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
		                errmsg("ducklake_fdw only supports SERVER and FOREIGN TABLE objects")));
	}

	foreach_node(DefElem, def, options_list) {
		if (!pgduckdb::IsValidDucklakeFdwOption(def->defname, catalog)) {
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME), errmsg("invalid option \"%s\"", def->defname)));
		}
	}

	pgduckdb::ValidateRequiredOptions(options_list, catalog);

	PG_RETURN_VOID();
}

} // extern "C"
