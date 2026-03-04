/*
 * pgducklake_fdw.cpp — Foreign Data Wrapper for DuckLake tables
 *
 * Implements a PostgreSQL FDW that provides read-only access to DuckLake
 * tables.  Supports two modes:
 *
 *   1. Regular FDW — references a DuckLake catalog backed by a PostgreSQL
 *      metadata database (options: dbname, metadata_schema).
 *   2. Frozen FDW — references a static .ducklake snapshot file hosted over
 *      HTTP/HTTPS (option: frozen_url).
 *
 * Queries are not executed through the FDW scan callbacks.  Instead, the
 * FDW registers itself with pg_duckdb's external-table-check hook so that
 * the planner routes the entire query to DuckDB, and a relation-name
 * callback supplies the DuckDB-qualified table name.
 *
 * Column inference:  CREATE FOREIGN TABLE without column definitions
 * automatically probes the remote schema via a temporary DuckDB connection
 * and populates columns from the prepared-statement result types.
 */

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement.hpp"

/* Forward-declare pg_duckdb type-mapping functions (from pgduckdb_types.hpp,
 * which cannot be included directly due to its cpp_only_file guard). */
namespace pgduckdb {
unsigned int GetPostgresDuckDBType(const duckdb::LogicalType &type,
                                   bool throw_error = false);
int32_t GetPostgresDuckDBTypemod(const duckdb::LogicalType &type);
} // namespace pgduckdb

#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_fdw.hpp"
#include "pgducklake/utility/cpp_wrapper.hpp"

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

#include "pgduckdb/pgduckdb_contracts.h"
}

/* ----------------------------------------------------------------
 * Option definitions
 * ---------------------------------------------------------------- */

struct DucklakeFdwOption {
  const char *optname;
  Oid context;
};

static const DucklakeFdwOption valid_server_options[] = {
    {"dbname", ForeignServerRelationId},
    {"metadata_schema", ForeignServerRelationId},
    {"frozen_url", ForeignServerRelationId},
    {nullptr, InvalidOid}};

static const DucklakeFdwOption valid_table_options[] = {
    {"schema_name", ForeignTableRelationId},
    {"table_name", ForeignTableRelationId},
    {nullptr, InvalidOid}};

static bool IsValidOption(const char *name, Oid context) {
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

static const char *GetOptionValue(List *options, const char *name) {
  ListCell *lc;
  foreach (lc, options) {
    DefElem *def = (DefElem *)lfirst(lc);
    if (strcmp(def->defname, name) == 0)
      return defGetString(def);
  }
  return nullptr;
}

/* ----------------------------------------------------------------
 * FDW name resolution helpers
 * ---------------------------------------------------------------- */

static const char *FDW_NAME = "ducklake_fdw";

static Oid GetDucklakeFdwOid() {
  static Oid cached_oid = InvalidOid;
  if (cached_oid != InvalidOid)
    return cached_oid;
  HeapTuple tup = SearchSysCache1(
      FOREIGNDATAWRAPPERNAME, CStringGetDatum(FDW_NAME));
  if (!HeapTupleIsValid(tup))
    return InvalidOid;
  Form_pg_foreign_data_wrapper fdw =
      (Form_pg_foreign_data_wrapper)GETSTRUCT(tup);
  cached_oid = fdw->oid;
  ReleaseSysCache(tup);
  return cached_oid;
}

static bool IsFrozenServer(ForeignServer *server) {
  return GetOptionValue(server->options, "frozen_url") != nullptr;
}

/* ----------------------------------------------------------------
 * External table check callback (registered with pg_duckdb)
 * ---------------------------------------------------------------- */

static bool IsDucklakeForeignTable(Oid relid) {
  if (get_rel_relkind(relid) != RELKIND_FOREIGN_TABLE)
    return false;
  ForeignTable *ft = GetForeignTable(relid);
  ForeignServer *server = GetForeignServer(ft->serverid);
  return server->fdwid == GetDucklakeFdwOid();
}

/* ----------------------------------------------------------------
 * Database ATTACH / DETACH lifecycle
 * ---------------------------------------------------------------- */

static duckdb::string GetDatabaseAlias(ForeignServer *server) {
  if (IsFrozenServer(server))
    return server->servername;
  const char *dbname = GetOptionValue(server->options, "dbname");
  if (!dbname)
    dbname = get_database_name(MyDatabaseId);
  return duckdb::StringUtil::Format("fdw_db_%s", dbname);
}

static duckdb::string BuildAttachQuery(ForeignServer *server,
                                       const char *db_alias,
                                       bool if_not_exists) {
  const char *exists_clause = if_not_exists ? " IF NOT EXISTS" : "";

  if (IsFrozenServer(server)) {
    const char *url = GetOptionValue(server->options, "frozen_url");
    return duckdb::StringUtil::Format(
        "ATTACH%s 'ducklake:%s' AS \"%s\"", exists_clause, url, db_alias);
  }

  const char *dbname = GetOptionValue(server->options, "dbname");
  const char *schema = GetOptionValue(server->options, "metadata_schema");
  if (!dbname)
    dbname = get_database_name(MyDatabaseId);
  if (!schema)
    schema = "ducklake";

  const char *user = GetUserNameFromId(GetUserId(), false);
  return duckdb::StringUtil::Format(
      "ATTACH%s 'postgres:dbname=%s user=%s' "
      "AS \"%s\" (TYPE DUCKLAKE, METADATA_SCHEMA '%s')",
      exists_clause, dbname, user, db_alias, schema);
}

/* ----------------------------------------------------------------
 * Relation name callback (registered with pg_duckdb)
 *
 * Returns the DuckDB-qualified name for a ducklake FDW table:
 *   Regular: fdw_db_<dbname>.<schema>.<table>
 *   Frozen:  <servername>.<schema>.<table>
 * ---------------------------------------------------------------- */

static char *GetDucklakeForeignTableName(Oid relid) {
  if (get_rel_relkind(relid) != RELKIND_FOREIGN_TABLE)
    return nullptr;

  ForeignTable *ft = GetForeignTable(relid);
  ForeignServer *server = GetForeignServer(ft->serverid);
  if (server->fdwid != GetDucklakeFdwOid())
    return nullptr;

  const char *schema_name =
      GetOptionValue(ft->options, "schema_name");
  if (!schema_name)
    schema_name = "public";

  const char *table_name =
      GetOptionValue(ft->options, "table_name");
  if (!table_name)
    table_name = get_rel_name(relid);

  duckdb::string db_alias = GetDatabaseAlias(server);

  return psprintf("%s.%s.%s", quote_identifier(db_alias.c_str()),
                  quote_identifier(schema_name),
                  quote_identifier(table_name));
}

static void AttachDucklakeDatabase(ForeignServer *server) {
  duckdb::string db_alias = GetDatabaseAlias(server);
  duckdb::string query = BuildAttachQuery(server, db_alias.c_str(), true);

  elog(DEBUG1, "ducklake_fdw: %s", query.c_str());

  const char *errmsg;
  int ret = pgducklake::ExecuteDuckDBQuery(query.c_str(), &errmsg);
  if (ret != 0)
    elog(ERROR, "ducklake_fdw: ATTACH failed: %s", errmsg);
}

/* ----------------------------------------------------------------
 * Query-tree walker: ATTACH databases and block DML
 * ---------------------------------------------------------------- */

static void RegisterForeignTablesInQuery(Query *query) {
  if (!query || !query->rtable)
    return;

  Oid fdw_oid = GetDucklakeFdwOid();
  if (fdw_oid == InvalidOid)
    return;

  ListCell *lc;
  foreach (lc, query->rtable) {
    RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
    if (rte->rtekind == RTE_SUBQUERY && rte->subquery)
      RegisterForeignTablesInQuery(rte->subquery);

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

  /* Walk CTEs */
  foreach (lc, query->cteList) {
    CommonTableExpr *cte = (CommonTableExpr *)lfirst(lc);
    if (IsA(cte->ctequery, Query))
      RegisterForeignTablesInQuery(castNode(Query, cte->ctequery));
  }

  /* Block DML on FDW tables */
  if (query->commandType != CMD_SELECT && query->resultRelation > 0) {
    RangeTblEntry *result_rte =
        list_nth_node(RangeTblEntry, query->rtable,
                      query->resultRelation - 1);
    if (result_rte->relid != InvalidOid &&
        IsDucklakeForeignTable(result_rte->relid)) {
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
              (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errmsg("ducklake_fdw tables are read-only"),
               errhint("%s is not supported on foreign tables "
                       "created with ducklake_fdw.",
                       op)));
    }
  }
}

/* ----------------------------------------------------------------
 * Column inference: probe remote schema via temporary DuckDB connection
 * ---------------------------------------------------------------- */

static List *InferForeignTableColumns(CreateForeignTableStmt *stmt) {
  /* Ensure DuckDB is initialized (triggers pg_duckdb startup via SPI) */
  if (!DuckdbIsInitialized()) {
    const char *errmsg;
    pgducklake::ExecuteDuckDBQuery("SELECT 1", &errmsg);
  }

  duckdb::DuckDB *db = ducklake_get_duckdb_database();
  if (!db)
    elog(ERROR, "ducklake_fdw: DuckDB not initialized");

  /* Resolve server options */
  const char *server_name = stmt->servername;
  ForeignServer *server = GetForeignServerByName(server_name, false);

  const char *schema_name =
      GetOptionValue(stmt->options, "schema_name");
  if (!schema_name)
    schema_name = "public";

  const char *table_name =
      GetOptionValue(stmt->options, "table_name");
  if (!table_name)
    table_name = stmt->base.relation->relname;

  /* Build ATTACH + PREPARE via a temporary connection */
  duckdb::Connection conn(*db);

  const char *probe_db = "__ducklake_fdw_probe__";
  duckdb::string attach_query = BuildAttachQuery(server, probe_db, false);

  auto attach_result = conn.Query(attach_query);
  if (attach_result->HasError()) {
    elog(ERROR, "ducklake_fdw: column inference ATTACH failed: %s",
         attach_result->GetError().c_str());
  }

  duckdb::string select_query = duckdb::StringUtil::Format(
      "SELECT * FROM \"%s\".%s.%s LIMIT 0", probe_db,
      duckdb::KeywordHelper::WriteOptionallyQuoted(schema_name).c_str(),
      duckdb::KeywordHelper::WriteOptionallyQuoted(table_name).c_str());

  auto prepared = conn.Prepare(select_query);
  if (prepared->HasError()) {
    conn.Query(duckdb::StringUtil::Format("DETACH \"%s\"", probe_db));
    elog(ERROR, "ducklake_fdw: cannot read table \"%s\".\"%s\": %s",
         schema_name, table_name, prepared->error.Message().c_str());
  }

  /* Build ColumnDef list from prepared statement types */
  List *columns = NIL;
  auto &names = prepared->GetNames();
  auto &types = prepared->GetTypes();

  for (size_t i = 0; i < names.size(); i++) {
    Oid pg_type = pgduckdb::GetPostgresDuckDBType(types[i]);
    int32_t typemod = pgduckdb::GetPostgresDuckDBTypemod(types[i]);

    ColumnDef *col = makeColumnDef(names[i].c_str(), pg_type, typemod,
                                   InvalidOid);
    columns = lappend(columns, col);
  }

  /* Cleanup */
  conn.Query(duckdb::StringUtil::Format("DETACH \"%s\"", probe_db));

  return columns;
}

/* ----------------------------------------------------------------
 * ProcessUtility hook: intercept CREATE FOREIGN TABLE for column inference
 * ---------------------------------------------------------------- */

static ProcessUtility_hook_type prev_fdw_process_utility_hook = NULL;

static void DucklakeFdwUtilityHook(PlannedStmt *pstmt,
                                   const char *query_string,
                                   bool read_only_tree,
                                   ProcessUtilityContext context,
                                   ParamListInfo params,
                                   struct QueryEnvironment *query_env,
                                   DestReceiver *dest,
                                   QueryCompletion *qc) {
  if (pstmt->utilityStmt &&
      IsA(pstmt->utilityStmt, CreateForeignTableStmt)) {
    auto *cft =
        castNode(CreateForeignTableStmt, pstmt->utilityStmt);

    /* Check if this is for our FDW */
    ForeignDataWrapper *fdw = GetForeignDataWrapperByName(FDW_NAME, true);
    if (fdw) {
      ForeignServer *server =
          GetForeignServerByName(cft->servername, true);
      if (server && server->fdwid == fdw->fdwid) {
        if (cft->base.tableElts != NIL) {
          ereport(ERROR,
                  (errcode(ERRCODE_SYNTAX_ERROR),
                   errmsg("ducklake_fdw foreign tables do not "
                          "accept column definitions"),
                   errhint("Omit the column list; columns are "
                           "automatically inferred from the "
                           "remote DuckLake table.")));
        }

        /* Need a mutable copy for column inference */
        if (read_only_tree) {
          pstmt = (PlannedStmt *)copyObjectImpl(pstmt);
          cft = castNode(CreateForeignTableStmt, pstmt->utilityStmt);
          read_only_tree = false;
        }

        cft->base.tableElts = InferForeignTableColumns(cft);
      }
    }
  }

  prev_fdw_process_utility_hook(pstmt, query_string, read_only_tree,
                                context, params, query_env, dest, qc);
}

/* ----------------------------------------------------------------
 * FDW handler: minimal callbacks (execution goes through DuckDB)
 * ---------------------------------------------------------------- */

static void DucklakeGetForeignRelSize(PlannerInfo *root,
                                      RelOptInfo *baserel,
                                      Oid foreigntableid) {
  baserel->rows = 1000;
}

static void DucklakeGetForeignPaths(PlannerInfo *root,
                                    RelOptInfo *baserel,
                                    Oid foreigntableid) {
  add_path(baserel,
           (Path *)create_foreignscan_path(root, baserel, NULL,
                                           baserel->rows, 10, 1000,
                                           NIL, NULL, NULL, NIL, NIL));
}

static ForeignScan *DucklakeGetForeignPlan(PlannerInfo *root,
                                           RelOptInfo *baserel,
                                           Oid foreigntableid,
                                           ForeignPath *best_path,
                                           List *tlist, List *scan_clauses,
                                           Plan *outer_plan) {
  scan_clauses = extract_actual_clauses(scan_clauses, false);
  return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, NIL,
                          NIL, NIL, outer_plan);
}

static void DucklakeBeginForeignScan(ForeignScanState *node, int eflags) {
  elog(ERROR, "ducklake_fdw: direct scan should not be reached; "
             "query should be routed through DuckDB");
}

static TupleTableSlot *DucklakeIterateForeignScan(ForeignScanState *node) {
  elog(ERROR, "ducklake_fdw: direct scan should not be reached; "
             "query should be routed through DuckDB");
  return nullptr;
}

static void DucklakeReScanForeignScan(ForeignScanState *node) {
  elog(ERROR, "ducklake_fdw: direct scan should not be reached; "
             "query should be routed through DuckDB");
}

static void DucklakeEndForeignScan(ForeignScanState *node) {
  /* nothing to clean up */
}

/* ----------------------------------------------------------------
 * PG_FUNCTION exports: handler + validator
 * ---------------------------------------------------------------- */

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
  PG_RETURN_POINTER(routine);
}

DECLARE_PG_FUNCTION(ducklake_fdw_validator) {
  List *options = untransformRelOptions(PG_GETARG_DATUM(0));
  Oid catalog = PG_GETARG_OID(1);

  ListCell *lc;
  foreach (lc, options) {
    DefElem *def = (DefElem *)lfirst(lc);
    if (!IsValidOption(def->defname, catalog)) {
      ereport(ERROR,
              (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
               errmsg("invalid ducklake_fdw option \"%s\"",
                      def->defname)));
    }
  }

  /* Validate frozen_url mutual exclusivity */
  if (catalog == ForeignServerRelationId) {
    bool has_frozen = GetOptionValue(options, "frozen_url") != nullptr;
    bool has_dbname = GetOptionValue(options, "dbname") != nullptr;
    bool has_schema = GetOptionValue(options, "metadata_schema") != nullptr;

    if (has_frozen && has_dbname)
      ereport(ERROR,
              (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
               errmsg("\"frozen_url\" and \"dbname\" are mutually exclusive")));
    if (has_frozen && has_schema)
      ereport(ERROR,
              (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
               errmsg("\"frozen_url\" and \"metadata_schema\" are "
                      "mutually exclusive")));
  }

  PG_RETURN_VOID();
}

} // extern "C"

/* ----------------------------------------------------------------
 * Initialization
 * ---------------------------------------------------------------- */

namespace pgducklake {

void RegisterForeignTablesInQuery(Query *query) {
  ::RegisterForeignTablesInQuery(query);
}

void InitFDW() {
  RegisterDuckdbExternalTableCheck(IsDucklakeForeignTable);
  RegisterDuckdbRelationNameCallback(GetDucklakeForeignTableName);

  prev_fdw_process_utility_hook =
      ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
  ProcessUtility_hook = DucklakeFdwUtilityHook;
}

} // namespace pgducklake
