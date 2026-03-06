/*
 * pgducklake_hooks.cpp
 *
 * Installs pg_ducklake planner and utility hooks.
 *
 * Planner hook:
 * - delegates INSERT UNNEST planning optimization to pgducklake_direct_insert
 *
 * Utility hook:
 * - catches explicit COMMIT utility statements and commits DuckDB early
 * - detaches the DuckLake catalog on DROP EXTENSION pg_ducklake so that
 *   a subsequent CREATE EXTENSION can re-attach a fresh catalog
 */

#include "pgducklake/pgducklake_hooks.hpp"
#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_direct_insert.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_fdw.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgduckdb/pgduckdb_contracts.hpp"

extern "C" {
#include "postgres.h"

#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "tcop/utility.h"
}

namespace {

planner_hook_type prev_planner_hook = NULL;
ProcessUtility_hook_type prev_process_utility_hook = NULL;

PlannedStmt *DucklakePlannerHook(Query *parse, const char *query_string,
                                 int cursor_options,
                                 ParamListInfo bound_params) {
  if (pgducklake::enable_direct_insert) {
    PlannedStmt *direct_insert_plan =
        pgducklake::TryCreateDirectInsertPlan(parse, bound_params);
    if (direct_insert_plan)
      return direct_insert_plan;
  }

  /* ATTACH databases for any ducklake FDW tables before pg_duckdb plans */
  if (pgduckdb::DuckdbIsInitialized())
    pgducklake::RegisterForeignTablesInQuery(parse);

  return prev_planner_hook(parse, query_string, cursor_options, bound_params);
}

bool IsCommitUtilityStmt(PlannedStmt *pstmt) {
  if (!pstmt || !pstmt->utilityStmt ||
      !IsA(pstmt->utilityStmt, TransactionStmt))
    return false;

  auto *stmt = castNode(TransactionStmt, pstmt->utilityStmt);
  return stmt->kind == TRANS_STMT_COMMIT;
}

void ForceDuckDBCommitOnExplicitCommit() {
  const char *duckdb_errmsg = nullptr;
  int rc = pgducklake::ExecuteDuckDBQuery("COMMIT", &duckdb_errmsg);
  if (rc == 0)
    return;

  // Explicit PG COMMIT should always be mirrored to DuckDB COMMIT here.
  ereport(ERROR, (errmsg("pg_ducklake commit hook failed to commit DuckDB: %s",
                         duckdb_errmsg ? duckdb_errmsg : "unknown error")));
}

/*
 * Check whether the statement is DROP EXTENSION pg_ducklake.
 */
bool IsDropDucklakeExtensionStmt(PlannedStmt *pstmt) {
  if (!pstmt || !pstmt->utilityStmt || !IsA(pstmt->utilityStmt, DropStmt))
    return false;

  auto *drop = castNode(DropStmt, pstmt->utilityStmt);
  if (drop->removeType != OBJECT_EXTENSION)
    return false;

  ListCell *lc;
  foreach (lc, drop->objects) {
    char *extname = strVal(lfirst(lc));
    if (strcmp(extname, PGDUCKLAKE_PG_EXTENSION) == 0)
      return true;
  }
  return false;
}

void DucklakeUtilityHook(PlannedStmt *pstmt, const char *query_string,
                         bool read_only_tree, ProcessUtilityContext context,
                         ParamListInfo params,
                         struct QueryEnvironment *query_env, DestReceiver *dest,
                         QueryCompletion *qc) {
  if (IsCommitUtilityStmt(pstmt) && pgduckdb::DuckdbIsInitialized()) {
    elog(DEBUG1, "pg_ducklake utility hook caught COMMIT");
    ForceDuckDBCommitOnExplicitCommit();
  }

  bool dropping_extension = IsDropDucklakeExtensionStmt(pstmt);

  prev_process_utility_hook(pstmt, query_string, read_only_tree, context,
                            params, query_env, dest, qc);

  // After DROP EXTENSION completes, detach the DuckLake catalog from DuckDB
  // so that a subsequent CREATE EXTENSION can attach a fresh one.
  if (dropping_extension)
    ducklake_detach_catalog();
}

} // namespace

namespace pgducklake {
void InitHooks() {
  // Install planner hook after pg_duckdb (LIFO: our hook runs first).
  prev_planner_hook = planner_hook ? planner_hook : standard_planner;
  planner_hook = DucklakePlannerHook;

  // Chain ProcessUtility so we can observe COMMIT utility statements.
  prev_process_utility_hook =
      ProcessUtility_hook ? ProcessUtility_hook : standard_ProcessUtility;
  ProcessUtility_hook = DucklakeUtilityHook;
}

} // namespace pgducklake
