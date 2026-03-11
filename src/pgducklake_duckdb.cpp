/*
 * pgducklake_duckdb.cpp -- DuckLake catalog lifecycle in DuckDB
 *
 * Manages the "pgducklake" DuckLake catalog attached inside DuckDB.
 * Two lifecycles exist:
 *
 *   First CREATE EXTENSION (DuckDB not yet initialized):
 *     ducklake_initialize()          -- SQL script entry point
 *       -> ExecuteDuckDBQuery("SELECT 1")
 *           -> DuckDBManager::Initialize()
 *               -> ducklake_load_extension()   [callback from pg_duckdb]
 *                   -> LoadStaticExtension, Register metadata manager
 *                   -> ducklake_attach_catalog()
 *
 *   DROP + CREATE EXTENSION (DuckDB already alive):
 *     DROP EXTENSION pg_ducklake
 *       -> DucklakeUtilityHook        [pgducklake_hooks.cpp]
 *           -> ducklake_detach_catalog()
 *     CREATE EXTENSION pg_ducklake
 *       -> ducklake_initialize()
 *           -> ExecuteDuckDBQuery("SELECT 1")   (no-op, DuckDB exists)
 *           -> ducklake_attach_catalog()        (catalog was detached)
 *
 * Query execution against DuckDB is handled via pg_duckdb's raw_query() UDF
 * through PostgreSQL's SPI in the PostgreSQL-facing translation units.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"
#include "pgducklake/pgducklake_time_travel.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/main/database.hpp"
#include "ducklake_extension.hpp"
#include "storage/ducklake_metadata_manager.hpp"

#include <filesystem>

extern "C" {
#include "postgres.h"

#include "commands/extension.h"
#include "miscadmin.h"
#include "utils/elog.h"
}

static duckdb::DuckDB *ducklake_duckdb_instance = nullptr;

duckdb::DuckDB *ducklake_get_duckdb_database() {
  return ducklake_duckdb_instance;
}

void ducklake_detach_catalog() {
  const char *errmsg;
  int ret = pgducklake::ExecuteDuckDBQuery(
      "DETACH DATABASE IF EXISTS " PGDUCKLAKE_DUCKDB_CATALOG, &errmsg);
  if (ret != 0) {
    elog(WARNING, "Failed to detach DuckLake catalog: %s",
         errmsg ? errmsg : "unknown error");
  }
}

void ducklake_attach_catalog() {
  duckdb::string query = "ATTACH 'ducklake:" PGDUCKLAKE_DUCKDB_CATALOG
                         ":' AS " PGDUCKLAKE_DUCKDB_CATALOG
                         "(METADATA_SCHEMA " PGDUCKLAKE_PG_SCHEMA_QUOTED;
  auto data_path = duckdb::StringUtil::Format("%s/pg_ducklake", DataDir);
  if (creating_extension) {
    try {
      std::filesystem::create_directory(data_path);
    } catch (const std::filesystem::filesystem_error &e) {
      ereport(ERROR,
              (errcode(ERRCODE_IO_ERROR),
               errmsg("failed to create DuckLake data directory \"%s\": %s",
                      data_path.c_str(), e.what())));
    }
  }

  query += ", DATA_PATH '" + data_path + "'";
  query += ")";

  elog(DEBUG1, "Executing query: %s", query.c_str());

  const char *errmsg;
  int ret = pgducklake::ExecuteDuckDBQuery(query.c_str(), &errmsg);

  if (ret != 0) {
    elog(ERROR, "Failed to attach DuckLake catalog: %s", errmsg);
  }
}

/*
 * Register wrapper table macros in DuckDB's system.main catalog.
 *
 * pg_duckdb's DuckDB-only routing rewrites PG function calls to
 * system.main.<func_name>(args...). DuckLake registers its functions
 * globally as ducklake_<name>(catalog, ...). These macros bridge the
 * gap: a PG function with a clean name (e.g., "snapshots") routes to
 * system.main.snapshots(), which this macro expands to
 * ducklake_snapshots('pgducklake').
 */
// clang-format off
static const duckdb::DefaultTableMacro pg_ducklake_wrapper_macros[] = {
  // catalog-level functions (no table arg)
  {DEFAULT_SCHEMA, "snapshots", {nullptr}, {{nullptr, nullptr}},
   "FROM ducklake_snapshots('" PGDUCKLAKE_DUCKDB_CATALOG "')"},
  {DEFAULT_SCHEMA, "current_snapshot", {nullptr}, {{nullptr, nullptr}},
   "FROM ducklake_current_snapshot('" PGDUCKLAKE_DUCKDB_CATALOG "')"},
  {DEFAULT_SCHEMA, "last_committed_snapshot", {nullptr}, {{nullptr, nullptr}},
   "FROM ducklake_last_committed_snapshot('" PGDUCKLAKE_DUCKDB_CATALOG "')"},
  {DEFAULT_SCHEMA, "table_info", {nullptr}, {{nullptr, nullptr}},
   "FROM ducklake_table_info('" PGDUCKLAKE_DUCKDB_CATALOG "')"},
  // table-scoped functions
  {DEFAULT_SCHEMA, "list_files", {"schema_name", "table_name", nullptr}, {{nullptr, nullptr}},
   "FROM ducklake_list_files('" PGDUCKLAKE_DUCKDB_CATALOG "', table_name, schema => schema_name)"},
  // data change feed functions (schema + table + start + end)
  {DEFAULT_SCHEMA, "table_insertions",
   {"schema_name", "table_name", "start_snapshot", "end_snapshot", nullptr},
   {{nullptr, nullptr}},
   "FROM ducklake_table_insertions('" PGDUCKLAKE_DUCKDB_CATALOG "', schema_name, table_name, start_snapshot, end_snapshot)"},
  {DEFAULT_SCHEMA, "table_deletions",
   {"schema_name", "table_name", "start_snapshot", "end_snapshot", nullptr},
   {{nullptr, nullptr}},
   "FROM ducklake_table_deletions('" PGDUCKLAKE_DUCKDB_CATALOG "', schema_name, table_name, start_snapshot, end_snapshot)"},
  {DEFAULT_SCHEMA, "table_changes",
   {"schema_name", "table_name", "start_snapshot", "end_snapshot", nullptr},
   {{nullptr, nullptr}},
   "FROM ducklake_table_changes('" PGDUCKLAKE_DUCKDB_CATALOG "', schema_name, table_name, start_snapshot, end_snapshot)"},
  {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};
// clang-format on

static void RegisterDuckLakeWrapperMacros(duckdb::DatabaseInstance &db) {
  auto &catalog = duckdb::Catalog::GetSystemCatalog(db);
  auto transaction = duckdb::CatalogTransaction::GetSystemTransaction(db);
  for (int i = 0; pg_ducklake_wrapper_macros[i].name != nullptr; i++) {
    auto info = duckdb::DefaultTableFunctionGenerator::CreateTableMacroInfo(
        pg_ducklake_wrapper_macros[i]);
    catalog.CreateFunction(transaction, *info);
  }
}

void ducklake_load_extension(duckdb::DuckDB &db) {
  ducklake_duckdb_instance = &db;
  db.LoadStaticExtension<duckdb::DucklakeExtension>();
  pgducklake::RegisterTimeTravelFunction(*db.instance);
  RegisterDuckLakeWrapperMacros(*db.instance);

  duckdb::DuckLakeMetadataManager::Register(
      PGDUCKLAKE_DUCKDB_CATALOG, pgducklake::PgDuckLakeMetadataManager::Create);
  ducklake_attach_catalog();
}
