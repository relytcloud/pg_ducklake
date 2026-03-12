/*
 * pgducklake_functions.cpp -- DuckLake function exposing.
 *
 * Exposes upstream DuckLake functions as PostgreSQL functions in the
 * `ducklake` schema. Two layers are involved:
 *
 *   C++ side (this file)
 *     - Registers function names with pg_duckdb via RegisterDuckdbOnlyFunction
 *       so the planner routes queries to DuckDB.
 *     - Registers wrapper table macros in DuckDB's system.main catalog that
 *       inject the catalog constant and delegate to ducklake_<name>() globals.
 *     - Registers DuckDB table functions (TableFunctionSet) for functions
 *       that need overloaded signatures (e.g., cleanup_old_files,
 *       flush_inlined_data).
 *
 *   SQL side (pg_ducklake--0.1.0.sql)
 *     - Defines the actual PG function signatures as DuckDB-only C stubs.
 *
 * === Function Mapping Rules ===
 *
 * pg_duckdb's DuckDB-only routing rewrites a PG function call to:
 *
 *     system.main.<pg_function_name>(args...)
 *
 * DuckLake extension registers its functions globally as ducklake_<name>
 * with a catalog arg. Two bridging mechanisms are used:
 *
 * 1. Wrapper table macros -- for simple mappings:
 *   PG function: ducklake.snapshots()
 *     -> DuckDB-only routing: system.main.snapshots()
 *     -> Wrapper macro: FROM ducklake_snapshots('pgducklake')
 *
 * 2. Table function sets -- for overloaded signatures:
 *   PG function: ducklake.cleanup_old_files() / cleanup_old_files(interval)
 *     -> DuckDB-only routing: system.main.cleanup_old_files(...)
 *     -> TableFunctionSet bind replaces with ducklake_cleanup_old_files()
 */

#include "pgducklake/pgducklake_functions.hpp"
#include "pgducklake/pgducklake_defs.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "pgduckdb/pgduckdb_contracts.hpp"

namespace pgducklake {

using namespace duckdb;

void RegisterDuckdbOnlyFunctions() {
  pgduckdb::RegisterDuckdbOnlyExtension("pg_ducklake");
  // Existing functions
  pgduckdb::RegisterDuckdbOnlyFunction("options");
  pgduckdb::RegisterDuckdbOnlyFunction("time_travel");
  // Snapshot functions
  pgduckdb::RegisterDuckdbOnlyFunction("snapshots");
  pgduckdb::RegisterDuckdbOnlyFunction("current_snapshot");
  pgduckdb::RegisterDuckdbOnlyFunction("last_committed_snapshot");
  // Metadata functions
  pgduckdb::RegisterDuckdbOnlyFunction("table_info");
  pgduckdb::RegisterDuckdbOnlyFunction("list_files");
  // Data change feed functions
  pgduckdb::RegisterDuckdbOnlyFunction("table_insertions");
  pgduckdb::RegisterDuckdbOnlyFunction("table_deletions");
  pgduckdb::RegisterDuckdbOnlyFunction("table_changes");
  // Maintenance functions
  pgduckdb::RegisterDuckdbOnlyFunction("cleanup_old_files");
  pgduckdb::RegisterDuckdbOnlyFunction("flush_inlined_data");
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
static const DefaultTableMacro pg_ducklake_wrapper_macros[] = {
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

void RegisterWrapperMacros(DatabaseInstance &db) {
  auto &catalog = Catalog::GetSystemCatalog(db);
  auto transaction = CatalogTransaction::GetSystemTransaction(db);
  for (int i = 0; pg_ducklake_wrapper_macros[i].name != nullptr; i++) {
    auto info = DefaultTableFunctionGenerator::CreateTableMacroInfo(
        pg_ducklake_wrapper_macros[i]);
    catalog.CreateFunction(transaction, *info);
  }
}

/*
 * cleanup_old_files() DuckDB table function set.
 *
 * DuckDB macros don't support overloading (same name, different params),
 * so cleanup_old_files uses a TableFunctionSet instead. Each overload's
 * bind function looks up the underlying ducklake_cleanup_old_files, sets
 * the right named parameters, and replaces input.table_function -- the
 * same pattern used by time_travel (pgducklake_time_travel.cpp).
 */
static TableFunction LookupDuckLakeCleanupFunction(ClientContext &context) {
  auto &catalog = Catalog::GetSystemCatalog(context);
  auto &entry = catalog.GetEntry<TableFunctionCatalogEntry>(
      context, DEFAULT_SCHEMA, "ducklake_cleanup_old_files");
  return entry.functions.GetFunctionByArguments(
      context, {LogicalType::VARCHAR});
}

static unique_ptr<FunctionData>
CleanupNoArgsBind(ClientContext &context, TableFunctionBindInput &input,
                  vector<LogicalType> &return_types, vector<string> &names) {
  input.inputs.clear();
  input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
  input.named_parameters.clear();
  input.named_parameters["cleanup_all"] = duckdb::Value::BOOLEAN(true);

  auto func = LookupDuckLakeCleanupFunction(context);
  input.table_function = func;
  return func.bind(context, input, return_types, names);
}

static unique_ptr<FunctionData>
CleanupIntervalBind(ClientContext &context, TableFunctionBindInput &input,
                    vector<LogicalType> &return_types, vector<string> &names) {
  auto interval_val = input.inputs[0].GetValue<interval_t>();
  auto now = duckdb::Timestamp::GetCurrentTimestamp();
  auto older_than =
      duckdb::Interval::Add(now, duckdb::Interval::Invert(interval_val));

  input.inputs.clear();
  input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
  input.named_parameters.clear();
  input.named_parameters["older_than"] =
      duckdb::Value::TIMESTAMPTZ(timestamp_tz_t(older_than.value));

  auto func = LookupDuckLakeCleanupFunction(context);
  input.table_function = func;
  return func.bind(context, input, return_types, names);
}

static unique_ptr<GlobalTableFunctionState>
CleanupInit(ClientContext &, TableFunctionInitInput &) {
  throw InternalException("CleanupInit should never be called");
}

static void CleanupExecute(ClientContext &, TableFunctionInput &,
                           DataChunk &) {
  throw InternalException("CleanupExecute should never be called");
}

void RegisterCleanupFunction(DatabaseInstance &db) {
  TableFunctionSet set("cleanup_old_files");
  set.AddFunction(
      TableFunction({}, CleanupExecute, CleanupNoArgsBind, CleanupInit));
  set.AddFunction(TableFunction({LogicalType::INTERVAL}, CleanupExecute,
                                CleanupIntervalBind, CleanupInit));

  CreateTableFunctionInfo info(set);
  auto &catalog = Catalog::GetSystemCatalog(db);
  auto transaction = CatalogTransaction::GetSystemTransaction(db);
  catalog.CreateTableFunction(transaction, info);
}

/*
 * flush_inlined_data() DuckDB table function set.
 *
 * Unlike cleanup_old_files, the upstream ducklake_flush_inlined_data uses
 * bind_operator (not bind) -- it replaces the entire logical plan at bind
 * time. Each overload's bind_operator looks up the upstream function, sets
 * the catalog and named parameters, then delegates to its bind_operator.
 */
static TableFunction LookupDuckLakeFlushFunction(ClientContext &context) {
  auto &catalog = Catalog::GetSystemCatalog(context);
  auto &entry = catalog.GetEntry<TableFunctionCatalogEntry>(
      context, DEFAULT_SCHEMA, "ducklake_flush_inlined_data");
  return entry.functions.GetFunctionByArguments(
      context, {LogicalType::VARCHAR});
}

static unique_ptr<LogicalOperator>
FlushNoArgsBindOp(ClientContext &context, TableFunctionBindInput &input,
                  idx_t bind_index, vector<string> &return_names) {
  input.inputs.clear();
  input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
  input.named_parameters.clear();

  auto func = LookupDuckLakeFlushFunction(context);
  input.table_function = func;
  return func.bind_operator(context, input, bind_index, return_names);
}

static unique_ptr<LogicalOperator>
FlushTableArgsBindOp(ClientContext &context, TableFunctionBindInput &input,
                     idx_t bind_index, vector<string> &return_names) {
  auto schema_name = input.inputs[0].GetValue<string>();
  auto table_name = input.inputs[1].GetValue<string>();

  input.inputs.clear();
  input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
  input.named_parameters.clear();
  input.named_parameters["schema_name"] = duckdb::Value(schema_name);
  input.named_parameters["table_name"] = duckdb::Value(table_name);

  auto func = LookupDuckLakeFlushFunction(context);
  input.table_function = func;
  return func.bind_operator(context, input, bind_index, return_names);
}

void RegisterFlushInlinedDataFunction(DatabaseInstance &db) {
  TableFunctionSet set("flush_inlined_data");

  TableFunction no_args({}, nullptr, nullptr, nullptr);
  no_args.bind_operator = FlushNoArgsBindOp;
  set.AddFunction(no_args);

  TableFunction table_args({LogicalType::VARCHAR, LogicalType::VARCHAR},
                           nullptr, nullptr, nullptr);
  table_args.bind_operator = FlushTableArgsBindOp;
  set.AddFunction(table_args);

  CreateTableFunctionInfo info(set);
  auto &catalog = Catalog::GetSystemCatalog(db);
  auto transaction = CatalogTransaction::GetSystemTransaction(db);
  catalog.CreateTableFunction(transaction, info);
}

} // namespace pgducklake
