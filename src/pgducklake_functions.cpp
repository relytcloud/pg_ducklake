/*
 * pgducklake_functions.cpp -- DuckLake function exposing.
 *
 * @scope backend: register DuckDB-only function names with pg_duckdb
 * @scope duckdb-instance: register wrapper macros, maintenance and
 *   flush table functions in DuckDB catalog
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
#include "duckdb/catalog/default/default_functions.hpp"
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
  pgduckdb::RegisterDuckdbOnlyFunction("cleanup_orphaned_files");
  pgduckdb::RegisterDuckdbOnlyFunction("flush_inlined_data");
  pgduckdb::RegisterDuckdbOnlyFunction("ensure_inlined_data_table");
  pgduckdb::RegisterDuckdbOnlyFunction("merge_adjacent_files");
  pgduckdb::RegisterDuckdbOnlyFunction("rewrite_data_files");
  pgduckdb::RegisterDuckdbOnlyFunction("expire_snapshots");
  // Virtual column accessors
  pgduckdb::RegisterDuckdbOnlyFunction("rowid");
  pgduckdb::RegisterDuckdbOnlyFunction("snapshot_id");
  pgduckdb::RegisterDuckdbOnlyFunction("filename");
  pgduckdb::RegisterDuckdbOnlyFunction("file_row_number");
  pgduckdb::RegisterDuckdbOnlyFunction("file_index");
  // Variant field extraction
  pgduckdb::RegisterDuckdbOnlyFunction("pg_variant_extract");
  pgduckdb::RegisterDuckdbOnlyFunction("pg_variant_extract_json");
  pgduckdb::RegisterDuckdbOnlyFunction("pg_variant_extract_json_idx");
  pgduckdb::RegisterDuckdbOnlyFunction("pg_variant_extract_idx");
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
  {DEFAULT_SCHEMA, "options", {nullptr}, {{nullptr, nullptr}},
   "FROM ducklake_options('" PGDUCKLAKE_DUCKDB_CATALOG "')"},
  // maintenance functions (no args)
  {DEFAULT_SCHEMA, "expire_snapshots", {nullptr}, {{nullptr, nullptr}},
   "FROM ducklake_expire_snapshots('" PGDUCKLAKE_DUCKDB_CATALOG "')"},
  // table-scoped functions
  {DEFAULT_SCHEMA, "ensure_inlined_data_table", {"schema_name", "table_name", nullptr}, {{nullptr, nullptr}},
   "FROM ducklake_ensure_inlined_table('" PGDUCKLAKE_DUCKDB_CATALOG "', schema_name, table_name)"},
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
    auto info = DefaultTableFunctionGenerator::CreateTableMacroInfo(pg_ducklake_wrapper_macros[i]);
    catalog.CreateFunction(transaction, *info);
  }
}

/*
 * Register scalar macros for variant field extraction.
 *
 * PG inserts store variant data as VARCHAR (JSON strings). DuckDB's
 * variant_extract only works on OBJECT variants (struct inserts).
 * These macros bridge the gap by extracting from the VARCHAR/JSON
 * representation, which handles both PG-inserted and DuckDB-inserted data.
 *
 * pg_duckdb's DuckDB-only routing rewrites PG function calls to
 * system.main.<func_name>(args...). These macros expand that to the
 * underlying json_extract_string / json_extract calls.
 */
// clang-format off
static const DefaultMacro pg_ducklake_scalar_macros[] = {
  // Virtual column accessors -- expand to bare column references
  {DEFAULT_SCHEMA, "rowid", {nullptr}, {{nullptr, nullptr}}, "rowid"},
  {DEFAULT_SCHEMA, "snapshot_id", {nullptr}, {{nullptr, nullptr}}, "snapshot_id"},
  {DEFAULT_SCHEMA, "filename", {nullptr}, {{nullptr, nullptr}}, "filename"},
  {DEFAULT_SCHEMA, "file_row_number", {nullptr}, {{nullptr, nullptr}}, "file_row_number"},
  {DEFAULT_SCHEMA, "file_index", {nullptr}, {{nullptr, nullptr}}, "file_index"},
  // Variant field extraction
  {DEFAULT_SCHEMA, "pg_variant_extract", {"v", "k", nullptr}, {{nullptr, nullptr}},
   "json_extract_string(v::VARCHAR, k)"},
  /* ::VARCHAR needed so DuckDB returns VARCHAR, which PG maps to variant */
  {DEFAULT_SCHEMA, "pg_variant_extract_json", {"v", "k", nullptr}, {{nullptr, nullptr}},
   "json_extract(v::VARCHAR, k)::VARCHAR"},
  {DEFAULT_SCHEMA, "pg_variant_extract_idx", {"v", "i", nullptr}, {{nullptr, nullptr}},
   "json_extract_string(v::VARCHAR, concat('$[', i, ']'))"},
  {DEFAULT_SCHEMA, "pg_variant_extract_json_idx", {"v", "i", nullptr}, {{nullptr, nullptr}},
   "json_extract(v::VARCHAR, concat('$[', i, ']'))::VARCHAR"},
  {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};
// clang-format on

void RegisterScalarMacros(DatabaseInstance &db) {
  auto &catalog = Catalog::GetSystemCatalog(db);
  auto transaction = CatalogTransaction::GetSystemTransaction(db);
  for (int i = 0; pg_ducklake_scalar_macros[i].name != nullptr; i++) {
    auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(pg_ducklake_scalar_macros[i]);
    catalog.CreateFunction(transaction, *info);
  }
}

/*
 * Shared helpers for table function wrappers.
 *
 * DuckDB macros don't support overloading (same name, different params),
 * so functions like cleanup_old_files and merge_adjacent_files use
 * TableFunctionSets instead. Each overload's bind function looks up the
 * underlying ducklake_<name>, injects the catalog constant, sets the
 * right named parameters, and replaces input.table_function.
 */

/* Look up an upstream ducklake_<name> table function by name and arity. */
static TableFunction LookupUpstreamFunction(ClientContext &context, const string &ducklake_name,
                                            vector<LogicalType> arg_types = {LogicalType::VARCHAR}) {
  auto &catalog = Catalog::GetSystemCatalog(context);
  auto &entry = catalog.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, ducklake_name);
  return entry.functions.GetFunctionByArguments(context, arg_types);
}

/* Stub init/execute for bind-pattern table functions -- bind replaces
 * the function pointer before these are reached. */
static unique_ptr<GlobalTableFunctionState> UnreachableInit(ClientContext &, TableFunctionInitInput &) {
  throw InternalException("UnreachableInit should never be called");
}

static void UnreachableExecute(ClientContext &, TableFunctionInput &, DataChunk &) {
  throw InternalException("UnreachableExecute should never be called");
}

/* Register a TableFunctionSet in the DuckDB system catalog. */
static void RegisterTableFunctionSet(DatabaseInstance &db, TableFunctionSet &set) {
  CreateTableFunctionInfo info(set);
  auto &catalog = Catalog::GetSystemCatalog(db);
  auto transaction = CatalogTransaction::GetSystemTransaction(db);
  catalog.CreateTableFunction(transaction, info);
}

/*
 * cleanup_old_files(): bind pattern with no-args and interval overloads.
 */
static unique_ptr<FunctionData> CleanupNoArgsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
  input.inputs.clear();
  input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
  input.named_parameters.clear();
  input.named_parameters["cleanup_all"] = duckdb::Value::BOOLEAN(true);

  auto func = LookupUpstreamFunction(context, "ducklake_cleanup_old_files");
  input.table_function = func;
  return func.bind(context, input, return_types, names);
}

static unique_ptr<FunctionData> CleanupIntervalBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
  auto interval_val = input.inputs[0].GetValue<interval_t>();
  auto now = duckdb::Timestamp::GetCurrentTimestamp();
  auto older_than = duckdb::Interval::Add(now, duckdb::Interval::Invert(interval_val));

  input.inputs.clear();
  input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
  input.named_parameters.clear();
  input.named_parameters["older_than"] = duckdb::Value::TIMESTAMPTZ(timestamp_tz_t(older_than.value));

  auto func = LookupUpstreamFunction(context, "ducklake_cleanup_old_files");
  input.table_function = func;
  return func.bind(context, input, return_types, names);
}

void RegisterCleanupFunction(DatabaseInstance &db) {
  TableFunctionSet set("cleanup_old_files");
  set.AddFunction(TableFunction({}, UnreachableExecute, CleanupNoArgsBind, UnreachableInit));
  set.AddFunction(TableFunction({LogicalType::INTERVAL}, UnreachableExecute, CleanupIntervalBind, UnreachableInit));
  RegisterTableFunctionSet(db, set);
}

/*
 * cleanup_orphaned_files(): no-args only, delegates to upstream
 * ducklake_delete_orphaned_files (note the different name).
 */
static unique_ptr<FunctionData> OrphanedNoArgsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
  input.inputs.clear();
  input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
  input.named_parameters.clear();
  input.named_parameters["cleanup_all"] = duckdb::Value::BOOLEAN(true);

  auto func = LookupUpstreamFunction(context, "ducklake_delete_orphaned_files");
  input.table_function = func;
  return func.bind(context, input, return_types, names);
}

void RegisterCleanupOrphanedFilesFunction(DatabaseInstance &db) {
  TableFunctionSet set("cleanup_orphaned_files");
  set.AddFunction(TableFunction({}, UnreachableExecute, OrphanedNoArgsBind, UnreachableInit));
  RegisterTableFunctionSet(db, set);
}

/*
 * Compaction functions: merge_adjacent_files() and rewrite_data_files().
 *
 * Both upstream functions use bind_operator (like flush_inlined_data) and
 * accept (catalog) or (catalog, table_name, schema=...) overloads.
 * PG exposes: no-args (whole catalog) and (text, text) (specific table).
 * The regclass overload is handled by ducklake_function_mapping in SQL,
 * which resolves to (schema_name text, table_name text).
 */
static unique_ptr<LogicalOperator> CompactionNoArgsBindOp(ClientContext &context, TableFunctionBindInput &input,
                                                          idx_t bind_index, vector<string> &return_names,
                                                          const string &ducklake_name) {
  input.inputs.clear();
  input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
  input.named_parameters.clear();

  auto func = LookupUpstreamFunction(context, ducklake_name);
  input.table_function = func;
  return func.bind_operator(context, input, bind_index, return_names);
}

static unique_ptr<LogicalOperator> CompactionTableArgsBindOp(ClientContext &context, TableFunctionBindInput &input,
                                                             idx_t bind_index, vector<string> &return_names,
                                                             const string &ducklake_name) {
  auto schema_name = input.inputs[0].GetValue<string>();
  auto table_name = input.inputs[1].GetValue<string>();

  input.inputs.clear();
  input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
  input.inputs.push_back(duckdb::Value(table_name));
  input.named_parameters.clear();
  input.named_parameters["schema"] = duckdb::Value(schema_name);

  auto func = LookupUpstreamFunction(context, ducklake_name, {LogicalType::VARCHAR, LogicalType::VARCHAR});
  input.table_function = func;
  return func.bind_operator(context, input, bind_index, return_names);
}

/* bind_operator requires a plain function pointer; define one per compaction function. */
#define DEFINE_COMPACTION_BIND_OPS(prefix, ducklake_name)                                                              \
  static unique_ptr<LogicalOperator> prefix##NoArgsBind(ClientContext &ctx, TableFunctionBindInput &input,             \
                                                        idx_t bind_index, vector<string> &return_names) {              \
    return CompactionNoArgsBindOp(ctx, input, bind_index, return_names, ducklake_name);                                \
  }                                                                                                                    \
  static unique_ptr<LogicalOperator> prefix##TableArgsBind(ClientContext &ctx, TableFunctionBindInput &input,          \
                                                           idx_t bind_index, vector<string> &return_names) {           \
    return CompactionTableArgsBindOp(ctx, input, bind_index, return_names, ducklake_name);                             \
  }

DEFINE_COMPACTION_BIND_OPS(Merge, "ducklake_merge_adjacent_files")
DEFINE_COMPACTION_BIND_OPS(Rewrite, "ducklake_rewrite_data_files")

#undef DEFINE_COMPACTION_BIND_OPS

static void RegisterOneCompactionFunction(DatabaseInstance &db, const string &pg_name,
                                          table_function_bind_operator_t no_args_bind,
                                          table_function_bind_operator_t table_args_bind) {
  TableFunctionSet set(pg_name);

  TableFunction no_args({}, nullptr, nullptr, nullptr);
  no_args.bind_operator = no_args_bind;
  set.AddFunction(no_args);

  TableFunction table_args({LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr, nullptr, nullptr);
  table_args.bind_operator = table_args_bind;
  set.AddFunction(table_args);

  RegisterTableFunctionSet(db, set);
}

void RegisterCompactionFunctions(DatabaseInstance &db) {
  RegisterOneCompactionFunction(db, "merge_adjacent_files", MergeNoArgsBind, MergeTableArgsBind);
  RegisterOneCompactionFunction(db, "rewrite_data_files", RewriteNoArgsBind, RewriteTableArgsBind);
}

/*
 * flush_inlined_data(): bind_operator pattern with no-args and
 * (schema, table) overloads. Unlike the bind-pattern functions above,
 * upstream flush uses bind_operator to replace the entire logical plan.
 */
static unique_ptr<LogicalOperator> FlushNoArgsBindOp(ClientContext &context, TableFunctionBindInput &input,
                                                     idx_t bind_index, vector<string> &return_names) {
  input.inputs.clear();
  input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
  input.named_parameters.clear();

  auto func = LookupUpstreamFunction(context, "ducklake_flush_inlined_data");
  input.table_function = func;
  return func.bind_operator(context, input, bind_index, return_names);
}

static unique_ptr<LogicalOperator> FlushTableArgsBindOp(ClientContext &context, TableFunctionBindInput &input,
                                                        idx_t bind_index, vector<string> &return_names) {
  auto schema_name = input.inputs[0].GetValue<string>();
  auto table_name = input.inputs[1].GetValue<string>();

  input.inputs.clear();
  input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
  input.named_parameters.clear();
  input.named_parameters["schema_name"] = duckdb::Value(schema_name);
  input.named_parameters["table_name"] = duckdb::Value(table_name);

  auto func = LookupUpstreamFunction(context, "ducklake_flush_inlined_data");
  input.table_function = func;
  return func.bind_operator(context, input, bind_index, return_names);
}

void RegisterFlushInlinedDataFunction(DatabaseInstance &db) {
  TableFunctionSet set("flush_inlined_data");

  TableFunction no_args({}, nullptr, nullptr, nullptr);
  no_args.bind_operator = FlushNoArgsBindOp;
  set.AddFunction(no_args);

  TableFunction table_args({LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr, nullptr, nullptr);
  table_args.bind_operator = FlushTableArgsBindOp;
  set.AddFunction(table_args);

  RegisterTableFunctionSet(db, set);
}

} // namespace pgducklake
