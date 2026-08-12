/*
 * Expose DuckLake's global ducklake_<name>(catalog, ...) functions under clean
 * PG names in the `ducklake` schema, keyed off the routing of
 * system.main.<name>(...). Two bridges: DuckDB macros for overload-free
 * mappings and variant extraction, and TableFunctionSets for overloaded
 * signatures macros can't express.
 *
 * The PG-side SQL stubs never run; the hooks route to DuckDB first. Each stub's
 * prosrc marker is either ducklake_only_function (route as-is: functions via
 * the planner hook, CALL procedures via the utility hook) or
 * ducklake_function_mapping (a regclass overload the planner first rewrites to
 * its (schema_name, table_name) form, then routes).
 */

#include "pgducklake/constants.hpp"
#include "pgducklake/ducklake_types.hpp"
#include "pgducklake/functions.hpp"
#include "pgducklake/time_travel.hpp"

#include <cstring>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/default/default_functions.hpp>
#include <duckdb/catalog/default/default_table_functions.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
}

namespace pgducklake {

using namespace duckdb;

// True for ducklake-schema objects with prosrc='ducklake_only_function'. The
// cached schema OID is checked first so the prosrc read is skipped for the many
// non-ducklake functions in a query.
bool
IsDucklakeOnlyFunction(Oid funcid) {
	Oid ducklake_nsp = DucklakeNamespaceOid();
	if (!OidIsValid(ducklake_nsp))
		return false;
	HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(tp))
		return false;
	if (((Form_pg_proc)GETSTRUCT(tp))->pronamespace != ducklake_nsp) {
		ReleaseSysCache(tp);
		return false;
	}
	bool isnull;
	Datum prosrc_datum = SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_prosrc, &isnull);
	if (isnull) {
		ReleaseSysCache(tp);
		return false;
	}
	char *prosrc_str = TextDatumGetCString(prosrc_datum);
	ReleaseSysCache(tp);
	return std::strcmp(prosrc_str, "ducklake_only_function") == 0;
}

} // namespace pgducklake

// Marker stub: routing to DuckDB happens before fmgr reaches this body, so if
// it ever runs, routing was missed.
#include "pgddb/utility/cpp_wrapper.hpp"

extern "C" {

DECLARE_PG_FUNCTION(ducklake_only_function) {
	char *name = DatumGetCString(DirectFunctionCall1(regprocout, ObjectIdGetDatum(fcinfo->flinfo->fn_oid)));
	elog(ERROR, "'%s' only works with DuckDB execution", name);
}

// Marker stub for regclass overloads: the planner rewrites these to their
// (schema_name, table_name) form before routing, so firing means the rewrite
// was missed.
DECLARE_PG_FUNCTION(ducklake_function_mapping) {
	ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("regclass function was not rewritten by planner hook"),
	                errhint("Use the (schema_name text, table_name text) form "
	                        "for dynamic table references.")));
	PG_RETURN_NULL();
}
}

namespace pgducklake {

// Wrapper table macros: system.main.<name>(...) expands to
// ducklake_<name>(catalog, ...).
// clang-format off
static const DefaultTableMacro pgducklake_wrapper_macros[] = {
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
  // data change feed function with concrete rowid/snapshot_id output columns
  {DEFAULT_SCHEMA, "table_changes",
   {"schema_name", "table_name", "start_snapshot", "end_snapshot", nullptr},
   {{nullptr, nullptr}},
   "FROM ducklake_table_changes('" PGDUCKLAKE_DUCKDB_CATALOG "', schema_name, table_name, start_snapshot, end_snapshot)"},
};
// clang-format on

// Scalar macros. Variant extraction goes through the VARCHAR/JSON
// representation because DuckDB's variant_extract only works on OBJECT variants
// (struct inserts), while PG inserts store variant data as VARCHAR JSON.
// clang-format off
static const DefaultMacro pgducklake_scalar_macros[] = {
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
};
// clang-format on

// Overloaded wrappers use TableFunctionSets since DuckDB macros can't
// overload. Each bind looks up the upstream ducklake_<name>, injects the
// catalog constant, sets named parameters, and replaces input.table_function.

/* Look up an upstream ducklake_<name> table function by name and arity. */
static TableFunction
LookupUpstreamFunction(ClientContext &context, const string &ducklake_name,
                       vector<LogicalType> arg_types = {LogicalType::VARCHAR}) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, DEFAULT_SCHEMA, ducklake_name)
	                  .Cast<TableFunctionCatalogEntry>();
	return entry.functions.GetFunctionByArguments(context, arg_types);
}

/* Stub init/execute -- bind replaces the function pointer before these run. */
static unique_ptr<GlobalTableFunctionState>
UnreachableInit(ClientContext &, TableFunctionInitInput &) {
	throw InternalException("UnreachableInit should never be called");
}

static void
UnreachableExecute(ClientContext &, TableFunctionInput &, DataChunk &) {
	throw InternalException("UnreachableExecute should never be called");
}

static void
RegisterTableFunctionSet(DatabaseInstance &db, TableFunctionSet &set) {
	CreateTableFunctionInfo info(set);
	auto &catalog = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	catalog.CreateTableFunction(transaction, info);
}

/* Reset bind input to just the catalog constant (the first positional arg of
 * every ducklake_<name>); the caller then adds overload-specific inputs. */
static void
ResetToCatalogOnly(TableFunctionBindInput &input) {
	input.inputs.clear();
	input.inputs.push_back(duckdb::Value(PGDUCKLAKE_DUCKDB_CATALOG));
	input.named_parameters.clear();
}

/* No-args cleanup_old_files/cleanup_orphaned_files pass cleanup_all=true. */
static unique_ptr<FunctionData>
CleanupAllBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
               vector<string> &names, const string &ducklake_name) {
	ResetToCatalogOnly(input);
	input.named_parameters["cleanup_all"] = duckdb::Value::BOOLEAN(true);

	auto func = LookupUpstreamFunction(context, ducklake_name);
	input.table_function = func;
	return func.bind(context, input, return_types, names);
}

static unique_ptr<FunctionData>
CleanupOldFilesNoArgsBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                          vector<string> &names) {
	return CleanupAllBind(context, input, return_types, names, "ducklake_cleanup_old_files");
}

static unique_ptr<FunctionData>
CleanupIntervalBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                    vector<string> &names) {
	auto interval_val = input.inputs[0].GetValue<interval_t>();
	auto now = duckdb::Timestamp::GetCurrentTimestamp();
	auto older_than = duckdb::Interval::Add(now, duckdb::Interval::Invert(interval_val));

	ResetToCatalogOnly(input);
	input.named_parameters["older_than"] = duckdb::Value::TIMESTAMPTZ(timestamp_tz_t(older_than.value));

	auto func = LookupUpstreamFunction(context, "ducklake_cleanup_old_files");
	input.table_function = func;
	return func.bind(context, input, return_types, names);
}

/* Upstream name differs: cleanup_orphaned_files -> ducklake_delete_orphaned_files. */
static unique_ptr<FunctionData>
OrphanedNoArgsBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                   vector<string> &names) {
	return CleanupAllBind(context, input, return_types, names, "ducklake_delete_orphaned_files");
}

/*
 * Delegate insertion/deletion feeds directly instead of wrapping them in table
 * macros. Their rowid and snapshot_id fields are virtual columns installed by
 * the replacement DuckLake scan function, and a table macro would hide them.
 */
static unique_ptr<FunctionData>
DataChangeFeedBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                   vector<string> &names, const string &ducklake_name) {
	auto schema_name = input.inputs[0];
	auto table_name = input.inputs[1];
	auto start_snapshot = input.inputs[2];
	auto end_snapshot = input.inputs[3];
	auto snapshot_type = start_snapshot.type();

	ResetToCatalogOnly(input);
	input.inputs.push_back(std::move(schema_name));
	input.inputs.push_back(std::move(table_name));
	input.inputs.push_back(std::move(start_snapshot));
	input.inputs.push_back(std::move(end_snapshot));

	auto func = LookupUpstreamFunction(
	    context, ducklake_name,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, snapshot_type, snapshot_type});
	input.table_function = func;
	return func.bind(context, input, return_types, names);
}

static unique_ptr<FunctionData>
TableInsertionsBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                    vector<string> &names) {
	return DataChangeFeedBind(context, input, return_types, names, "ducklake_table_insertions");
}

static unique_ptr<FunctionData>
TableDeletionsBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                   vector<string> &names) {
	return DataChangeFeedBind(context, input, return_types, names, "ducklake_table_deletions");
}

static void
RegisterDataChangeFeedSet(DatabaseInstance &db, const string &name, table_function_bind_t bind) {
	TableFunctionSet set(name);
	for (const auto &snapshot_type : {LogicalType::BIGINT, LogicalType::TIMESTAMP_TZ}) {
		set.AddFunction(TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, snapshot_type, snapshot_type},
		                              UnreachableExecute, bind, UnreachableInit));
	}
	RegisterTableFunctionSet(db, set);
}

/*
 * flush_inlined_data, merge_adjacent_files, rewrite_data_files use upstream
 * bind_operator (replaces the whole logical plan). The no-args overload is
 * identical across all three; the (text, text) overload differs in how
 * (schema, table) reach upstream, which also sets the positional arity used to
 * resolve the upstream overload:
 *   - merge/rewrite: (catalog, table_name) positional + schema => named
 *                    -> looked up by {VARCHAR, VARCHAR}
 *   - flush:         (catalog) positional + schema_name => / table_name => named
 *                    -> looked up by {VARCHAR} (the default)
 */
static unique_ptr<LogicalOperator>
BindOpNoArgs(ClientContext &context, TableFunctionBindInput &input, idx_t bind_index, vector<string> &return_names,
             const string &ducklake_name) {
	ResetToCatalogOnly(input);

	auto func = LookupUpstreamFunction(context, ducklake_name);
	input.table_function = func;
	return func.bind_operator(context, input, bind_index, return_names);
}

/* merge/rewrite: upstream signature is (catalog, table_name, schema => ...). */
static unique_ptr<LogicalOperator>
BindOpPositionalTable(ClientContext &context, TableFunctionBindInput &input, idx_t bind_index,
                      vector<string> &return_names, const string &ducklake_name) {
	auto schema_name = input.inputs[0].GetValue<string>();
	auto table_name = input.inputs[1].GetValue<string>();

	ResetToCatalogOnly(input);
	input.inputs.push_back(duckdb::Value(table_name));
	input.named_parameters["schema"] = duckdb::Value(schema_name);

	auto func = LookupUpstreamFunction(context, ducklake_name, {LogicalType::VARCHAR, LogicalType::VARCHAR});
	input.table_function = func;
	return func.bind_operator(context, input, bind_index, return_names);
}

/* flush: upstream signature is (catalog, schema_name => ..., table_name => ...). */
static unique_ptr<LogicalOperator>
BindOpNamedTable(ClientContext &context, TableFunctionBindInput &input, idx_t bind_index, vector<string> &return_names,
                 const string &ducklake_name) {
	auto schema_name = input.inputs[0].GetValue<string>();
	auto table_name = input.inputs[1].GetValue<string>();

	ResetToCatalogOnly(input);
	input.named_parameters["schema_name"] = duckdb::Value(schema_name);
	input.named_parameters["table_name"] = duckdb::Value(table_name);

	// Named params, so the upstream overload resolves on the single positional
	// VARCHAR (catalog) -- the default arity.
	auto func = LookupUpstreamFunction(context, ducklake_name);
	input.table_function = func;
	return func.bind_operator(context, input, bind_index, return_names);
}

/* bind_operator needs plain function pointers, so stamp a no-args/table-args
 * pair per pg function closing over the upstream name and table-arg style. */
#define DEFINE_BIND_OP_SET(prefix, ducklake_name, table_args_helper)                                                   \
	static unique_ptr<LogicalOperator> prefix##NoArgsBind(ClientContext &ctx, TableFunctionBindInput &input,           \
	                                                      idx_t bind_index, vector<string> &return_names) {            \
		return BindOpNoArgs(ctx, input, bind_index, return_names, ducklake_name);                                      \
	}                                                                                                                  \
	static unique_ptr<LogicalOperator> prefix##TableArgsBind(ClientContext &ctx, TableFunctionBindInput &input,        \
	                                                         idx_t bind_index, vector<string> &return_names) {         \
		return table_args_helper(ctx, input, bind_index, return_names, ducklake_name);                                 \
	}

DEFINE_BIND_OP_SET(Merge, "ducklake_merge_adjacent_files", BindOpPositionalTable)
DEFINE_BIND_OP_SET(Rewrite, "ducklake_rewrite_data_files", BindOpPositionalTable)
DEFINE_BIND_OP_SET(Flush, "ducklake_flush_inlined_data", BindOpNamedTable)

#undef DEFINE_BIND_OP_SET

static void
RegisterBindOperatorSet(DatabaseInstance &db, const string &pg_name, table_function_bind_operator_t no_args_bind,
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

/* Register all DuckLake-function bridges on a fresh DuckDB instance (called
 * from DuckDBManager::OnPostInit). */
void
RegisterDucklakeFunctions(DatabaseInstance &db) {
	auto &catalog = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);

	for (const auto &macro : pgducklake_wrapper_macros) {
		auto info = DefaultTableFunctionGenerator::CreateTableMacroInfo(macro);
		catalog.CreateFunction(transaction, *info);
	}

	for (const auto &macro : pgducklake_scalar_macros) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(macro);
		catalog.CreateFunction(transaction, *info);
	}

	// time_travel(...): built in time_travel.cpp.
	auto time_travel = GetTimeTravelFunctions();
	RegisterTableFunctionSet(db, time_travel);

	// table_insertions/deletions: preserve the replacement scan's virtual columns.
	RegisterDataChangeFeedSet(db, "table_insertions", TableInsertionsBind);
	RegisterDataChangeFeedSet(db, "table_deletions", TableDeletionsBind);

	// cleanup_old_files(): no-args + interval overloads (.bind pattern).
	{
		TableFunctionSet set("cleanup_old_files");
		set.AddFunction(TableFunction({}, UnreachableExecute, CleanupOldFilesNoArgsBind, UnreachableInit));
		set.AddFunction(
		    TableFunction({LogicalType::INTERVAL}, UnreachableExecute, CleanupIntervalBind, UnreachableInit));
		RegisterTableFunctionSet(db, set);
	}

	// cleanup_orphaned_files(): no-args only (.bind pattern).
	{
		TableFunctionSet set("cleanup_orphaned_files");
		set.AddFunction(TableFunction({}, UnreachableExecute, OrphanedNoArgsBind, UnreachableInit));
		RegisterTableFunctionSet(db, set);
	}

	// Compaction + flush: bind_operator sets (no-args + (text, text)).
	RegisterBindOperatorSet(db, "merge_adjacent_files", MergeNoArgsBind, MergeTableArgsBind);
	RegisterBindOperatorSet(db, "rewrite_data_files", RewriteNoArgsBind, RewriteTableArgsBind);
	RegisterBindOperatorSet(db, "flush_inlined_data", FlushNoArgsBind, FlushTableArgsBind);
}

} // namespace pgducklake
