#include "duckdb/catalog/default/default_table_functions.hpp"
#include "functions/ducklake_table_functions.hpp"

namespace duckdb {

// clang-format off
static const DefaultTableMacro ducklake_table_macros[] = {
	{DEFAULT_SCHEMA, "ducklake_table_changes", {"catalog", "schema_name", "table_name", "start_snapshot", "end_snapshot", nullptr}, {{nullptr, nullptr}},  R"(
SELECT snapshot_id, rowid, CASE WHEN (snapshot_id, rowid) in (SELECT snapshot_id, rowid FROM ducklake_table_deletions(catalog, schema_name, table_name, start_snapshot, end_snapshot))
       THEN 'update_postimage'
       ELSE 'insert'
       END AS change_type, * FROM ducklake_table_insertions(catalog, schema_name, table_name, start_snapshot, end_snapshot)
UNION ALL
SELECT snapshot_id, rowid, CASE WHEN (snapshot_id, rowid) in (SELECT snapshot_id, rowid FROM ducklake_table_insertions(catalog, schema_name, table_name, start_snapshot, end_snapshot))
       THEN 'update_preimage'
       ELSE 'delete'
       END AS change_type, * FROM ducklake_table_deletions(catalog, schema_name, table_name, start_snapshot, end_snapshot)
)"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
	};
// clang-format on

unique_ptr<CreateMacroInfo> DuckLakeTableInsertionsFunction::GetDuckLakeTableChanges() {
	return DefaultTableFunctionGenerator::CreateTableMacroInfo(ducklake_table_macros[0]);
}

} // namespace duckdb
