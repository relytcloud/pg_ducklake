#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "common/ducklake_util.hpp"

namespace duckdb {

static unique_ptr<FunctionData> DuckLakeEnsureInlinedTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
	auto &transaction = DuckLakeTransaction::Get(context, ducklake_catalog);
	auto &metadata_manager = transaction.GetMetadataManager();
	auto snapshot = transaction.GetSnapshot();

	auto schema_name = input.inputs[1].GetValue<string>();
	auto table_name = input.inputs[2].GetValue<string>();

	// Look up table_id from metadata
	auto lookup_query = StringUtil::Format("SELECT dt.table_id FROM {METADATA_CATALOG}.ducklake_table dt "
	                                       "JOIN {METADATA_CATALOG}.ducklake_schema ds ON dt.schema_id = ds.schema_id "
	                                       "WHERE dt.table_name = %s AND ds.schema_name = %s "
	                                       "AND dt.end_snapshot IS NULL AND ds.end_snapshot IS NULL",
	                                       SQLString(table_name), SQLString(schema_name));
	auto lookup_result = metadata_manager.Query(snapshot, lookup_query);
	auto lookup_chunk = lookup_result->Fetch();
	if (!lookup_chunk || lookup_chunk->size() == 0) {
		throw InvalidInputException("Table \"%s\".\"%s\" not found in DuckLake catalog", schema_name, table_name);
	}
	auto table_id_val = lookup_chunk->GetValue(0, 0).GetValue<idx_t>();
	TableIndex table_id(table_id_val);

	// Check if inlined data table already exists
	auto check_query = StringUtil::Format(
	    "SELECT table_name FROM {METADATA_CATALOG}.ducklake_inlined_data_tables WHERE table_id = %d", table_id.index);
	auto check_result = metadata_manager.Query(snapshot, check_query);
	string existing_name;
	auto check_chunk = check_result->Fetch();
	if (check_chunk && check_chunk->size() > 0) {
		existing_name = check_chunk->GetValue(0, 0).GetValue<string>();
	}

	names.push_back("table_name");
	return_types.push_back(LogicalType::VARCHAR);

	auto result = make_uniq<MetadataBindData>();
	if (!existing_name.empty()) {
		result->rows.push_back({Value(existing_name)});
		return std::move(result);
	}

	// Get table entry from the catalog for column info
	auto table_entry_ptr = ducklake_catalog.GetEntryById(transaction, snapshot, table_id);
	if (!table_entry_ptr) {
		throw InternalException("Could not find table entry for table_id %d", table_id.index);
	}
	auto &table = table_entry_ptr->Cast<DuckLakeTableEntry>();

	auto table_info = table.GetTableInfo();
	table_info.columns = table.GetTableColumns();

	if (DuckLakeUtil::HasInlinedSystemColumnConflict(table_info.columns)) {
		throw InvalidInputException("Table \"%s\" has columns that conflict with inlined system columns (row_id, "
		                            "begin_snapshot, end_snapshot)",
		                            table_name);
	}
	if (!metadata_manager.SupportsInliningColumns(table_info.columns)) {
		throw InvalidInputException("Table \"%s\" has columns with types that do not support inlining", table_name);
	}

	// Bump schema_version for the inlined table
	DuckLakeSnapshot ensure_snapshot = snapshot;
	ensure_snapshot.schema_version = snapshot.schema_version + 1;

	// Generate inlined table SQL using existing infrastructure
	string inlined_tables;
	string inlined_table_queries;
	auto inlined_tbl_name =
	    metadata_manager.GetInlinedTableQueries(ensure_snapshot, table_info, inlined_tables, inlined_table_queries);

	// Execute: insert metadata row and create the table
	string batch_query;
	batch_query += "INSERT INTO {METADATA_CATALOG}.ducklake_inlined_data_tables VALUES " + inlined_tables + ";";
	batch_query += inlined_table_queries;
	metadata_manager.Execute(snapshot, batch_query);

	result->rows.push_back({Value(inlined_tbl_name)});
	return std::move(result);
}

DuckLakeEnsureInlinedTableFunction::DuckLakeEnsureInlinedTableFunction()
    : DuckLakeBaseMetadataFunction("ducklake_ensure_inlined_table",
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   DuckLakeEnsureInlinedTableBind) {
}

} // namespace duckdb
