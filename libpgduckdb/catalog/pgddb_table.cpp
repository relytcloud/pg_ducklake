#include "pgddb/catalog/pgddb_table.hpp"

#include "pgddb/scan/postgres_scan.hpp"
#include "pgddb/catalog/pgddb_schema.hpp"
#include "pgddb/logger.hpp"
#include "pgddb/pg/relations.hpp"
#include "pgddb/pgddb_process_lock.hpp"
#include "pgddb/pgddb_types.hpp" // ConvertPostgresToDuckColumnType

#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "pgddb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgddb {

PostgresTable::PostgresTable(duckdb::Catalog &_catalog, duckdb::SchemaCatalogEntry &_schema,
                             duckdb::CreateTableInfo &_info, RelationDesc _desc, Snapshot _snapshot)
    : duckdb::TableCatalogEntry(_catalog, _schema, _info), desc(std::move(_desc)), snapshot(_snapshot) {
}

PostgresTable::~PostgresTable() {
}

Relation
PostgresTable::OpenRelation(Oid relid) {
	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	return pgddb::OpenRelation(relid);
}

void
PostgresTable::CloseRelation(Relation rel) {
	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	pgddb::CloseRelation(rel);
}

void
PostgresTable::SetTableInfo(duckdb::CreateTableInfo &info, const RelationDesc &desc) {
	for (const auto &col : desc.columns) {
		info.columns.AddColumn(duckdb::ColumnDefinition(col.name, col.type));
		pd_log(DEBUG2, "(DuckDB/SetTableInfo) Column name: '%s', Type: %s", col.name.c_str(),
		       col.type.ToString().c_str());
	}
}

duckdb::unique_ptr<duckdb::BaseStatistics>
PostgresTable::GetStatistics(duckdb::ClientContext &, duckdb::column_t) {
	throw duckdb::NotImplementedException("GetStatistics not supported yet");
}

duckdb::TableFunction
PostgresTable::GetScanFunction(duckdb::ClientContext &, duckdb::unique_ptr<duckdb::FunctionData> &bind_data) {
	bind_data = duckdb::make_uniq<PostgresScanFunctionData>(desc, snapshot);
	return PostgresScanTableFunction();
}

duckdb::TableStorageInfo
PostgresTable::GetStorageInfo(duckdb::ClientContext &) {
	throw duckdb::NotImplementedException("GetStorageInfo not supported yet");
}

} // namespace pgddb
