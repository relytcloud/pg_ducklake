#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "pgddb/catalog/relation_desc.hpp"
#include "pgddb/pg/declarations.hpp"

#include "pgddb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgddb {

class PostgresTable : public duckdb::TableCatalogEntry {
public:
	PostgresTable(duckdb::Catalog &catalog, duckdb::SchemaCatalogEntry &schema, duckdb::CreateTableInfo &info,
	              RelationDesc desc, Snapshot snapshot);

	virtual ~PostgresTable();

	duckdb::unique_ptr<duckdb::BaseStatistics> GetStatistics(duckdb::ClientContext &context,
	                                                         duckdb::column_t column_id) override;
	duckdb::TableFunction GetScanFunction(duckdb::ClientContext &context,
	                                      duckdb::unique_ptr<duckdb::FunctionData> &bind_data) override;
	duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext &context) override;

	static Relation OpenRelation(Oid relid);
	static void CloseRelation(Relation rel);
	static void SetTableInfo(duckdb::CreateTableInfo &info, const RelationDesc &desc);

protected:
	RelationDesc desc;
	Snapshot snapshot;

private:
	PostgresTable(const PostgresTable &) = delete;
	PostgresTable &operator=(const PostgresTable &) = delete;
};

} // namespace pgddb
