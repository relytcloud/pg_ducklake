#pragma once

#include <common/ducklake_snapshot.hpp>
#include <duckdb/common/unique_ptr.hpp>
#include <storage/ducklake_metadata_info.hpp>
#include <storage/ducklake_metadata_manager.hpp>
#include <storage/ducklake_transaction.hpp>

namespace pgduckdb {

class PgDuckLakeMetadataManager : public duckdb::DuckLakeMetadataManager {
public:
	explicit PgDuckLakeMetadataManager(duckdb::DuckLakeTransaction &transaction);
	~PgDuckLakeMetadataManager() override;

	static duckdb::unique_ptr<duckdb::DuckLakeMetadataManager>
	Create(duckdb::DuckLakeTransaction &transaction) {
		return duckdb::make_uniq<PgDuckLakeMetadataManager>(transaction);
	}

	duckdb::unique_ptr<duckdb::QueryResult> Execute(duckdb::DuckLakeSnapshot snapshot, duckdb::string &query) override;

	duckdb::unique_ptr<duckdb::QueryResult> Query(duckdb::string query) override;
	duckdb::unique_ptr<duckdb::QueryResult> Query(duckdb::DuckLakeSnapshot snapshot, duckdb::string query) override;

	bool IsInitialized() override;

	// Some queries contain DuckDB syntax (e.g. LIST, STRUCT), we have to rewite them in PGSQL.
	duckdb::DuckLakeCatalogInfo GetCatalogForSnapshot(duckdb::DuckLakeSnapshot snapshot) override;
};

} // namespace pgduckdb
