#pragma once

#include <common/ducklake_options.hpp>
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

	virtual duckdb::unique_ptr<duckdb::QueryResult> Execute(duckdb::string query) override;
	virtual duckdb::unique_ptr<duckdb::QueryResult> Execute(duckdb::DuckLakeSnapshot snapshot, duckdb::string query) override;

	duckdb::unique_ptr<duckdb::QueryResult> Query(duckdb::string query) override;
	duckdb::unique_ptr<duckdb::QueryResult> Query(duckdb::DuckLakeSnapshot snapshot, duckdb::string query) override;

	static bool IsInitialized();
	bool
	IsInitialized(duckdb::DuckLakeOptions & /*options*/) override {
		return IsInitialized();
	}

	// Some queries contain DuckDB syntax (e.g. LIST, STRUCT), we have to rewite them in PGSQL.
	duckdb::string CastStatsToTarget(const duckdb::string &stats, const duckdb::LogicalType &type) override;
	duckdb::DuckLakeCatalogInfo GetCatalogForSnapshot(duckdb::DuckLakeSnapshot snapshot) override;

protected:
	// Postgres-specific implementations for parsing query results
	duckdb::vector<duckdb::DuckLakeTag> LoadTags(const duckdb::Value &tag_map) const override;
	duckdb::vector<duckdb::DuckLakeInlinedTableInfo> LoadInlinedDataTables(const duckdb::Value &list) const override;
	duckdb::string WrapWithListAggregation(const duckdb::vector<std::pair<duckdb::string, duckdb::string>> &fields) const override;
};

} // namespace pgduckdb
