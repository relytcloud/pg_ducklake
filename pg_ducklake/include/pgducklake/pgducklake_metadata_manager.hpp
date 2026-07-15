#pragma once

#include "pgddb/pg/declarations.hpp"

#include <common/ducklake_encryption.hpp>
#include <common/ducklake_options.hpp>
#include <common/ducklake_snapshot.hpp>
#include <duckdb/common/unique_ptr.hpp>
#include <metadata_manager/postgres_metadata_manager.hpp>
#include <storage/ducklake_metadata_info.hpp>
#include <storage/ducklake_metadata_manager.hpp>
#include <storage/ducklake_transaction.hpp>

namespace pgducklake {

class PgDuckLakeMetadataManager : public duckdb::PostgresMetadataManager {
public:
	explicit PgDuckLakeMetadataManager(duckdb::DuckLakeTransaction &transaction);
	~PgDuckLakeMetadataManager() override;

	static duckdb::unique_ptr<duckdb::DuckLakeMetadataManager>
	Create(duckdb::DuckLakeTransaction &transaction) {
		return duckdb::make_uniq<PgDuckLakeMetadataManager>(transaction);
	}

	// No-snapshot SPI execution primitive.
	duckdb::unique_ptr<duckdb::QueryResult> Execute(duckdb::string query) override;
	duckdb::unique_ptr<duckdb::QueryResult> Execute(duckdb::DuckLakeSnapshot snapshot, duckdb::string query) override;
	duckdb::unique_ptr<duckdb::QueryResult> ExecuteCommit(duckdb::DuckLakeSnapshot snapshot,
	                                                      duckdb::string query) override;

	duckdb::unique_ptr<duckdb::QueryResult> Query(duckdb::string query) override;
	duckdb::unique_ptr<duckdb::QueryResult> Query(duckdb::DuckLakeSnapshot snapshot, duckdb::string query) override;

	duckdb::unique_ptr<duckdb::QueryResult>
	ReadInlinedData(duckdb::DuckLakeSnapshot snapshot, const duckdb::string &inlined_table_name,
	                const duckdb::vector<duckdb::string> &columns_to_read) override;

	duckdb::unique_ptr<duckdb::QueryResult>
	ReadAllInlinedDataForFlush(duckdb::DuckLakeSnapshot snapshot, const duckdb::string &inlined_table_name,
	                           const duckdb::vector<duckdb::string> &columns_to_read) override;

	duckdb::vector<duckdb::string> GetFlushPartitionSQLExpressions(const duckdb::DuckLakeTableEntry &table) override;

	static bool IsInitialized();
	// In-process SPI backend has no DuckDB metadata connection: the base AttachMetadata
	// ATTACHes on it (null here) and MetadataExists probes ducklake_metadata in a way that
	// would abort the PG transaction if absent. Override both seams.
	duckdb::unique_ptr<duckdb::QueryResult> AttachMetadata(const duckdb::string &attach_query) override;
	bool MetadataExists() override;
	void InitializeDuckLake(bool has_explicit_schema, duckdb::DuckLakeEncryption encryption) override;

private:
	static void EnsureSnapshotTrigger();

protected:
	duckdb::string GetInlinedTableQueries(duckdb::DuckLakeSnapshot commit_snapshot,
	                                      const duckdb::DuckLakeTableInfo &table, duckdb::string &inlined_tables,
	                                      duckdb::string &inlined_table_queries) override;

	// Runs in-process via SPI, so use the base class's plain-SQL form rather than
	// PostgresMetadataManager's postgres_query()-wrapped variant (DuckDB postgres_scanner).
	duckdb::string GenerateFileColumnStatsCTEBody(const duckdb::CTERequirement &req,
	                                              duckdb::TableIndex table_id) override;
};

/* Direct-insert planner-time state. GetTableInliningInfo() returns true only for
 * TI_OK; callers needing the reason use GetTableInliningState() directly. */
enum TableInliningState {
	TI_OK = 0,
	TI_NO_TABLE,                /* table not found in ducklake metadata */
	TI_NO_INLINED_TABLE,        /* data_inlining_row_limit not set / <= 0 */
	TI_SCHEMA_VERSION_MISMATCH, /* inlined schema_version != max schema_version */
};

/* Also returns data_inlining_row_limit when state == TI_OK; row_limit_out may
 * be NULL. */
TableInliningState GetTableInliningState(Oid table_oid, uint64_t *table_id_out, uint64_t *schema_version_out,
                                         int64_t *row_limit_out);

bool GetTableInliningInfo(Oid table_oid, uint64_t *table_id_out, uint64_t *schema_version_out);

uint64_t GetNextRowIdForTable(uint64_t table_id, uint64_t schema_version);
uint64_t GetNextSnapshotId();
void CreateSnapshotForDirectInsert(uint64_t snapshot_id, uint64_t table_id, int64_t rows_inserted);

} // namespace pgducklake
