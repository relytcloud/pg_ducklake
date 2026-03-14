#pragma once

#include "pgduckdb/pg/declarations.hpp"

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

  duckdb::unique_ptr<duckdb::QueryResult> Execute(duckdb::string query) override;
  duckdb::unique_ptr<duckdb::QueryResult>
  Execute(duckdb::DuckLakeSnapshot snapshot, duckdb::string query) override;
  duckdb::unique_ptr<duckdb::QueryResult>
  ExecuteCommit(duckdb::DuckLakeSnapshot snapshot,
                duckdb::string query) override;

  duckdb::unique_ptr<duckdb::QueryResult> Query(duckdb::string query) override;
  duckdb::unique_ptr<duckdb::QueryResult>
  Query(duckdb::DuckLakeSnapshot snapshot, duckdb::string query) override;

  static bool IsInitialized();
  bool IsInitialized(duckdb::DuckLakeOptions & /*options*/) override;
  void InitializeDuckLake(bool has_explicit_schema,
                          duckdb::DuckLakeEncryption encryption) override;

private:
  static void EnsureSnapshotTrigger();

  bool TypeIsNativelySupported(const duckdb::LogicalType &type) override;
  duckdb::string GetColumnTypeInternal(const duckdb::LogicalType &type) override;

protected:
  // Postgres-specific implementations for parsing query results
  duckdb::string GetInlinedTableQueries(duckdb::DuckLakeSnapshot commit_snapshot,
                                        const duckdb::DuckLakeTableInfo &table,
                                        duckdb::string &inlined_tables,
                                        duckdb::string &inlined_table_queries) override;
};

// Helper functions for direct insert optimization
bool GetTableInliningInfo(Oid table_oid, uint64_t *table_id_out,
                          uint64_t *schema_version_out);
uint64_t GetNextRowIdForTable(uint64_t table_id, uint64_t schema_version);
uint64_t GetNextSnapshotId();
void CreateSnapshotForDirectInsert(uint64_t snapshot_id,
                                   uint64_t schema_version);

} // namespace pgducklake
