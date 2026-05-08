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

  static duckdb::unique_ptr<duckdb::DuckLakeMetadataManager> Create(duckdb::DuckLakeTransaction &transaction) {
    return duckdb::make_uniq<PgDuckLakeMetadataManager>(transaction);
  }

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

  static bool IsInitialized();
  bool IsInitialized(duckdb::DuckLakeOptions & /*options*/) override;
  void InitializeDuckLake(bool has_explicit_schema, duckdb::DuckLakeEncryption encryption) override;

private:
  static void EnsureSnapshotTrigger();

protected:
  // Postgres-specific implementations for parsing query results
  duckdb::string GetInlinedTableQueries(duckdb::DuckLakeSnapshot commit_snapshot,
                                        const duckdb::DuckLakeTableInfo &table, duckdb::string &inlined_tables,
                                        duckdb::string &inlined_table_queries) override;
};

// Helper functions for direct insert optimization

/* Direct-insert planner-time state for a candidate target table.  The
 * bare bool GetTableInliningInfo() below returns true only for TI_OK
 * and discards the specific failure reason; callers that need to
 * surface the reason (e.g. the stats counters) use
 * GetTableInliningState() directly. */
enum TableInliningState {
  TI_OK = 0,
  TI_NO_TABLE,                /* table not found in ducklake metadata */
  TI_NO_INLINED_TABLE,        /* data_inlining_row_limit not set / <= 0 */
  TI_SCHEMA_VERSION_MISMATCH, /* inlined schema_version != max schema_version */
};

/* Extended version: also returns data_inlining_row_limit when state == TI_OK.
 * row_limit_out may be NULL if the caller doesn't need the limit. */
TableInliningState GetTableInliningState(Oid table_oid, uint64_t *table_id_out, uint64_t *schema_version_out,
                                         int64_t *row_limit_out);

/* Thin wrapper kept for existing callers that only need the bool. */
bool GetTableInliningInfo(Oid table_oid, uint64_t *table_id_out, uint64_t *schema_version_out);

uint64_t GetNextRowIdForTable(uint64_t table_id, uint64_t schema_version);
uint64_t GetNextSnapshotId();
void CreateSnapshotForDirectInsert(uint64_t snapshot_id, uint64_t table_id, int64_t rows_inserted);

} // namespace pgducklake
