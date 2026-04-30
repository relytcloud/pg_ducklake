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

/* MUST come after the DuckLake/DuckDB headers above: this header
 * transitively pulls in postgres.h, whose FATAL macro clobbers
 * DuckDB's ExceptionType::FATAL when DuckDB headers are parsed
 * afterwards. */
#include "pgducklake/pgducklake_direct_insert_stats.hpp"

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

/* COPY FROM commit path.  Distinct from the direct-insert path because COPY
 * does not know its row count up-front and cannot use the atomic reservation
 * API.  Inserts the snapshot row, the snapshot_changes row, and
 * updates/inserts the stats row from a pre-allocated snapshot_id and
 * post-hoc rows_inserted count.  Has the same MAX(snapshot_id)+1 and
 * read-then-update concurrency hazards as the pre-#191 direct-insert path;
 * a chunked reservation migration for COPY FROM is tracked separately. */
void CreateSnapshotForCopyFrom(uint64_t snapshot_id, uint64_t table_id, int64_t rows_inserted);

/* Advisory-lock namespace for direct-insert per-table row_id reservation.
 * Used as the first argument of pg_advisory_xact_lock(int4, int4); the
 * second argument is table_id cast to int4 (DuckLake table_ids are
 * monotonically issued from 1, so 32 bits is ample in practice).  Different
 * tables get distinct keys and remain parallel; concurrent direct inserts
 * on the same table serialize on this lock.  The constant is arbitrary but
 * fixed: 'pDLK' in ASCII = 0x70444C4B.  Exposed to SQL via
 * ducklake.direct_insert_lock_ns() so isolation specs do not duplicate the
 * literal. */
static constexpr int32_t DUCKLAKE_DIRECT_INSERT_LOCK_NS = 0x70444C4B;

/* RAII handle for a direct-insert metadata reservation.  Encodes the
 * three-step protocol -- reserve row_ids, reserve snapshot_id, commit --
 * at the type level so callers cannot misuse it.
 *
 * Lifecycle (all within one Postgres transaction; direct insert is
 * autocommit, so that is automatic):
 *   1. Construct: takes a per-table advisory lock, atomically advances
 *      ducklake_table_stats.next_row_id by `nrows`, then reserves a fresh
 *      snapshot_id via INSERT ... ON CONFLICT (snapshot_id) DO NOTHING
 *      with a bounded retry loop (cap: ducklake.direct_insert_max_retries).
 *      Each lost race bumps DI_R_RETRY for `pattern`.  Raises ERROR
 *      with SQLSTATE 40001 if retries are exhausted.
 *   2. Caller writes inlined rows tagged with RowIdStart()..+nrows-1
 *      and SnapshotId().
 *   3. Commit(): writes ducklake_snapshot_changes and idempotently
 *      bootstraps ducklake_table_column_stats via WHERE NOT EXISTS
 *      (safe to run on every direct insert; cheap when rows already
 *      exist).
 *
 * If the caller throws or returns without calling Commit(), the
 * surrounding Postgres transaction will abort and roll back the
 * reservation rows; the advisory lock auto-releases at xact end. */
class DirectInsertReservation {
public:
  DirectInsertReservation(uint64_t table_id, uint64_t nrows, DirectInsertPattern pattern);

  uint64_t RowIdStart() const {
    return row_id_start_;
  }
  uint64_t SnapshotId() const {
    return snapshot_id_;
  }

  void Commit();

  DirectInsertReservation(const DirectInsertReservation &) = delete;
  DirectInsertReservation &operator=(const DirectInsertReservation &) = delete;

private:
  uint64_t table_id_;
  uint64_t row_id_start_;
  uint64_t snapshot_id_;
};

} // namespace pgducklake
