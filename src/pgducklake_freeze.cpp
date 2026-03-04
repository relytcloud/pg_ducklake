/*
 * pgducklake_freeze.cpp — Export DuckLake metadata to a standalone .ducklake file.
 *
 * Copies all 22 ducklake_* metadata tables from PostgreSQL into a new DuckDB
 * database file, producing a "frozen" snapshot that DuckDB clients can query
 * directly without PostgreSQL.
 *
 * Usage:
 *   CALL ducklake.freeze('/path/to/output.ducklake');
 *   CALL ducklake.freeze('/path/to/output.ducklake', 's3://bucket/data/');
 */

#include <duckdb/common/string_util.hpp>
#include <duckdb/parser/keyword_helper.hpp>

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
}

namespace pgducklake {

// Must match the tables created by ducklake's InitializeDatabase() in
// third_party/ducklake/src/storage/ducklake_metadata_manager.cpp.
static const char *metadata_tables[] = {
    "ducklake_metadata",
    "ducklake_snapshot",
    "ducklake_snapshot_changes",
    "ducklake_schema",
    "ducklake_table",
    "ducklake_view",
    "ducklake_tag",
    "ducklake_column_tag",
    "ducklake_data_file",
    "ducklake_file_column_stats",
    "ducklake_delete_file",
    "ducklake_column",
    "ducklake_table_stats",
    "ducklake_table_column_stats",
    "ducklake_partition_info",
    "ducklake_partition_column",
    "ducklake_file_partition_value",
    "ducklake_files_scheduled_for_deletion",
    "ducklake_inlined_data_tables",
    "ducklake_column_mapping",
    "ducklake_name_mapping",
    "ducklake_schema_versions",
};

static constexpr const char *FROZEN_DB = "__pgducklake_frozen__";

static void DetachFrozenDB() {
  auto detach = duckdb::StringUtil::Format("DETACH %s", FROZEN_DB);
  ExecuteDuckDBQuery(detach.c_str(), nullptr);
}

static void FreezeFail(const char *step, const char *error_msg) {
  DetachFrozenDB();
  ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                  errmsg("failed to freeze DuckLake at %s: %s", step,
                         error_msg ? error_msg : "unknown error")));
}

} // namespace pgducklake

extern "C" {

DECLARE_PG_FUNCTION(ducklake_freeze) {
  if (PG_ARGISNULL(0))
    elog(ERROR, "output_path cannot be NULL");

  char *output_path = text_to_cstring(PG_GETARG_TEXT_PP(0));
  bool has_data_path = PG_NARGS() > 1 && !PG_ARGISNULL(1);
  char *data_path = has_data_path ? text_to_cstring(PG_GETARG_TEXT_PP(1)) : nullptr;

  const char *error_msg = nullptr;

  // If data inlining is enabled, the caller must flush inlined data before
  // freezing (CALL ducklake.flush_inlined_data()). The flush must be a
  // separate top-level PG statement so pg_duckdb's catalog cache refreshes
  // before the copy reads from pgduckdb.ducklake.*.
  std::string batch;
  batch += duckdb::StringUtil::Format(
      "ATTACH %s AS %s;\n",
      duckdb::KeywordHelper::WriteQuoted(output_path).c_str(),
      pgducklake::FROZEN_DB);

  for (auto &table : pgducklake::metadata_tables) {
    bool empty = (strcmp(table, "ducklake_files_scheduled_for_deletion") == 0 ||
                  strcmp(table, "ducklake_inlined_data_tables") == 0);
    batch += duckdb::StringUtil::Format(
        "CREATE TABLE %s.main.%s AS SELECT * FROM pgduckdb." PGDUCKLAKE_PG_SCHEMA ".%s%s;\n",
        pgducklake::FROZEN_DB, table, table,
        empty ? " WHERE false" : "");
  }

  if (has_data_path) {
    batch += duckdb::StringUtil::Format(
        "UPDATE %s.main.ducklake_metadata SET value = %s WHERE key = 'data_path';\n",
        pgducklake::FROZEN_DB,
        duckdb::KeywordHelper::WriteQuoted(data_path).c_str());
  }

  if (pgducklake::ExecuteDuckDBQuery(batch.c_str(), &error_msg) != 0)
    pgducklake::FreezeFail("copy metadata", error_msg);

  // 3. DETACH
  pgducklake::DetachFrozenDB();

  PG_RETURN_VOID();
}

}
