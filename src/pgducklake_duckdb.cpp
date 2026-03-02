/*
 * pgducklake_duckdb.cpp — DuckDB-facing translation unit
 *
 * This file includes DuckDB and DuckLake headers but NEVER PostgreSQL headers.
 * It provides the DuckLake extension lifecycle functions (init + load).
 *
 * Query execution against DuckDB is handled via pg_duckdb's raw_query() UDF
 * through PostgreSQL's SPI in the PostgreSQL-facing translation units.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"

#include "duckdb/main/database.hpp"
#include "ducklake_extension.hpp"
#include "storage/ducklake_metadata_manager.hpp"

#include <filesystem>

extern "C" {
#include "postgres.h"

#include "commands/extension.h"
#include "miscadmin.h"
#include "utils/elog.h"
}

void ducklake_load_extension(void *db_ptr, void *context_ptr) {
  auto *db = static_cast<duckdb::DuckDB *>(db_ptr);
  db->LoadStaticExtension<duckdb::DucklakeExtension>();

  duckdb::DuckLakeMetadataManager::Register(
      PGDUCKLAKE_DUCKDB_CATALOG, pgducklake::PgDuckLakeMetadataManager::Create);
  duckdb::string query = "ATTACH 'ducklake:" PGDUCKLAKE_DUCKDB_CATALOG
                         ":' AS " PGDUCKLAKE_DUCKDB_CATALOG
                         "(METADATA_SCHEMA " PGDUCKLAKE_PG_SCHEMA_QUOTED;
  auto data_path = duckdb::StringUtil::Format("%s/pg_ducklake", DataDir);
  if (creating_extension) {
    try {
      std::filesystem::create_directory(data_path);
    } catch (const std::filesystem::filesystem_error &e) {
      ereport(ERROR,
              (errcode(ERRCODE_IO_ERROR),
               errmsg("failed to create DuckLake data directory \"%s\": %s",
                      data_path.c_str(), e.what())));
    }
  }

  query += ", DATA_PATH '" + data_path + "'";
  query += ")";

  elog(DEBUG1, "Executing query: %s", query.c_str());

  const char *errmsg;
  int ret = pgducklake::ExecuteDuckDBQuery(query.c_str(), &errmsg);

  if (ret != 0) {
    elog(ERROR, "Failed to execute query, error: %s", errmsg);
  }
}
