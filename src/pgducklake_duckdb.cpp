/*
 * pgducklake_duckdb.cpp — DuckDB-facing translation unit
 *
 * This file includes DuckDB and DuckLake headers but NEVER PostgreSQL headers.
 * It provides the DuckLake extension lifecycle functions (init + load).
 *
 * Query execution against DuckDB is handled via pg_duckdb's raw_query() UDF
 * through PostgreSQL's SPI in the PostgreSQL-facing translation units.
 */

#include "pgducklake/pgducklake_metadata_manager.hpp"

#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "ducklake_extension.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include <duckdb/main/client_context.hpp>
#include <filesystem>

extern "C" {
#include "postgres.h"

#include "commands/extension.h"
#include "miscadmin.h"
#include "utils/elog.h"
}

extern "C" void ducklake_init_extension(void) {
}

extern "C" void ducklake_load_extension(void *db_ptr, void *context_ptr) {
  auto *db = static_cast<duckdb::DuckDB *>(db_ptr);
  db->LoadStaticExtension<duckdb::DucklakeExtension>();

  auto context = static_cast<duckdb::ClientContext *>(context_ptr);

  duckdb::DuckLakeMetadataManager::Register(
      "pgducklake", pgducklake::PgDuckLakeMetadataManager::Create);
  duckdb::string query = "ATTACH 'ducklake:pgducklake:' AS pgducklake "
                         "(METADATA_SCHEMA 'ducklake'";
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

  auto res = context->Query(query, false);
  if (res->HasError()) {
    elog(ERROR, "Failed to execute query, error: %s", res->GetError().c_str());
    res->ThrowError();
  }
}
