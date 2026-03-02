/*
 * pgducklake_vacuum.cpp — VACUUM support for DuckLake tables
 *
 * Implements VACUUM by calling DuckDB's ducklake_rewrite_data_files() and
 * ducklake_merge_adjacent_files() functions via duckdb.raw_query().
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_guc.hpp"

#include <duckdb/parser/keyword_helper.hpp>

extern "C" {
#include "postgres.h"

#include "commands/vacuum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

extern "C" void ducklake_do_vacuum(Relation onerel, VacuumParams *params,
                                   BufferAccessStrategy bstrategy) {
  const char *relname = NameStr(onerel->rd_rel->relname);
  const char *schema_name =
      get_namespace_name_or_temp(onerel->rd_rel->relnamespace);

  if (!schema_name) {
    elog(ERROR, "cache lookup failed for namespace %u",
         onerel->rd_rel->relnamespace);
  }

  bool verbose = params->options & VACOPT_VERBOSE;

  if (verbose) {
    ereport(INFO, (errmsg("vacuuming ducklake table \"%s.%s\"", schema_name,
                          relname)));
  }

  std::string db_name(PGDUCKLAKE_DUCKDB_CATALOG);
  std::string relname_str(relname);
  std::string schema_name_str(schema_name);

  /*
   * 1. Rewrite data files to remove deleted rows.
   * The delete threshold is configurable via ducklake.vacuum_delete_threshold
   * GUC. Default is 0.1 (10%), more aggressive than DuckLake's default.
   */
  std::string rewrite_query =
      "SELECT * FROM ducklake_rewrite_data_files(" +
      duckdb::KeywordHelper::WriteQuoted(db_name, '\'') + ", " +
      duckdb::KeywordHelper::WriteQuoted(relname_str, '\'') + ", " +
      "schema=" + duckdb::KeywordHelper::WriteQuoted(schema_name_str, '\'') +
      ", delete_threshold => " +
      std::to_string(pgducklake::vacuum_delete_threshold) + ")";

  const char *error_msg = nullptr;
  int result =
      pgducklake::ExecuteDuckDBQuery(rewrite_query.c_str(), &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to rewrite data files: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  if (verbose) {
    ereport(INFO, (errmsg("rewritten data files with deleted rows")));
  }

  /* 2. Merge adjacent small files to optimize scan performance */
  std::string merge_query =
      "SELECT * FROM ducklake_merge_adjacent_files(" +
      duckdb::KeywordHelper::WriteQuoted(db_name, '\'') + ", " +
      duckdb::KeywordHelper::WriteQuoted(relname_str, '\'') + ", " +
      "schema=" + duckdb::KeywordHelper::WriteQuoted(schema_name_str, '\'') +
      ")";

  result = pgducklake::ExecuteDuckDBQuery(merge_query.c_str(), &error_msg);
  if (result != 0) {
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to merge adjacent files: %s",
                           error_msg ? error_msg : "unknown error")));
  }

  if (verbose) {
    ereport(INFO, (errmsg("merged adjacent small files")));
  }

  elog(DEBUG1, "vacuumed ducklake table \"%s.%s\"", schema_name, relname);
}
