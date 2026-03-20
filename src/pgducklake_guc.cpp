/*
 * pgducklake_guc.cpp -- DuckLake GUC definitions and registration.
 *
 * @scope backend: register GUCs
 *
 * Defines extension-level configuration variables and registers them in
 * _PG_init().
 */

#include "pgducklake/pgducklake_guc.hpp"

extern "C" {
#include "postgres.h"

#include "utils/guc.h"
}

namespace pgducklake {

char *default_table_path = strdup("");
double vacuum_delete_threshold = 0.1;
char *as_of_timestamp = strdup("");
bool enable_direct_insert = true;
bool ctas_skip_data = false;

char *superuser_role = strdup("ducklake_superuser");
char *writer_role = strdup("ducklake_writer");
char *reader_role = strdup("ducklake_reader");

void RegisterGUCs() {
  DefineCustomStringVariable(
      "ducklake.default_table_path",
      "Default directory path for DuckLake tables. If set, tables will be "
      "created under this path.",
      NULL, &default_table_path, "", PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomRealVariable(
      "ducklake.vacuum_delete_threshold",
      "Minimum fraction of deleted rows (0.0-1.0) before VACUUM rewrites a "
      "data file.",
      NULL, &vacuum_delete_threshold, 0.1, 0.0, 1.0, PGC_USERSET, 0, NULL, NULL,
      NULL);

  DefineCustomStringVariable(
      "ducklake.as_of_timestamp",
      "Timestamp for point-in-time queries on DuckLake tables. When set, all "
      "DuckLake table queries use AT (TIMESTAMP => 'value').",
      NULL, &as_of_timestamp, "", PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("ducklake.enable_direct_insert",
                           "Enable direct insert optimization for INSERT ... "
                           "SELECT UNNEST($n) statements.",
                           NULL, &enable_direct_insert, true, PGC_USERSET, 0,
                           NULL, NULL, NULL);

  DefineCustomStringVariable(
      "ducklake.superuser_role",
      "Role with full DDL + DML access to DuckLake tables. "
      "Created during CREATE EXTENSION if it does not exist.",
      NULL, &superuser_role, "ducklake_superuser", PGC_POSTMASTER,
      GUC_SUPERUSER_ONLY, NULL, NULL, NULL);

  DefineCustomStringVariable(
      "ducklake.writer_role",
      "Role with DML access (SELECT/INSERT/UPDATE/DELETE) to DuckLake tables. "
      "Created during CREATE EXTENSION if it does not exist.",
      NULL, &writer_role, "ducklake_writer", PGC_POSTMASTER,
      GUC_SUPERUSER_ONLY, NULL, NULL, NULL);

  DefineCustomStringVariable(
      "ducklake.reader_role",
      "Role with SELECT-only access to DuckLake tables. "
      "Created during CREATE EXTENSION if it does not exist.",
      NULL, &reader_role, "ducklake_reader", PGC_POSTMASTER,
      GUC_SUPERUSER_ONLY, NULL, NULL, NULL);
}

} // namespace pgducklake
