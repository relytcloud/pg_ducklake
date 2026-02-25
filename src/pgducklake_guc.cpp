/*
 * pgducklake_guc.cpp — DuckLake GUC definitions and registration.
 *
 * Defines extension-level configuration variables and registers them in _PG_init().
 */

#include "pgducklake/pgducklake_guc.hpp"

extern "C" {
#include "postgres.h"

#include "utils/guc.h"
}

namespace pg_ducklake {

char *default_table_path = strdup("");
double vacuum_delete_threshold = 0.1;
char *as_of_timestamp = strdup("");
bool enable_inline_bypass = true;
bool ctas_skip_data = false;

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
      NULL, &vacuum_delete_threshold, 0.1, 0.0, 1.0, PGC_USERSET, 0, NULL,
      NULL, NULL);

  DefineCustomStringVariable(
      "ducklake.as_of_timestamp",
      "Timestamp for point-in-time queries on DuckLake tables. When set, all "
      "DuckLake table queries use AT (TIMESTAMP => 'value').",
      NULL, &as_of_timestamp, "", PGC_USERSET, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable(
      "ducklake.enable_inline_bypass",
      "Enable the DuckLake inline bypass optimization for INSERT ... SELECT "
      "UNNEST($n) statements.",
      NULL, &enable_inline_bypass, true, PGC_USERSET, 0, NULL, NULL, NULL);
}

} // namespace pg_ducklake
