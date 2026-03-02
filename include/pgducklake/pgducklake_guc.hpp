#pragma once

namespace pg_ducklake {

extern char *default_table_path;
extern double vacuum_delete_threshold;
extern char *as_of_timestamp;
extern bool enable_direct_insert;
extern bool ctas_skip_data;

/* Register all DuckLake GUCs — call from _PG_init() */
void RegisterGUCs();

} // namespace pg_ducklake
