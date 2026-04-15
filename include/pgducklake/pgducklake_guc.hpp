#pragma once

namespace pgducklake {

extern char *default_table_path;
extern double vacuum_delete_threshold;
extern bool enable_direct_insert;
extern bool ctas_skip_data;

extern bool enable_metadata_sync;

extern char *superuser_role;
extern char *writer_role;
extern char *reader_role;

/* Maintenance worker GUCs */
extern bool maintenance_enabled;
extern int maintenance_naptime;
extern int maintenance_max_workers;
extern bool maintenance_flush_inlined_data;
extern bool maintenance_expire_snapshots;
extern bool maintenance_cleanup_old_files;

/* Register all DuckLake GUCs — call from _PG_init() */
void RegisterGUCs();

} // namespace pgducklake
