#pragma once

namespace pgducklake {

extern char *default_table_path;
extern double vacuum_delete_threshold;
extern bool enable_direct_insert;
extern bool ctas_skip_data;

extern bool enable_metadata_sync;

extern int threads;
extern bool use_shared_worker;
extern int worker_max_sessions;
extern int worker_arrow_pool_pages;
extern int worker_arrow_page_size;
extern int worker_scan_pool_size;
extern int worker_scan_producers;

extern char *superuser_role;
extern char *writer_role;
extern char *reader_role;

extern bool maintenance_enabled;
extern int maintenance_naptime;
extern int maintenance_max_workers;
extern bool maintenance_flush_inlined_data;
extern bool maintenance_expire_snapshots;
extern bool maintenance_cleanup_old_files;

} // namespace pgducklake
