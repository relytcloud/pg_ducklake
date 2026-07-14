#pragma once

namespace pgduckdb {
void InitGUC();
void InitGUCHooks();

extern bool duckdb_force_execution;
extern bool duckdb_use_shared_worker;
extern int duckdb_arrow_pool_pages;
extern int duckdb_arrow_page_size;
extern int duckdb_scan_pool_size;
extern int duckdb_scan_producers;
extern int duckdb_max_worker_sessions;
extern int duckdb_worker_threads;
extern bool duckdb_unsafe_allow_execution_inside_functions;
extern bool duckdb_unsafe_allow_mixed_transactions;
extern int duckdb_threads;
extern int duckdb_maximum_memory;
extern char *duckdb_disabled_filesystems;
extern char *duckdb_allowed_directories;
extern bool duckdb_enable_external_access;
extern bool duckdb_allow_community_extensions;
extern bool duckdb_allow_unsigned_extensions;
extern bool duckdb_autoinstall_known_extensions;
extern bool duckdb_autoload_known_extensions;
extern char *duckdb_postgres_role;
extern char *duckdb_motherduck_session_hint;
extern bool duckdb_force_motherduck_views;
extern char *duckdb_temp_directory;
extern char *duckdb_extension_directory;
extern char *duckdb_max_temp_directory_size;
extern char *duckdb_default_collation;
extern char *duckdb_azure_transport_option_type;
extern char *duckdb_custom_user_agent;
} // namespace pgduckdb
