# Architecture Overview

## Component map

- @src/pgducklake.cpp: `_PG_init`, bootstrap, extension registration wiring
- @src/pgducklake_duckdb.cpp: DuckDB bridge, deferred static extension load
- @src/pgducklake_metadata_manager.cpp: `PgDuckLakeMetadataManager` implementation
- @src/pgducklake_ddl.cpp: create/drop event triggers, DuckDB DDL synchronization
- @src/pgducklake_table_am.cpp: table access method registration

## Lifecycle (deferred extension load)

1. `pg_duckdb` loads (`shared_preload_libraries`).
2. `pg_ducklake` loads (`shared_preload_libraries`), triggers `_PG_init()`.
   - `RegisterDuckdbLoadExtension(...)` registers hooks on DuckDB creation.
3. `CREATE EXTENSION pg_ducklake` triggers `sql/pg_ducklake-*.sql`.
   - Creates DDL triggers, table access method, UDFs
   - Executes `ducklake_initialize`
4. During DuckDB instance (re-)initialization in each PostgreSQL process, callbacks registered by `RegisterDuckdbLoadExtension(...)` are executed.
   - `ducklake_load_extension` loads static extension `ducklake` into DuckDB, registers custom DuckLake metadata manager, and attaches pg_ducklake catalog on duckdb instance.

Reason: static extension loading requires fully initialized DuckDB instance; metadata manager registration does not.

## DDL path

DDL is executed by PostgreSQL, then `ducklake_<ddl>_trigger` is called to handle DDL operations (see @src/pgducklake_ddl.cpp).
Event triggers check events and synchronize corresponding DuckDB objects in `PGDUCKLAKE_DUCKDB_CATALOG`.

## DML path

DML containing ducklake objects is caught by `pg_duckdb` hooks (see `DuckdbInitHooks` in @third_party/pg_duckdb/src/pgduckdb_hooks.cpp).
SQL is parsed and converted to DuckDB SQL by `DuckdbPlannerHook`, then passed to DuckDB for execution. Ducklake tables are converted to `pgducklake.<schema_name>.<table_name>` (`RegisterDuckdbTableAm`).
