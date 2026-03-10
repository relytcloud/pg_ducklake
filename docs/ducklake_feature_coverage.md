# DuckLake Feature Coverage

Comparison of upstream DuckLake extension features
(documented at <https://ducklake.select/docs/preview/duckdb/introduction>)
against what pg_ducklake currently exposes or supports.

Legend: `[x]` supported, `[ ]` not supported

Last updated: 2026-03-10

## Core DML Operations

- [x] `INSERT`: Including CTAS, INSERT...SELECT
- [x] `SELECT`: Via table AM and FDW
- [x] `UPDATE`
- [x] `DELETE`
- [ ] `MERGE INTO` (upsert): DuckLake uses MERGE since no PK support

## DDL / Schema Evolution

- [x] `CREATE TABLE`: `USING ducklake`
- [x] `DROP TABLE`: Via event trigger
- [x] `ALTER TABLE ADD COLUMN`
- [x] `ALTER TABLE DROP COLUMN`
- [x] `ALTER TABLE RENAME TABLE`
- [x] `ALTER TABLE RENAME COLUMN`
- [x] `ALTER TABLE ALTER COLUMN TYPE`
- [x] `ALTER TABLE SET DEFAULT`
- [ ] `ALTER TABLE SET/DROP NOT NULL`: Constraint management
- [ ] `CREATE VIEW`: Stored in ducklake_view metadata
- [ ] `DROP VIEW`
- [ ] `CREATE MACRO` (scalar + table): Stored in ducklake_macro metadata
- [ ] `DROP MACRO` / `DROP MACRO TABLE`
- [ ] `COMMENT ON TABLE/COLUMN`: Stored in ducklake_tag metadata
- [ ] `CREATE SCHEMA`: DuckLake multi-schema support

## Time Travel

- [x] Query at version: `ducklake.time_travel(tbl, version)`
- [x] Query at timestamp: `ducklake.time_travel(tbl, ts)` and `ducklake.as_of_timestamp` GUC

## Snapshots

- [ ] `snapshots()`: List all snapshots and changesets
- [ ] `current_snapshot()`: Get current snapshot ID
- [ ] `last_committed_snapshot()`: Get latest committed snapshot
- [ ] `set_commit_message()`: Add author/message to snapshots

## Data Change Feed

- [ ] `table_changes(tbl, start, end)`: Query changes between snapshots
- [ ] `table_deletions(tbl, start, end)`: Query deleted rows between snapshots
- [ ] `table_insertions(tbl, start, end)`: Query inserted rows between snapshots
- [ ] `rowid` virtual column: Unique row lineage identifier

## Partitioning

- [x] Set partition keys: `ducklake.set_partition()`
- [x] Reset partition: `ducklake.reset_partition()`
- [x] Get partition info: `ducklake.get_partition()`
- [x] Partition transforms (year/month/day/hour)

## Advanced Features

- [x] Data inlining: `ducklake.flush_inlined_data()` and `data_inlining_row_limit` option
- [ ] Encryption (`ENCRYPTED` flag): Parquet-level encryption
- [ ] Sorted tables (`SET SORTED BY`): Physical sort order for better min/max stats
- [ ] Conflict resolution (auto-retry): pg_ducklake relies on PG transactions but lacks DuckLake's auto-retry
- [x] Transactions (ACID): Via PostgreSQL transaction model
- [x] Freeze/export to `.ducklake`: `ducklake.freeze()`

## Configuration

- [x] `set_option()` / `options()`: `ducklake.set_option()` and `ducklake.options()`
- [ ] `ducklake_settings()`: Instance metadata
- [ ] Extension-level retry settings: `ducklake_max_retry_count`, `ducklake_retry_wait_ms`, `ducklake_retry_backoff`
- [ ] `ducklake_default_data_inlining_row_limit`: Extension-level default
- [ ] Schema-level option scoping: pg_ducklake has global + table only

## Maintenance

- [x] `VACUUM` (merge + rewrite): `VACUUM tablename`
- [ ] `ducklake_merge_adjacent_files()`: Dedicated merge function
- [ ] `ducklake_expire_snapshots()`: Expire old snapshots
- [x] `ducklake_cleanup_old_files()`: `ducklake.ducklake_cleanup_old_files()`
- [ ] `ducklake_delete_orphaned_files()`: Remove untracked files
- [ ] `ducklake_rewrite_data_files()`: VACUUM covers rewrite via `vacuum_delete_threshold`, but no dedicated function
- [ ] `CHECKPOINT` (all-in-one maintenance): Runs all maintenance ops sequentially

## Metadata

- [ ] `ducklake_list_files()`: List data/delete files for a table
- [ ] `ducklake_add_data_files()`: Register external Parquet files
- [x] Metadata tables (queryable): All `ducklake_*` tables in `ducklake` schema

## Migration

- [ ] `COPY FROM DATABASE`: Migrate DuckDB to DuckLake

## pg_ducklake-Specific Features

These features are unique to pg_ducklake and not part of the upstream DuckLake extension.

- [x] Role-based access control: `ducklake_superuser`, `ducklake_writer`, `ducklake_reader` roles
- [x] Foreign data wrapper (read-only): `ducklake_fdw` for read-only access to DuckLake tables
- [x] Direct insert optimization: Fast path for `INSERT ... SELECT UNNEST($n)`

## Summary

- **Supported:** 21 features -- core DML, CREATE/DROP TABLE, ALTER TABLE variants, time travel, partitioning, data inlining, freeze, options, VACUUM, cleanup old files, transactions, metadata tables
- **Not supported:** 19 features -- MERGE INTO, views, macros, comments, sorted tables, NOT NULL constraint management, snapshot functions, data change feed, rowid, encryption, dedicated maintenance functions, CHECKPOINT, list_files, add_data_files, migration, schema-level option scoping, extension-level settings
