# DuckLake Feature Coverage

Comparison of upstream DuckLake extension features
(documented at <https://ducklake.select/docs/preview/duckdb/introduction>)
against what pg_ducklake currently exposes or supports.

Legend: `[x]` supported, `[ ]` not supported

Last updated: 2026-04-10

## Core DML Operations

- [x] `INSERT`: Including CTAS, INSERT...SELECT
- [x] `SELECT`: Via table AM and FDW
- [x] `UPDATE`
- [x] `DELETE`
- [x] `MERGE INTO` (upsert): PG 15+, single UPDATE/DELETE action per MERGE

## DDL / Schema Evolution

- [x] `CREATE TABLE`: `USING ducklake`
- [x] `DROP TABLE`: Via event trigger
- [x] `ALTER TABLE ADD COLUMN`
- [x] `ALTER TABLE DROP COLUMN`
- [x] `ALTER TABLE RENAME TABLE`
- [x] `ALTER TABLE RENAME COLUMN`
- [x] `ALTER TABLE ALTER COLUMN TYPE`
- [x] `ALTER TABLE SET DEFAULT` / `DROP DEFAULT`
- [x] `ALTER TABLE SET/DROP NOT NULL`: Constraint management via alter table event trigger
- [x] `COMMENT ON TABLE/COLUMN`: Via `ducklake_comment_trigger` event trigger; stored in ducklake_tag metadata
- [ ] `CREATE VIEW`: Stored in ducklake_view metadata
- [ ] `DROP VIEW`
- [ ] `CREATE MACRO` (scalar + table): Stored in ducklake_macro metadata
- [ ] `DROP MACRO` / `DROP MACRO TABLE`
- [ ] `CREATE SCHEMA`: DuckLake multi-schema support

## Time Travel

- [x] Query at version: `ducklake.time_travel(tbl, version)`
- [x] Query at timestamp: `ducklake.time_travel(tbl, ts)`

## Snapshots

- [x] `snapshots()`: List all snapshots and changesets
- [x] `current_snapshot()`: Get current snapshot ID
- [x] `last_committed_snapshot()`: Get latest committed snapshot
- [x] `set_commit_message()`: Add author/message to snapshots

## Data Change Feed

- [x] `table_changes(tbl, start, end)`: Query changes between snapshots
- [x] `table_deletions(tbl, start, end)`: Query deleted rows between snapshots
- [x] `table_insertions(tbl, start, end)`: Query inserted rows between snapshots

## Virtual Columns

- [x] `ducklake.rowid()`: Unique row lineage identifier
- [x] `ducklake.snapshot_id()`: Snapshot ID of the row's insertion
- [x] `ducklake.filename()`: Data file path containing the row
- [x] `ducklake.file_row_number()`: Row number within the data file
- [x] `ducklake.file_index()`: Internal file index for the row

## Partitioning

- [x] Set partition keys: `ducklake.set_partition()`
- [x] Reset partition: `ducklake.reset_partition()`
- [x] Get partition info: `ducklake.get_partition()`
- [x] Partition transforms (year/month/day/hour)

## Sorted Tables

- [x] `CREATE INDEX ... USING ducklake_sorted`: PG-native syntax for `SET SORTED BY`
- [x] `ducklake.set_sort()` / `ducklake.reset_sort()` / `ducklake.get_sort()`: Procedure-based alternative
- [x] Bidirectional sync: DuckDB sort keys sync to `pg_class` indexes and vice versa

## Advanced Features

- [x] Data inlining: `ducklake.flush_inlined_data()` and `data_inlining_row_limit` option
- [x] Variant type: `ducklake.variant` column type with `->` / `->>` extraction operators
- [ ] Encryption (`ENCRYPTED` flag): Parquet-level encryption
- [ ] Conflict resolution (auto-retry): pg_ducklake relies on PG transactions but lacks DuckLake's auto-retry
- [x] Transactions (ACID): Via PostgreSQL transaction model
- [x] Freeze/export to `.ducklake`: `ducklake.freeze()`

## Configuration

- [x] `set_option()` / `options()`: `ducklake.set_option()` and `ducklake.options()`
- [ ] `ducklake_settings()`: Instance metadata
- [ ] Extension-level retry settings: `ducklake_max_retry_count`, `ducklake_retry_wait_ms`, `ducklake_retry_backoff`
- [ ] `ducklake_default_data_inlining_row_limit`: Extension-level default
- [x] Schema-level option scoping: `ducklake.set_option(name, val, 'schema'::regnamespace)`

## Maintenance

- [x] Background maintenance worker: automatically runs flush, rewrite, merge, expire, cleanup
- [x] `VACUUM`: No-op on DuckLake tables (maintenance handled by background worker)
- [x] `ducklake_merge_adjacent_files()`: `ducklake.merge_adjacent_files()`
- [x] `ducklake_expire_snapshots()`: `ducklake.expire_snapshots()`
- [x] `ducklake_cleanup_old_files()`: `ducklake.cleanup_old_files()`
- [x] `ducklake_delete_orphaned_files()`: `ducklake.cleanup_orphaned_files()`
- [x] `ducklake_rewrite_data_files()`: `ducklake.rewrite_data_files()`
- [ ] `CHECKPOINT` (all-in-one maintenance): Runs all maintenance ops sequentially

## Metadata

- [x] `ducklake_list_files()`: `ducklake.list_files()`
- [x] `ducklake_table_info()`: `ducklake.table_info()`
- [ ] `ducklake_add_data_files()`: Register external Parquet files
- [x] Metadata tables (queryable): All `ducklake_*` tables in `ducklake` schema

## Migration

- [ ] `COPY FROM DATABASE`: Migrate DuckDB to DuckLake

## pg_ducklake-Specific Features

These features are unique to pg_ducklake and not part of the upstream DuckLake extension.

- [x] Role-based access control: `ducklake_superuser`, `ducklake_writer`, `ducklake_reader` roles
- [x] Foreign data wrapper (read-only): `ducklake_fdw` for read-only access to DuckLake tables
- [x] `IMPORT FOREIGN SCHEMA`: Bulk-import tables from a remote DuckLake catalog via FDW
- [x] Direct insert optimization: Fast path for `INSERT ... SELECT UNNEST($n)`

## Summary

- **Supported:** 55 features
- **Not supported:** 13 features
