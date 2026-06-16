# Settings

pg_ducklake has two kinds of settings:

- **PostgreSQL GUCs** -- set via `SET` or `postgresql.conf`
- **DuckLake catalog options** -- set via `CALL ducklake.set_option(name, value [, scope])`

Use `SELECT * FROM ducklake.options()` to list all DuckLake catalog options and their current values.

## PostgreSQL GUCs

| Name | Default | Access |
| :--- | :------ | :----- |
| [`ducklake.default_table_path`](#ducklakedefault_table_path) | `""` | Per-session |
| [`ducklake.enable_direct_insert`](#ducklakeenable_direct_insert) | `true` | Per-session |
| [`ducklake.enable_metadata_sync`](#ducklakeenable_metadata_sync) | `true` | Per-session |
| [`ducklake.maintenance_enabled`](#ducklakemaintenance_enabled) | `true` | Reload (`SIGHUP`) |
| [`ducklake.maintenance_naptime`](#ducklakemaintenance_naptime) | `60` | Reload (`SIGHUP`) |
| [`ducklake.maintenance_max_workers`](#ducklakemaintenance_max_workers) | `3` | Requires restart |
| [`ducklake.maintenance_flush_inlined_data`](#ducklakemaintenance_flush_inlined_data) | `true` | Reload (`SIGHUP`) |
| [`ducklake.maintenance_expire_snapshots`](#ducklakemaintenance_expire_snapshots) | `true` | Reload (`SIGHUP`) |
| [`ducklake.maintenance_cleanup_old_files`](#ducklakemaintenance_cleanup_old_files) | `false` | Reload (`SIGHUP`) |
| [`ducklake.reader_role`](#ducklakereader_role) | `"ducklake_reader"` | Requires restart |
| [`ducklake.superuser_role`](#ducklakesuperuser_role) | `"ducklake_superuser"` | Requires restart |
| [`ducklake.vacuum_delete_threshold`](#ducklakevacuum_delete_threshold) | `0.1` | Per-session |
| [`ducklake.writer_role`](#ducklakewriter_role) | `"ducklake_writer"` | Requires restart |

## DuckLake Catalog Options

Defaults and scopes track the upstream
[DuckLake configuration reference](https://ducklake.select/docs/stable/duckdb/usage/configuration).
Every option can be scoped global, schema, or table (most specific wins).

| Name | Default |
| :--- | :------ |
| [`auto_compact`](#auto_compact) | `true` |
| [`created_by`](#created_by) | -- |
| [`data_inlining_row_limit`](#data_inlining_row_limit) | `10` |
| [`data_path`](#data_path) | -- |
| [`delete_older_than`](#delete_older_than) | -- |
| [`expire_older_than`](#expire_older_than) | -- |
| [`hive_file_pattern`](#hive_file_pattern) | `true` |
| [`parquet_compression`](#parquet_compression) | `snappy` |
| [`parquet_compression_level`](#parquet_compression_level) | `3` |
| [`parquet_row_group_size`](#parquet_row_group_size) | `122880` |
| [`parquet_row_group_size_bytes`](#parquet_row_group_size_bytes) | -- |
| [`parquet_version`](#parquet_version) | `1` |
| [`per_thread_output`](#per_thread_output) | `false` |
| [`require_commit_message`](#require_commit_message) | `false` |
| [`rewrite_delete_threshold`](#rewrite_delete_threshold) | `0.95` |
| [`sort_on_insert`](#sort_on_insert) | `true` |
| [`target_file_size`](#target_file_size) | `512MB` |

## Detailed Descriptions

### PostgreSQL GUCs

### `ducklake.default_table_path`

Default directory path for DuckLake tables. If set, tables will be created under this path. Supports cloud storage paths (e.g., `s3://my-bucket/prefix/`).

- **Default**: `""` (empty -- uses local storage)
- **Access**: Per-session

### `ducklake.maintenance_enabled`

Enable the background maintenance worker. When enabled, a launcher process periodically spawns workers that run the full maintenance pipeline (flush inlined data, rewrite data files, merge adjacent files, expire snapshots, cleanup old files) on every database with pg_ducklake installed.

- **Default**: `true`
- **Access**: Reload (`SIGHUP`)

### `ducklake.maintenance_naptime`

Seconds between maintenance cycles. The launcher sleeps this long between scans of `pg_database`.

- **Default**: `60`
- **Range**: 1 -- 86400
- **Access**: Reload (`SIGHUP`)

### `ducklake.maintenance_max_workers`

Maximum number of concurrent maintenance workers across all databases.

- **Default**: `3`
- **Range**: 1 -- 8
- **Access**: Requires restart

### `ducklake.maintenance_flush_inlined_data`

Flush inlined data to Parquet files during background maintenance. Disable to skip this step if inlined data should remain in the metadata catalog.

- **Default**: `true`
- **Access**: Reload (`SIGHUP`)

### `ducklake.maintenance_expire_snapshots`

Expire old snapshots during background maintenance. The retention window is controlled by the `expire_older_than` DuckLake catalog option.

- **Default**: `true`
- **Access**: Reload (`SIGHUP`)

### `ducklake.maintenance_cleanup_old_files`

Clean up unreferenced data files from storage during background maintenance.

- **Default**: `false`
- **Access**: Reload (`SIGHUP`)

### `ducklake.vacuum_delete_threshold`

Minimum fraction of deleted rows before the background maintenance worker rewrites a data file. This GUC applies only to the worker. A direct `CALL ducklake.rewrite_data_files()` does not consult this GUC; it uses the DuckLake catalog option `rewrite_delete_threshold` instead. Note: `VACUUM` on DuckLake tables is a no-op; compaction is handled by the background maintenance worker.

- **Default**: `0.1`
- **Range**: 0.0 -- 1.0
- **Access**: Per-session

### `ducklake.enable_direct_insert`

Enable direct insert optimization for `INSERT ... SELECT UNNEST($n)` statements.

- **Default**: `true`
- **Access**: Per-session

### `ducklake.enable_metadata_sync`

Enable reverse metadata sync from DuckDB to PostgreSQL. When enabled (default), a snapshot trigger detects tables created or dropped by external DuckDB clients and creates/drops the corresponding `pg_class` entries. Disable this when all DDL and DML goes through PostgreSQL, to avoid the per-commit trigger overhead.

- **Default**: `true`
- **Access**: Per-session

### `ducklake.superuser_role`

Role with full DDL + DML access to DuckLake tables. Created during `CREATE EXTENSION` if it does not exist. Set to empty string to skip.

- **Default**: `"ducklake_superuser"`
- **Access**: Requires restart (superuser-only)

### `ducklake.writer_role`

Role with DML access (SELECT/INSERT/UPDATE/DELETE) to DuckLake tables. Created during `CREATE EXTENSION` if it does not exist. Set to empty string to skip.

- **Default**: `"ducklake_writer"`
- **Access**: Requires restart (superuser-only)

### `ducklake.reader_role`

Role with SELECT-only access to DuckLake tables. Created during `CREATE EXTENSION` if it does not exist. Set to empty string to skip.

- **Default**: `"ducklake_reader"`
- **Access**: Requires restart (superuser-only)

See [access_control.md](access_control.md) for role usage details.

### DuckLake Catalog Options

Set via `CALL ducklake.set_option(name, value [, scope])`. The optional `scope` parameter limits the option to a specific table (`'my_table'::regclass`) or schema (`'my_schema'::regnamespace`). Without `scope`, the option applies globally; the most specific scope set wins (table > schema > global).

These options are managed by the DuckLake catalog and stored in metadata tables, not in `postgresql.conf`.

### `auto_compact`

Whether a table is targeted when compaction runs without a specific table argument (e.g. by the background maintenance worker).

- **Default**: `true`

### `created_by`

Informational tag recording the tool that wrote the DuckLake. No functional effect.

- **Default**: unset

### `data_inlining_row_limit`

Number of rows to keep inlined in the metadata catalog before writing to Parquet files. Small inserts are stored inline for better performance; `0` disables inlining.

- **Default**: `10`

```sql
CALL ducklake.set_option('data_inlining_row_limit', 100);
CALL ducklake.set_option('data_inlining_row_limit', 50, 'my_table'::regclass);
```

### `data_path`

Directory where data files are written. Usually set per-session with the [`ducklake.default_table_path`](#ducklakedefault_table_path) GUC or per-table with `WITH (ducklake.table_path = ...)`.

- **Default**: unset

### `parquet_compression`

Compression algorithm for newly written Parquet files.

- **Default**: `snappy`
- **Values**: `uncompressed`, `snappy`, `gzip`, `zstd`, `brotli`, `lz4`, `lz4_raw`

### `parquet_compression_level`

Compression level (codec-specific, e.g., 1--22 for zstd).

- **Default**: `3`

### `parquet_version`

Parquet format version.

- **Default**: `1`
- **Values**: `1`, `2`

### `parquet_row_group_size`

Number of rows per Parquet row group.

- **Default**: `122880`

### `parquet_row_group_size_bytes`

Maximum size of a Parquet row group (e.g., `'64MB'`).

- **Default**: unset

### `target_file_size`

Target size for data files written by insertion and compaction (e.g., `'128MB'`).

- **Default**: `512MB`

### `per_thread_output`

Whether each thread outputs to separate files during parallel writes.

- **Default**: `false`

### `hive_file_pattern`

Whether partitioned data uses a Hive-style folder layout.

- **Default**: `true`

### `rewrite_delete_threshold`

Fraction of deleted rows (0.0--1.0) before a file is rewritten during maintenance.

- **Default**: `0.95`

### `sort_on_insert`

Whether `INSERT` sorts data according to the table's `SET SORTED BY` order.

- **Default**: `true`

### `require_commit_message`

Whether to require a commit message when creating snapshots.

- **Default**: `false`

### `delete_older_than`

Required age of unused files before cleanup removes them (e.g., `'24 hours'`).

- **Default**: unset

### `expire_older_than`

Required age of snapshots before expiration (e.g., `'7 days'`).

- **Default**: unset
