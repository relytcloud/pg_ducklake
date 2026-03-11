# Functions

All functions and procedures are installed into the `ducklake` schema.

## Table Management

| Name | Type | Description |
| :--- | :--- | :---------- |
| [`cleanup_old_files`](#cleanup_old_files) | function | Clean up old data files |
| [`flush_inlined_data`](#flush_inlined_data) | function | Flush inlined data rows to Parquet files |
| [`freeze`](#freeze) | procedure | Export metadata to a standalone `.ducklake` file |
| [`get_partition`](#get_partition) | function | Show partition keys for a table |
| [`options`](#options) | function | List all DuckLake options and their values |
| [`reset_partition`](#reset_partition) | procedure | Remove partitioning from a table |
| [`set_option`](#set_option) | procedure | Set a DuckLake option |
| [`set_partition`](#set_partition) | procedure | Set file-level partitioning on a table |

## Time Travel

| Name | Type | Description |
| :--- | :--- | :---------- |
| [`time_travel`](#time_travel) | function | Query a table at a previous version or timestamp |

## Snapshots

| Name | Type | Description |
| :--- | :--- | :---------- |
| [`current_snapshot`](#current_snapshot) | function | Get current snapshot ID |
| [`last_committed_snapshot`](#last_committed_snapshot) | function | Get latest committed snapshot |
| [`snapshots`](#snapshots) | function | List all snapshots and changesets |

## Data Change Feed

| Name | Type | Description |
| :--- | :--- | :---------- |
| [`table_changes`](#table_changes) | function | Query all changes between snapshots |
| [`table_deletions`](#table_deletions) | function | Query deleted rows between snapshots |
| [`table_insertions`](#table_insertions) | function | Query inserted rows between snapshots |

## Metadata

| Name | Type | Description |
| :--- | :--- | :---------- |
| [`list_files`](#list_files) | function | List data/delete files for a table |
| [`table_info`](#table_info) | function | List table metadata |

## Detailed Descriptions

#### <a name="set_option"></a>`ducklake.set_option(option_name text, value "any", scope regclass DEFAULT NULL)`

Sets a DuckLake catalog option. When `scope` is provided, the option applies only to that table.

```sql
-- Set global option
CALL ducklake.set_option('data_inlining_row_limit', 100);

-- Set table-scoped option
CALL ducklake.set_option('data_inlining_row_limit', 50, 'my_table'::regclass);
```

#### <a name="options"></a>`ducklake.options()` -> `SETOF record`

Lists all DuckLake options with their current values. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
SELECT * FROM ducklake.options();
```

#### <a name="flush_inlined_data"></a>`ducklake.flush_inlined_data()` / `ducklake.flush_inlined_data(schema_name text, table_name text)` / `ducklake.flush_inlined_data(scope regclass)` -> `SETOF duckdb.row`

Flushes inlined data rows to Parquet files. When a table is specified, only that table is flushed. Accepts either a `regclass` table reference or explicit schema/table text arguments. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
-- Flush all tables
SELECT * FROM ducklake.flush_inlined_data();

-- Flush a specific table (regclass)
SELECT * FROM ducklake.flush_inlined_data('my_table'::regclass);

-- Flush a specific table (text-arg form)
SELECT * FROM ducklake.flush_inlined_data('public', 'my_table');
```

#### <a name="set_partition"></a>`ducklake.set_partition(scope regclass, VARIADIC partition_by text[])`

Sets file-level partitioning on a DuckLake table. Each partition key is a separate argument. Supports DuckLake transforms: `year`, `month`, `day`, `hour`.

Partitioning can be set on tables that already contain data -- existing files remain unpartitioned while new inserts are written into partitioned files.

```sql
-- Single column
CALL ducklake.set_partition('my_table'::regclass, 'category');

-- Multiple columns
CALL ducklake.set_partition('my_table'::regclass, 'a', 'b');

-- With transforms
CALL ducklake.set_partition('events'::regclass, 'year(ts)', 'month(ts)');
```

#### <a name="reset_partition"></a>`ducklake.reset_partition(scope regclass)`

Removes partitioning from a DuckLake table. Existing partitioned files remain as-is; new inserts are no longer partitioned.

```sql
CALL ducklake.reset_partition('my_table'::regclass);
```

#### <a name="get_partition"></a>`ducklake.get_partition(scope regclass)` -> `SETOF record(partition_key_index, column_name, transform)`

Returns the active partition keys for a DuckLake table. Returns zero rows if the table is not partitioned.

```sql
SELECT * FROM ducklake.get_partition('events'::regclass);
 partition_key_index | column_name | transform
---------------------+-------------+-----------
                   0 | ts          | year
                   1 | ts          | month
```

#### <a name="freeze"></a>`ducklake.freeze(output_path text)`

Exports the DuckLake catalog metadata to a standalone `.ducklake` file. If data inlining is enabled, call `flush_inlined_data()` first to ensure all rows are materialized as Parquet files.

```sql
CALL ducklake.freeze('/path/to/output.ducklake');
```

#### <a name="cleanup_old_files"></a>`ducklake.cleanup_old_files()` / `ducklake.cleanup_old_files(older_than interval)` -> `SETOF duckdb.row`

Cleans up old data files that are no longer referenced by the current snapshot. When `older_than` is provided, only files older than the given interval are cleaned up. Without arguments, all scheduled files are cleaned. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
-- Clean up files older than 24 hours
SELECT * FROM ducklake.cleanup_old_files('24 hours'::interval);

-- Clean up all old files
SELECT * FROM ducklake.cleanup_old_files();
```

#### <a name="time_travel"></a>`ducklake.time_travel(table_name text, version bigint)` / `ducklake.time_travel(table_name text, timestamp timestamptz)` -> `SETOF duckdb.row`

Queries a DuckLake table at a previous version or timestamp. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
-- Query by version number
SELECT * FROM ducklake.time_travel('my_table', 1);

-- Query by timestamp
SELECT * FROM ducklake.time_travel('my_table', '2024-01-01'::timestamptz);
```

#### <a name="snapshots"></a>`ducklake.snapshots()` -> `SETOF duckdb.row`

Lists all snapshots and changesets. Returns snapshot metadata including snapshot IDs, timestamps, and changeset information. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
SELECT * FROM ducklake.snapshots();
```

#### <a name="current_snapshot"></a>`ducklake.current_snapshot()` -> `SETOF duckdb.row`

Returns the current snapshot ID. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
SELECT * FROM ducklake.current_snapshot();
```

#### <a name="last_committed_snapshot"></a>`ducklake.last_committed_snapshot()` -> `SETOF duckdb.row`

Returns the latest committed snapshot. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
SELECT * FROM ducklake.last_committed_snapshot();
```

#### <a name="table_insertions"></a>`ducklake.table_insertions(scope regclass, ...)` / `ducklake.table_insertions(schema_name text, table_name text, ...)` -> `SETOF duckdb.row`

Queries rows inserted into a table between two snapshots (by version or timestamp). Accepts either a `regclass` table reference or explicit schema/table text arguments.

```sql
-- By version (regclass)
SELECT * FROM ducklake.table_insertions('my_table'::regclass, 1, 5);

-- By timestamp (regclass)
SELECT * FROM ducklake.table_insertions('my_table'::regclass, '2024-01-01'::timestamptz, now());

-- Text-arg form
SELECT * FROM ducklake.table_insertions('public', 'my_table', 1, 5);
```

#### <a name="table_deletions"></a>`ducklake.table_deletions(scope regclass, ...)` / `ducklake.table_deletions(schema_name text, table_name text, ...)` -> `SETOF duckdb.row`

Queries rows deleted from a table between two snapshots (by version or timestamp). Accepts either a `regclass` table reference or explicit schema/table text arguments.

```sql
-- By version (regclass)
SELECT * FROM ducklake.table_deletions('my_table'::regclass, 1, 5);

-- By timestamp (regclass)
SELECT * FROM ducklake.table_deletions('my_table'::regclass, '2024-01-01'::timestamptz, now());

-- Text-arg form
SELECT * FROM ducklake.table_deletions('public', 'my_table', 1, 5);
```

#### <a name="table_changes"></a>`ducklake.table_changes(scope regclass, ...)` / `ducklake.table_changes(schema_name text, table_name text, ...)` -> `SETOF duckdb.row`

Queries all changes (insertions and deletions) to a table between two snapshots (by version or timestamp). Each row includes a `change_type` column: `insert`, `delete`, `update_preimage`, or `update_postimage`. Accepts either a `regclass` table reference or explicit schema/table text arguments.

```sql
-- By version (regclass)
SELECT * FROM ducklake.table_changes('my_table'::regclass, 1, 5);

-- By timestamp (regclass)
SELECT * FROM ducklake.table_changes('my_table'::regclass, '2024-01-01'::timestamptz, now());

-- Text-arg form
SELECT * FROM ducklake.table_changes('public', 'my_table', 1, 5);
```

#### <a name="list_files"></a>`ducklake.list_files(scope regclass)` / `ducklake.list_files(schema_name text, table_name text)` -> `SETOF duckdb.row`

Lists data and delete files for a DuckLake table. Accepts either a `regclass` table reference or explicit schema/table text arguments.

```sql
-- Regclass form
SELECT * FROM ducklake.list_files('my_table'::regclass);

-- Text-arg form
SELECT * FROM ducklake.list_files('public', 'my_table');
```

#### <a name="table_info"></a>`ducklake.table_info()` -> `SETOF duckdb.row`

Lists metadata for all tables in the DuckLake catalog. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
SELECT * FROM ducklake.table_info();
```
