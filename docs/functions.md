# Functions

All functions and procedures are installed into the `ducklake` schema.

## Table Management

| Name | Type | Description |
| :--- | :--- | :---------- |
| [`set_option`](#set_option) | procedure | Set a DuckLake option |
| [`options`](#options) | function | List all DuckLake options and their values |
| [`flush_inlined_data`](#flush_inlined_data) | procedure | Flush inlined data rows to Parquet files |
| [`set_partition`](#set_partition) | procedure | Set file-level partitioning on a table |
| [`reset_partition`](#reset_partition) | procedure | Remove partitioning from a table |
| [`get_partition`](#get_partition) | function | Show partition keys for a table |
| [`freeze`](#freeze) | procedure | Export metadata to a standalone `.ducklake` file |
| [`ducklake_cleanup_old_files`](#ducklake_cleanup_old_files) | function | Clean up old data files |

## Time Travel

| Name | Type | Description |
| :--- | :--- | :---------- |
| [`time_travel`](#time_travel) | function | Query a table at a previous version or timestamp |

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

#### <a name="flush_inlined_data"></a>`ducklake.flush_inlined_data(scope regclass DEFAULT NULL)`

Flushes inlined data rows to Parquet files. When `scope` is provided, only that table is flushed.

```sql
-- Flush all tables
CALL ducklake.flush_inlined_data();

-- Flush a specific table
CALL ducklake.flush_inlined_data('my_table'::regclass);
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

#### <a name="ducklake_cleanup_old_files"></a>`ducklake.ducklake_cleanup_old_files(older_than interval DEFAULT NULL)` -> `bigint`

Cleans up old data files that are no longer referenced by the current snapshot. When `older_than` is provided, only files older than the given interval are cleaned up. When NULL, all scheduled files are cleaned.

```sql
-- Clean up files older than 24 hours
SELECT ducklake.ducklake_cleanup_old_files('24 hours'::interval);

-- Clean up all old files
SELECT ducklake.ducklake_cleanup_old_files();
```

#### <a name="time_travel"></a>`ducklake.time_travel(table_name text, version bigint)` / `ducklake.time_travel(table_name text, timestamp timestamptz)` -> `SETOF duckdb.row`

Queries a DuckLake table at a previous version or timestamp. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
-- Query by version number
SELECT * FROM ducklake.time_travel('my_table', 1);

-- Query by timestamp
SELECT * FROM ducklake.time_travel('my_table', '2024-01-01'::timestamptz);
```
