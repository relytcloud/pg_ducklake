# SQL Objects

All objects created by `pg_ducklake--0.1.0.sql`.

## Schema

| Name |
|------|
| `ducklake` |

## Table Access Method

| Access Method | Handler |
|---------------|---------|
| `ducklake` | `ducklake._am_handler(internal)` |

## Index Access Method

| Access Method | Handler | Purpose |
|---------------|---------|---------|
| `ducklake_sorted` | `ducklake._sorted_am_handler(internal)` | Sorted table marker; intercepted by utility hook |

Default operator classes are registered for common types (bool, int2, int4, int8, float4, float8, numeric, text, varchar, bpchar, date, timestamp, timestamptz, interval, uuid, oid, bytea) in the `ducklake.sorted_ops` operator family.

## Types

| Type | Purpose |
|------|---------|
| `ducklake.variant` | DuckDB VARIANT column type for ducklake tables; PG stores text representation, DuckDB handles actual data |

## Event Triggers

| Event Trigger | Handler | Event | Tags |
|---------------|---------|-------|------|
| `ducklake_create_table_trigger` | `ducklake._create_table_trigger()` | `ddl_command_end` | CREATE TABLE, CREATE TABLE AS |
| `ducklake_drop_table_trigger` | `ducklake._drop_table_trigger()` | `sql_drop` | (all) |
| `ducklake_alter_table_trigger` | `ducklake._alter_table_trigger()` | `ddl_command_end` | ALTER TABLE |
| *(created at runtime)* | `ducklake._snapshot_trigger()` | row-level trigger | DuckDB-to-PG catalog sync |

## Foreign Data Wrapper

| FDW | Handler | Validator |
|-----|---------|-----------|
| `ducklake_fdw` | `ducklake._fdw_handler()` | `ducklake._fdw_validator(text[], oid)` |

## Functions & Procedures

| Group | Function / Procedure | Kind | Regclass Overload |
|-------|----------------------|------|-------------------|
| Options | [`ducklake.set_option(text, "any")`](#set_option) | duckdb-only proc | `(text, "any", regclass)` |
| | [`ducklake.options()`](#options) | passthrough | - |
| Flush | [`ducklake.flush_inlined_data()`](#flush_inlined_data) | passthrough | - |
| | [`ducklake.flush_inlined_data(text, text)`](#flush_inlined_data) | passthrough | `(regclass)` -- rewrite |
| Partitioning | [`ducklake.set_partition(regclass, VARIADIC text[])`](#set_partition) | native proc | - |
| | [`ducklake.reset_partition(regclass)`](#reset_partition) | native proc | - |
| | [`ducklake.get_partition(regclass)`](#get_partition) | pure SQL | - |
| Sorted Tables | [`CREATE INDEX ... USING ducklake_sorted`](#ducklake_sorted) | index intercept | - |
| | [`ducklake.set_sort(regclass, VARIADIC text[])`](#set_sort) | native proc | - |
| | [`ducklake.reset_sort(regclass)`](#reset_sort) | native proc | - |
| | [`ducklake.get_sort(regclass)`](#get_sort) | pure SQL | - |
| Snapshots | [`ducklake.snapshots()`](#snapshots) | passthrough | - |
| | [`ducklake.current_snapshot()`](#current_snapshot) | passthrough | - |
| | [`ducklake.last_committed_snapshot()`](#last_committed_snapshot) | passthrough | - |
| Metadata | [`ducklake.table_info()`](#table_info) | passthrough | - |
| | [`ducklake.list_files(text, text)`](#list_files) | passthrough | `(regclass)` -- rewrite |
| Time Travel | [`ducklake.time_travel(text, bigint)`](#time_travel) | passthrough | - |
| | [`ducklake.time_travel(text, timestamptz)`](#time_travel) | passthrough | - |
| Change Feed | [`ducklake.table_insertions(text, text, bigint, bigint)`](#table_insertions) | passthrough | `(regclass, bigint, bigint)` -- rewrite |
| | [`ducklake.table_insertions(text, text, timestamptz, timestamptz)`](#table_insertions) | passthrough | `(regclass, timestamptz, timestamptz)` -- rewrite |
| | [`ducklake.table_deletions(text, text, bigint, bigint)`](#table_deletions) | passthrough | `(regclass, bigint, bigint)` -- rewrite |
| | [`ducklake.table_deletions(text, text, timestamptz, timestamptz)`](#table_deletions) | passthrough | `(regclass, timestamptz, timestamptz)` -- rewrite |
| | [`ducklake.table_changes(text, text, bigint, bigint)`](#table_changes) | passthrough | `(regclass, bigint, bigint)` -- rewrite |
| | [`ducklake.table_changes(text, text, timestamptz, timestamptz)`](#table_changes) | passthrough | `(regclass, timestamptz, timestamptz)` -- rewrite |
| Cleanup | [`ducklake.cleanup_old_files()`](#cleanup_old_files) | passthrough | - |
| | [`ducklake.cleanup_old_files(interval)`](#cleanup_old_files) | passthrough | - |
| Freeze | [`ducklake.freeze(text)`](#freeze) | native proc | - |

**Kind legend:**
- **passthrough** -- SQL stub in pg_ducklake, pg_duckdb routes the query to DuckDB as-is
- **rewrite** -- planner rewrites `regclass` to `(schema_name, table_name)` then routes to the passthrough version
- **duckdb-only proc** -- CALL is intercepted by utility hook and executed in DuckDB
- **native proc** -- procedure runs in PostgreSQL (C language)
- **pure SQL** -- executes entirely in PostgreSQL against DuckLake metadata tables
- **index intercept** -- utility hook intercepts CREATE/DROP INDEX and translates to DuckDB ALTER TABLE

## Bootstrap

| Type | Name | Purpose |
|------|------|---------|
| Function | `ducklake._initialize()` | Extension bootstrap |
| DO block | - | Call `_initialize()` at CREATE EXTENSION time |
| DO block | - | Create access-control roles and grant privileges |

---

## Detailed Descriptions

#### <a name="set_option"></a>`ducklake.set_option(option_name text, value "any")` / `ducklake.set_option(option_name text, value "any", scope regclass)`

Sets a DuckLake catalog option. When `scope` is provided, the option applies only to that table. This is a DuckDB-only procedure (routed to DuckDB for execution).

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

#### <a name="ducklake_sorted"></a>`CREATE INDEX ... USING ducklake_sorted`

Sets the sort order on a DuckLake table using standard PostgreSQL CREATE INDEX syntax. The `ducklake_sorted` index access method creates a catalog-only index in `pg_class` and translates the sort specification into `ALTER TABLE ... SET SORTED BY` in DuckDB. `DROP INDEX` resets the sort order.

The index stores no data and is never used by the planner -- it exists purely as a catalog marker for the sort configuration.

```sql
-- Single column
CREATE INDEX my_idx ON my_table USING ducklake_sorted (ts);

-- Multi-key with directions and null ordering
CREATE INDEX my_idx ON my_table USING ducklake_sorted (a ASC NULLS LAST, b DESC NULLS FIRST);

-- Expression-based sort key
CREATE INDEX my_idx ON events USING ducklake_sorted (date_trunc('day', ts));

-- Drop index resets sort order
DROP INDEX my_idx;
```

**Unsupported options:** CONCURRENTLY, UNIQUE, WHERE, INCLUDE, TABLESPACE, custom opclass, COLLATE.

**Bidirectional sync:** When an external DuckDB client sets sort keys via `ALTER TABLE ... SET SORTED BY`, the snapshot trigger creates a corresponding `ducklake_sorted` index in `pg_class`. Similarly, `ALTER TABLE ... RESET SORTED BY` from DuckDB drops the index.

#### <a name="set_sort"></a>`ducklake.set_sort(scope regclass, VARIADIC sorted_by text[])`

Sets the sort order on a DuckLake table. This is the procedure-based alternative to `CREATE INDEX ... USING ducklake_sorted`. Each sort key is a separate argument containing a column name or expression, optionally followed by `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST`. Sorting is applied during file compaction and inlined data flushing, not during direct inserts.

```sql
-- Single column, ascending
CALL ducklake.set_sort('my_table'::regclass, 'ts ASC');

-- Multiple keys with direction and null order
CALL ducklake.set_sort('my_table'::regclass, 'a ASC NULLS LAST', 'b DESC NULLS FIRST');

-- Expression-based sort key
CALL ducklake.set_sort('events'::regclass, 'date_trunc(''day'', ts) ASC');
```

#### <a name="reset_sort"></a>`ducklake.reset_sort(scope regclass)`

Removes the sort order from a DuckLake table.

```sql
CALL ducklake.reset_sort('my_table'::regclass);
```

#### <a name="get_sort"></a>`ducklake.get_sort(scope regclass)` -> `SETOF record(sort_key_index, expression, direction, null_order)`

Returns the active sort keys for a DuckLake table. Returns zero rows if the table has no sort order.

```sql
SELECT * FROM ducklake.get_sort('events'::regclass);
 sort_key_index |      expression       | direction | null_order
----------------+-----------------------+-----------+------------
              0 | date_trunc('day', ts) | ASC       | NULLS_LAST
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

#### <a name="table_info"></a>`ducklake.table_info()` -> `SETOF duckdb.row`

Lists metadata for all tables in the DuckLake catalog. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
SELECT * FROM ducklake.table_info();
```

#### <a name="list_files"></a>`ducklake.list_files(scope regclass)` / `ducklake.list_files(schema_name text, table_name text)` -> `SETOF duckdb.row`

Lists data and delete files for a DuckLake table. Accepts either a `regclass` table reference or explicit schema/table text arguments.

```sql
-- Regclass form
SELECT * FROM ducklake.list_files('my_table'::regclass);

-- Text-arg form
SELECT * FROM ducklake.list_files('public', 'my_table');
```

#### <a name="time_travel"></a>`ducklake.time_travel(table_name text, version bigint)` / `ducklake.time_travel(table_name text, timestamp timestamptz)` -> `SETOF duckdb.row`

Queries a DuckLake table at a previous version or timestamp. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
-- Query by version number
SELECT * FROM ducklake.time_travel('my_table', 1);

-- Query by timestamp
SELECT * FROM ducklake.time_travel('my_table', '2024-01-01'::timestamptz);
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

#### <a name="cleanup_old_files"></a>`ducklake.cleanup_old_files()` / `ducklake.cleanup_old_files(older_than interval)` -> `SETOF duckdb.row`

Cleans up old data files that are no longer referenced by the current snapshot. When `older_than` is provided, only files older than the given interval are cleaned up. Without arguments, all scheduled files are cleaned. This is a DuckDB-only function (routed to DuckDB for execution).

```sql
-- Clean up files older than 24 hours
SELECT * FROM ducklake.cleanup_old_files('24 hours'::interval);

-- Clean up all old files
SELECT * FROM ducklake.cleanup_old_files();
```

#### <a name="freeze"></a>`ducklake.freeze(output_path text)`

Exports the DuckLake catalog metadata to a standalone `.ducklake` file. If data inlining is enabled, call `flush_inlined_data()` first to ensure all rows are materialized as Parquet files.

```sql
CALL ducklake.freeze('/path/to/output.ducklake');
```
