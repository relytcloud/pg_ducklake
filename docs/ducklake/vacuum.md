# DuckLake Table Maintenance (VACUUM)

`pg_ducklake` supports the PostgreSQL `VACUUM` command to perform table maintenance operations. This includes compacting small files and garbage collecting deleted rows.

## Features

### 1. File Compaction (Merge Adjacent Files)
When data is ingested in small batches, it can result in many small Parquet files, which may impact query performance. `VACUUM` detects these small files and merges them into larger files to optimize read performance.

### 2. Garbage Collection (Rewrite Data Files)
When rows are deleted from a DuckLake table, `pg_ducklake` writes "delete files" (tombstones) rather than immediately modifying the data files. `VACUUM` consolidates these changes by rewriting the affected data files without the deleted rows and removing the obsolete delete files.

## Usage

You can run `VACUUM` on a DuckLake table just like a regular PostgreSQL table.

```sql
VACUUM [VERBOSE] table_name;
```

The `VERBOSE` option provides detailed output about the operations being performed.

## Examples

### Scenario 1: Merging Small Files

If you have a table with many small inserts:

```sql
CREATE TABLE vacuum_merge (a int, b text) USING ducklake;

-- Multiple small inserts create multiple files
INSERT INTO vacuum_merge VALUES (1, 'one');
INSERT INTO vacuum_merge VALUES (2, 'two');
INSERT INTO vacuum_merge VALUES (3, 'three');

-- Trigger compaction
VACUUM VERBOSE vacuum_merge;
```

After `VACUUM`, the small files will be merged into fewer, larger files.

### Scenario 2: Reclaiming Space after Deletes

If you perform deletions:

```sql
CREATE TABLE vacuum_rewrite (a int, b text) USING ducklake;
INSERT INTO vacuum_rewrite SELECT i, 'val' || i FROM generate_series(1, 100) i;

-- Delete some rows
DELETE FROM vacuum_rewrite WHERE a <= 20;

-- Trigger garbage collection
VACUUM VERBOSE vacuum_rewrite;
```

After `VACUUM`, the data files are rewritten to exclude the deleted rows, and the temporary delete markers are cleaned up.

## Configuration

### ducklake.vacuum_delete_threshold

This configuration parameter controls when `VACUUM` should rewrite a data file during garbage collection. A data file is rewritten only if the proportion of deleted rows in that file exceeds this threshold.

- **Type**: Floating point
- **Range**: 0.0 to 1.0
- **Default**: 0.1 (10%)

Example:
```sql
-- Rewrite files only if more than 20% of rows are deleted
SET ducklake.vacuum_delete_threshold = 0.2;
VACUUM vacuum_rewrite;
```
