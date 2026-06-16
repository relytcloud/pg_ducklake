<div align="center">

# pg_ducklake

Native data lakehouse in PostgreSQL, powered by [DuckLake](https://ducklake.select/) (a lakehouse format built on SQL database and Parquet files).

[![dockerhub](https://img.shields.io/docker/pulls/pgducklake/pgducklake?logo=docker)](https://hub.docker.com/r/pgducklake/pgducklake)
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/relytcloud/pg_ducklake/blob/main/LICENSE)

</div>

## Key Features

- **Managed DuckLake tables**: create, write, and query DuckLake tables in PostgreSQL via SQL (e.g. psql/JDBC), with full lakehouse features -- time travel, transactions, partitioning, and sort keys.
- **Fast Analytics**: DuckLake tables are stored columnar and analyzed by DuckDB. Hybrid queries that join them with heap tables are also supported.
- **Realtime Ingestion**: with data inlining, small writes are buffered in the catalog (fast, avoids the small files problem) and are immediately visible to all readers.
- **CDC Support**: incremental heap-to-DuckLake conversion with [pg_duckpipe](https://github.com/relytcloud/pg_duckpipe).
- **DuckDB compatibility**: tables created by `pg_ducklake` are directly queryable from DuckDB clients.

## See it in action

### Your first Data Lake in PostgreSQL

```sql
CREATE TABLE my_table (
    id INT,
    name TEXT,
    age INT
)
-- WITH (ducklake.table_path = 's3://my-bucket/prefix/') -- Or use AWS S3 as data storage.
USING ducklake;

INSERT INTO my_table VALUES (1, 'Alice', 25), (2, 'Bob', 30);

-- Each commit is a snapshot; capture the one before the DELETE.
SELECT max(snapshot_id) AS before_delete FROM ducklake.ducklake_snapshot \gset

DELETE FROM my_table WHERE id = 1;

SELECT * FROM my_table;

-- Time-travel back to the snapshot that still had Alice.
SELECT * FROM ducklake.time_travel('my_table'::regclass, :before_delete);
```

### Access your data with external DuckDB

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:postgres:dbname=postgres host=localhost' AS my_ducklake (METADATA_SCHEMA 'ducklake');
SELECT * FROM my_ducklake.public.my_table;
```

## Quick Start

### Docker

Run PostgreSQL with pg_ducklake pre-installed in a docker container:

```bash
docker run -d -e POSTGRES_PASSWORD=duckdb --name pgducklake pgducklake/pgducklake:18-main

docker exec -it pgducklake psql
```

### Compile from source

Requirements:

- **PostgreSQL**: 14, 15, 16, 17, 18
- **Operating Systems**: Ubuntu 22.04-24.04, macOS

```bash
git clone https://github.com/relytcloud/pg_ducklake
cd pg_ducklake
make install
```

_See [documentation](pg_ducklake/docs/README.md) for detailed instructions._

## Usecases

For a detailed comparison of upstream DuckLake features and what pg_ducklake currently supports, see [DuckLake Feature Coverage](pg_ducklake/docs/ducklake_feature_coverage.md).

### Convert a PostgreSQL heap table into a DuckLake table

This pattern performs a one-time ETL copy from row-store (PostgreSQL heap) tables to DuckLake (column-store) tables for fast analytics, while OLTP continues to use the original heap tables.

```sql
-- Create a PostgreSQL row-store (heap) table.
CREATE TABLE row_store_table AS
SELECT i AS id, 'hello pg_ducklake' AS msg
FROM generate_series(1, 10000) AS i;

-- Create a DuckLake column-store table via ETL.
CREATE TABLE col_store_table USING ducklake AS
SELECT *
FROM row_store_table;

-- Run analytics against the converted table.
SELECT max(id) FROM col_store_table;
```

### Load an external dataset

External datasets (e.g., CSV/Parquet) can be ingested with DuckDB readers and materialized as tables for analytics.

```sql
CREATE TABLE titanic USING ducklake AS
SELECT * FROM ducklake.read_csv('https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv');

SELECT "Pclass", "Sex", COUNT(*), AVG("Survived") AS survival_rate
FROM titanic
GROUP BY "Pclass", "Sex";
```

### Data Inlining

Small writes are buffered in the metadata catalog instead of producing a Parquet
file per insert, then flushed to Parquet in bulk. This keeps high-frequency
ingestion fast, avoids the small files problem, and the buffered rows are
immediately visible to every reader. Inlining is on by default (row limit `10`);
tune it with the `data_inlining_row_limit` option.

```sql
CALL ducklake.set_option('data_inlining_row_limit', 100);

CREATE TABLE events (id INT, kind TEXT) USING ducklake;

-- Inlined rows are immediately visible to every reader.
INSERT INTO events VALUES (1, 'login'), (2, 'click');
SELECT * FROM events ORDER BY id;

-- Flush inlined rows out to Parquet (the background worker does this too).
SELECT * FROM ducklake.flush_inlined_data('events'::regclass);
```

### Sorted Tables & Bucket Partitioning

Partition data files by column values or transforms (`bucket(N, col)`,
`year`/`month`/`day`/`hour`) so queries prune irrelevant files, and keep rows
sorted within each file for faster range scans and better compression.

```sql
CREATE TABLE measurements (
    device_id INT,
    ts TIMESTAMP,
    reading DOUBLE PRECISION
) USING ducklake;

-- Distribute files by a hash bucket on device_id and by month of the timestamp.
CALL ducklake.set_partition('measurements'::regclass, 'bucket(4, device_id)', 'month(ts)');

-- Keep rows ordered within each file (or use CALL ducklake.set_sort(...)).
CREATE INDEX ON measurements USING ducklake_sorted (device_id, ts);

INSERT INTO measurements VALUES
    (1, '2024-01-15 10:00', 21.5),
    (2, '2024-02-20 11:00', 22.1);
```

### Maintenance

DuckLake tables accumulate small files, deleted rows, and old snapshots over
time. A background worker compacts them automatically, and the same operations
are available on demand:

```sql
-- Compact small adjacent Parquet files into fewer, larger ones.
SELECT * FROM ducklake.merge_adjacent_files('my_table'::regclass);

-- Expire snapshots beyond a retention window, then delete their files.
CALL ducklake.set_option('expire_older_than', '7 days');
SELECT * FROM ducklake.expire_snapshots();
SELECT * FROM ducklake.cleanup_old_files();
```

`ducklake.rewrite_data_files()` rewrites files to physically purge rows removed
by `UPDATE`/`DELETE`. See [Settings](pg_ducklake/docs/settings.md) for the
maintenance worker GUCs.

### Cloud storage credentials

Register an S3 (or GCS/R2/Azure) secret so DuckLake can read and write data files
on object storage:

```sql
SELECT ducklake.create_s3_secret(
    's3', 'AKIA...', 'secret...',
    region => 'us-east-1', endpoint => 's3.amazonaws.com');

SET ducklake.default_table_path = 's3://my-bucket/prefix/';
```

Credentials are stored in the PostgreSQL catalog (a `FOREIGN SERVER` + per-user
`USER MAPPING` on the `ducklake_secret` foreign data wrapper) and applied to
DuckDB automatically. The `ducklake.create_s3_secret` / `ducklake.create_azure_secret`
helpers wrap the raw `CREATE SERVER` / `CREATE USER MAPPING` form; see the upstream
[DuckLake connection and secrets guide](https://ducklake.select/docs/stable/duckdb/usage/connecting)
for the full set of options.

## Documentation

See [docs/](pg_ducklake/docs/README.md) for full documentation including SQL reference, settings, access control, and feature coverage.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for how to get involved.

## Acknowledgments

This project is built with [pg_duckdb](https://github.com/duckdb/pg_duckdb) and [ducklake](https://github.com/duckdb/ducklake).
