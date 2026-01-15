<div align="center">

# pg_ducklake

PostgreSQL Extension for DuckLake

[![dockerhub](https://img.shields.io/docker/pulls/pgducklake/pgducklake?logo=docker)](https://hub.docker.com/r/pgducklake/pgducklake)
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/relytcloud/pg_ducklake/blob/main/LICENSE)

</div>

_This project is under high development and is not yet ready for production use._

**pg_ducklake** brings a native datalake experience into PostgreSQL, powered by [DuckLake](https://ducklake.select/) (a DuckDB lakehouse format with SQL catalog metadata and open Parquet data files).

## Key Features

- **Managed DuckLake tables**: Create/Write/Query DuckLake tables in PostgreSQL via SQL (e.g., psql/JDBC).
- **DuckDB compatibility**: tables created by `pg_ducklake` are directly queryable from DuckDB clients.
- **Cloud storage**: store data files in AWS S3 (or GCS, R2) to decouple storage and compute for serverless analytics.
- **Fast analytics**: columnar storage + DuckDB vectorized execution, with hybrid queries over PostgreSQL heap tables supported.

## See it in action

### Your first Data Lake in PostgreSQL

```sql
-- Use local filesystem.
SELECT ducklake.create_metadata();
-- Or use AWS S3 as data storage.
-- SELECT ducklake.create_metadata('s3://my-bucket/prefix/');

CREATE TABLE my_table (
    id INT,
    name TEXT,
    age INT
) USING ducklake;

INSERT INTO my_table VALUES (1, 'Alice', 25), (2, 'Bob', 30);

SELECT * FROM my_table;
```

### Access your data with DuckDB

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:postgres://postgres:duckdb@localhost:5432/postgres' AS my_ducklake (METADATA_SCHEMA 'ducklake');
SELECT * FROM my_ducklake.my_table;
```

**Known limitation**: When using local filesystem, DuckDB might not able to access your data.

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

_See [compilation guide](docs/compilation.md) for detailed instructions._

## Usecases

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
SELECT * FROM read_csv('https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv');

SELECT "Pclass", "Sex", COUNT(*), AVG("Survived") AS survival_rate
FROM titanic
GROUP BY "Pclass", "Sex";
```

## Roadmap

### Features

- [x] INSERT / SELECT / DELETE / UPDATE for DuckLake tables
- [ ] Online schema evolution (ADD COLUMN / DROP COLUMN / type promotion)
- [ ] Time-travel queries
- [ ] Partitioned tables
- [ ] Read-only `pg_ducklake` tables referencing shared DuckLake datasets (e.g., frozen DuckLake)
- [ ] Table maintenance (e.g., compaction / GC) via PostgreSQL (e.g., VACUUM or UDFs) [^]
- [ ] HTAP support for incremental row-store → column-store conversion (PostgreSQL heap → DuckLake)
- [ ] Complex types

> [^]: Table maintenance can be carried out by standalone DuckDB clients (preferable, since it is serverless and avoids burdening the PostgreSQL server); `pg_ducklake` still plans to expose these operations for ease of use.

### Performance

- [ ] Native inlined (heap) table for small writes
- [ ] Better transaction concurrency model (based on PostgreSQL XID)
- [ ] Faster metadata operations via PostgreSQL native functions (e.g., SPI)

### Docs

- [ ] Access control behavior for DuckLake tables [^]

> [^]: DuckLake tables are exposed via PostgreSQL table access methods (AM), so PostgreSQL table/column privileges may already apply; the current behavior and gaps will be reviewed and documented. DuckLake itself relies on its metadata service for ACL management.

## Contributing

We welcome contributions! Please see:

- [Contributing Guidelines](CONTRIBUTING.md)
- [Issues](https://github.com/relytcloud/pg_ducklake/issues) for bug reports

## Acknowledgments

This project is forked from [pg_duckdb](https://github.com/duckdb/pg_duckdb), and integrating with [ducklake](https://github.com/duckdb/ducklake).
