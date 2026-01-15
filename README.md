<div align="center">

# pg_ducklake

PostgreSQL Extension for DuckLake

[![dockerhub](https://img.shields.io/docker/pulls/pgducklake/pgducklake?logo=docker)](https://hub.docker.com/r/pgducklake/pgducklake)
[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/relytcloud/pg_ducklake/blob/main/LICENSE)

</div>

_This project is under high development and is not yet ready for production use._

**pg_ducklake** integrates [DuckLake](https://ducklake.select) lakehouse format and DuckDB's columnar-vectorized analytics engine into PostgreSQL, enabling columnar storage and lakehouse scalability.

## Key Features

- **Write and analyze DuckLake** just in PostgreSQL, or anywhere with DuckDB.
- **Columnar storage** in PostgreSQL table interface. Data files (parquet) could be stored in local filesystem or cloud storage (S3, GCS, Azure, R2).
- **Performance boost from DuckDB**!

## See it in action

### Your first Data Lake in PostgreSQL

```sql
-- Use AWS S3 as data storage.
SELECT ducklake.create_metadata('s3://my-bucket/prefix/');
-- Or defaultly use local filesystem.
-- SELECT ducklake.create_metadata();

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
docker run -d -e POSTGRES_PASSWORD=duckdb -name pgducklake pgducklake/pgducklake:18-main

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

## Features

- Local filesystem and S3-compatible object stores (MinIO, S3)

## Milestones

- Complete docs
- Performance
    - Bypass access to metadata tables
    - Bypass inline table writes
    - Direct parquet writes
- Fine-grained access control

## Contributing

We welcome contributions! Please see:

- [Contributing Guidelines](CONTRIBUTING.md)
- [Issues](https://github.com/relytcloud/pg_ducklake/issues) for bug reports

## Acknowledgments

This project is forked from [pg_duckdb](https://github.com/duckdb/pg_duckdb), and integrating with [ducklake](https://github.com/duckdb/ducklake).
