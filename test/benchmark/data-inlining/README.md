# Data Inlining Benchmark

Compares **pg_ducklake** (all-in-one PG extension) vs **DuckDB + DuckLake**
(standalone DuckDB with PG catalog) using real
[ClickBench HITS](https://github.com/ClickHouse/ClickBench) data.

Inspired by the [DuckLake data inlining blog post](https://ducklake.select/2026/04/02/data-inlining-in-ducklake/).
Default settings match the blog: 300k rows, batches of 10.

## What it measures

1. **Batch insert** -- streaming 10-row batches from pyarrow via client-side COPY/INSERT
2. **Query before flush** -- 8 ClickBench aggregation queries on inlined data
3. **Flush** -- `flush_inlined_data()` materializes rows to Parquet
4. **Query after flush** -- same queries on Parquet files

## Quick start

```bash
# Scenario 1: pg_ducklake
bash setup_pg_ducklake.sh          # downloads HITS partition (~140 MB), starts Docker
uv run bench_pg_ducklake.py > results_pg_ducklake.json

# Scenario 2: DuckDB + DuckLake (PG catalog)
bash setup_duckdb_ducklake.sh      # downloads HITS partition, starts catalog PG
uv run bench_duckdb_ducklake.py > results_duckdb_ducklake.json
```

## Configuration (env vars)

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCH_TOTAL_ROWS` | `300000` | Rows to load from HITS partition |
| `BENCH_BATCH_SIZE` | `10` | Rows per INSERT batch (blog default) |
| `BENCH_PG_CONNSTR` | `host=localhost ...` | pg_ducklake connection string |
| `BENCH_PG_CATALOG_CONNSTR` | `host=localhost port=5433 ...` | Catalog PG for standalone DuckDB |
| `BENCH_HITS_FILE` | `/tmp/hits_0.parquet` | Local HITS parquet path |
| `BENCH_HITS_FILE_HOST` | `/tmp/hits_0.parquet` | Host-side parquet (pg_ducklake reads from mount) |
| `BENCH_DATA_PATH` | `(tmpdir)` | Parquet data dir (standalone only, auto-cleaned) |

## Data source

Uses one partition (~140 MB, ~1M rows) of the ClickBench HITS dataset:
`https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet`

## CI

Runs as `data_inlining_bench` in the Docker workflow, parallel to ClickBench.
Non-blocking (`continue-on-error`), PG 18 amd64 only, 30k rows / batch 10.
