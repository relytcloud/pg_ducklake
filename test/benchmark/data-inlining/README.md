# Data Inlining Benchmark

Compares three scenarios using real
[ClickBench HITS](https://github.com/ClickHouse/ClickBench) data:

| Scenario | Insert path | Query engine |
|----------|------------|--------------|
| **pg_heap** (baseline) | Regular PG heap INSERT | PostgreSQL seq scan |
| **pg_ducklake** | Direct insert (UNNEST, SPI bypass) | DuckDB via pg_duckdb |
| **DuckDB + DuckLake** | DuckDB with PG catalog | DuckDB native |

Inspired by the [DuckLake data inlining blog post](https://ducklake.select/2026/04/02/data-inlining-in-ducklake/).
Default settings match the blog: 300k rows, batches of 10.

## What it measures

1. **Batch insert** -- streaming 10-row batches from pyarrow
2. **Query before flush** -- all 43 ClickBench queries (Q0-Q42) on inlined data
3. **Flush** -- `flush_inlined_data()` materializes rows to Parquet (ducklake only)
4. **Query after flush** -- same 43 queries on Parquet files (ducklake only)

## Quick start

```bash
# Setup (downloads HITS partition ~140 MB, starts Docker containers)
bash setup_pg_ducklake.sh
bash setup_duckdb_ducklake.sh

# Run all three scenarios
BENCH_MODE=heap     uv run bench_pg_ducklake.py > results_heap.json
BENCH_MODE=ducklake uv run bench_pg_ducklake.py > results_ducklake.json
uv run bench_duckdb_ducklake.py > results_duckdb.json
```

## Configuration (env vars)

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCH_MODE` | `ducklake` | `ducklake` or `heap` (pg_ducklake script only) |
| `BENCH_TOTAL_ROWS` | `300000` | Rows to load from HITS partition |
| `BENCH_BATCH_SIZE` | `10` | Rows per INSERT batch (blog default) |
| `BENCH_PG_CONNSTR` | `host=localhost ...` | pg_ducklake / heap connection string |
| `BENCH_PG_CATALOG_CONNSTR` | `host=localhost port=5433 ...` | Catalog PG for standalone DuckDB |
| `BENCH_HITS_FILE_HOST` | `/tmp/hits_0.parquet` | Host-side parquet path |
| `BENCH_DATA_PATH` | `(tmpdir)` | Parquet data dir (standalone DuckDB, auto-cleaned) |

## Data source

Uses one partition (~140 MB, ~1M rows) of the ClickBench HITS dataset:
`https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet`

## CI

Runs as `data_inlining_bench` in the Docker workflow, parallel to ClickBench.
Non-blocking (`continue-on-error`), PG 18 amd64 only, 30k rows / batch 10.
Runs all three scenarios: pg_heap, pg_ducklake, DuckDB+DuckLake.
