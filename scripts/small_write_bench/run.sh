#!/bin/bash
# Run the small write benchmark for DuckLake inlined tables.
# This benchmark randomly samples ClickBench hits data from hits_0.parquet
# and writes it to PostgreSQL using INSERT ... UNNEST methods.
#
# Usage:
#   ./run.sh [--total-rows N] [--batch LIST] [--limit LIST] [--connstr "..."]
#
# Options:
#   --total-rows N       Total number of rows to insert (default: 1000000)
#   --batch LIST         Comma-separated list of batch sizes for insertions (default: 100)
#   --limit LIST         Comma-separated list of limit sizes to test (default: 0,10000)
#   --connstr "..."      PostgreSQL connection string (default: "dbname=postgres")
#
# Examples:
#   ./run.sh --total-rows 50000 --batch 50
#   ./run.sh --limit "0,5000,10000"
set -eu

BASE_DIR=$(dirname "$0")
BENCH_SCRIPT="$BASE_DIR/benchmark.py"

# Check for uv
if ! which uv >/dev/null 2>&1; then
    echo "Error: 'uv' is required but not found. Please install it to manage dependencies."
    exit 1
fi

# Download dataset
if [ ! -f $BASE_DIR/hits_0.parquet ]; then
    if ! which wget >/dev/null 2>&1; then
        echo "Error: 'wget' is required but not found. Please install it to download the dataset."
        exit 1
    fi
    wget --continue -O $BASE_DIR/hits_0.parquet https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet
fi

echo "Initializing Small Write Benchmark..."
echo "Loading data from hits_0.parquet and randomly sampling batches..."
uv run "$BENCH_SCRIPT" "$@"
