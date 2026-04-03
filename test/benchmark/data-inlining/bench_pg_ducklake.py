#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]", "pyarrow"]
# ///
#
# Data inlining benchmark for pg_ducklake.
#
# Reads a ClickBench HITS partition via pyarrow, streams 10-row batches
# into a ducklake table using INSERT ... SELECT UNNEST() with the direct
# insert path (SPI bypass), then measures aggregation query latency
# before and after flushing inlined data to Parquet files.
#
# Sibling script: bench_duckdb_ducklake.py (queries kept in sync via hits_schema.py).
#
# Environment variables:
#   BENCH_TOTAL_ROWS      - rows to load (default 300000)
#   BENCH_BATCH_SIZE      - rows per INSERT batch (default 10)
#   BENCH_PG_CONNSTR      - libpq connection string
#   BENCH_HITS_FILE_HOST  - parquet path on host (default /tmp/hits_0.parquet)
#
# Progress -> stderr; JSON results -> stdout.

import json
import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from psycopg import ClientCursor

sys.path.insert(0, os.path.dirname(__file__))
from hits_schema import (
    CREATE_TABLE_SQL, PREPARE_SQL, EXECUTE_SQL, QUERIES_PG,
    convert_batch_params,
)

TOTAL_ROWS = int(os.environ.get("BENCH_TOTAL_ROWS", "300000"))
BATCH_SIZE = int(os.environ.get("BENCH_BATCH_SIZE", "10"))
PG_CONNSTR = os.environ.get(
    "BENCH_PG_CONNSTR",
    "host=localhost port=5432 dbname=postgres user=postgres password=duckdb",
)
HITS_FILE = os.environ.get("BENCH_HITS_FILE_HOST", "/tmp/hits_0.parquet")


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def run_queries(conn):
    times = []
    for q in QUERIES_PG:
        t0 = time.monotonic()
        conn.execute(q).fetchall()
        times.append(time.monotonic() - t0)
    return times


def coerce_binary_to_string(table):
    """Cast binary columns to string to match binary_as_string => true."""
    cols = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
            cols.append(col.cast(pa.string()))
        else:
            cols.append(col)
    return pa.table(dict(zip(table.column_names, cols)))


def main():
    log(f"Reading {TOTAL_ROWS} rows from {HITS_FILE}...")
    t0 = time.monotonic()
    table = coerce_binary_to_string(pq.read_table(HITS_FILE).slice(0, TOTAL_ROWS))
    stage_sec = time.monotonic() - t0
    actual_rows = len(table)
    log(f"  loaded {actual_rows} rows ({table.num_columns} cols) in {stage_sec:.1f}s")

    conn = psycopg.connect(PG_CONNSTR, autocommit=True)

    conn.execute("DROP TABLE IF EXISTS hits")
    conn.execute(CREATE_TABLE_SQL)
    conn.execute(
        f"CALL ducklake.set_option('data_inlining_row_limit', {actual_rows + 1})"
    )
    conn.execute("SET ducklake.enable_direct_insert = true")
    conn.execute(PREPARE_SQL)

    num_batches = (actual_rows + BATCH_SIZE - 1) // BATCH_SIZE
    log(f"Inserting {actual_rows} rows in {num_batches} batches of {BATCH_SIZE} "
        f"(direct insert)...")

    t0 = time.monotonic()
    with ClientCursor(conn) as cur:
        for i in range(0, actual_rows, BATCH_SIZE):
            batch = table.slice(i, min(BATCH_SIZE, actual_rows - i))
            params = convert_batch_params(
                [col.to_pylist() for col in batch.columns]
            )
            try:
                cur.execute(EXECUTE_SQL, params)
            except psycopg.errors.InternalError_:
                # pg_duckdb plan cache invalidation; re-prepare and retry
                conn.execute("DEALLOCATE di")
                conn.execute(PREPARE_SQL)
                cur.execute(EXECUTE_SQL, params)
    insert_sec = time.monotonic() - t0
    log(f"  inserted in {insert_sec:.1f}s ({actual_rows / insert_sec:.0f} rows/s)")

    log("Running queries before flush...")
    pre_flush = run_queries(conn)
    log(f"  total: {sum(pre_flush):.3f}s")

    log("Flushing inlined data...")
    t0 = time.monotonic()
    try:
        conn.execute("CALL ducklake.flush_inlined_data()")
    except psycopg.errors.WrongObjectType:
        conn.execute("SELECT * FROM ducklake.flush_inlined_data()").fetchall()
    flush_sec = time.monotonic() - t0
    log(f"  flushed in {flush_sec:.3f}s")

    log("Running queries after flush...")
    post_flush = run_queries(conn)
    log(f"  total: {sum(post_flush):.3f}s")

    results = {
        "scenario": "pg_ducklake",
        "config": {
            "total_rows": actual_rows,
            "batch_size": BATCH_SIZE,
            "num_batches": num_batches,
        },
        "results": {
            "stage_data_sec": round(stage_sec, 3),
            "insert_sec": round(insert_sec, 3),
            "insert_rows_per_sec": round(actual_rows / insert_sec),
            "queries_before_flush_sec": round(sum(pre_flush), 3),
            "queries_before_flush_detail": [round(t, 3) for t in pre_flush],
            "flush_sec": round(flush_sec, 3),
            "queries_after_flush_sec": round(sum(post_flush), 3),
            "queries_after_flush_detail": [round(t, 3) for t in post_flush],
        },
    }
    print(json.dumps(results, indent=2))

    conn.execute("DROP TABLE IF EXISTS hits")
    conn.close()


if __name__ == "__main__":
    main()
