#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]", "pyarrow"]
# ///
#
# Data inlining benchmark for pg_ducklake (and heap baseline).
#
# BENCH_MODE=ducklake (default):
#   Direct insert into a ducklake table via PREPARE/EXECUTE UNNEST.
#   Measures insert, queries before/after flush.
#
# BENCH_MODE=heap:
#   Regular INSERT into a PostgreSQL heap table (no ducklake).
#   Baseline for write throughput vs query performance tradeoff.
#
# Sibling script: bench_duckdb_ducklake.py (queries kept in sync via hits_schema.py).
#
# Environment variables:
#   BENCH_MODE            - "ducklake" (default) or "heap"
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
    CREATE_TABLE_SQL, CREATE_HEAP_TABLE_SQL,
    PREPARE_SQL, EXECUTE_SQL, QUERIES_PG,
    convert_batch_params,
)

MODE = os.environ.get("BENCH_MODE", "ducklake")
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
    cols = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
            cols.append(col.cast(pa.string()))
        else:
            cols.append(col)
    return pa.table(dict(zip(table.column_names, cols)))


def insert_ducklake(conn, table, actual_rows):
    """Direct insert via PREPARE/EXECUTE UNNEST into ducklake table."""
    conn.execute(CREATE_TABLE_SQL)
    conn.execute(
        f"CALL ducklake.set_option('data_inlining_row_limit', {actual_rows + 1})"
    )
    conn.execute("SET ducklake.enable_direct_insert = true")
    conn.execute(PREPARE_SQL)

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
                conn.execute("DEALLOCATE di")
                conn.execute(PREPARE_SQL)
                cur.execute(EXECUTE_SQL, params)
    return time.monotonic() - t0


def insert_heap(conn, table, actual_rows):
    """Regular INSERT INTO heap table via executemany."""
    conn.execute(CREATE_HEAP_TABLE_SQL)

    ncols = table.num_columns
    placeholders = ", ".join(["%s"] * ncols)
    cols_quoted = ", ".join(f'"{c}"' for c in table.column_names)
    insert_sql = f"INSERT INTO hits ({cols_quoted}) VALUES ({placeholders})"

    t0 = time.monotonic()
    with conn.cursor() as cur:
        for i in range(0, actual_rows, BATCH_SIZE):
            batch = table.slice(i, min(BATCH_SIZE, actual_rows - i))
            params = convert_batch_params(
                [col.to_pylist() for col in batch.columns]
            )
            rows = list(zip(*params))
            cur.executemany(insert_sql, rows)
    return time.monotonic() - t0


def main():
    is_heap = MODE == "heap"
    scenario = "pg_heap" if is_heap else "pg_ducklake"
    log(f"[{scenario}] Reading {TOTAL_ROWS} rows from {HITS_FILE}...")
    t0 = time.monotonic()
    table = coerce_binary_to_string(pq.read_table(HITS_FILE).slice(0, TOTAL_ROWS))
    stage_sec = time.monotonic() - t0
    actual_rows = len(table)
    log(f"  loaded {actual_rows} rows ({table.num_columns} cols) in {stage_sec:.1f}s")

    conn = psycopg.connect(PG_CONNSTR, autocommit=True)

    # Capture key settings for the report
    settings = {}
    for guc in ("duckdb.memory_limit", "duckdb.threads", "duckdb.worker_threads",
                "ducklake.enable_direct_insert"):
        try:
            settings[guc] = conn.execute(
                "SELECT current_setting(%s)", [guc]
            ).fetchone()[0]
        except Exception:
            pass
    if settings:
        log("Settings: " + ", ".join(f"{k}={v}" for k, v in settings.items()))

    conn.execute("DROP TABLE IF EXISTS hits")

    num_batches = (actual_rows + BATCH_SIZE - 1) // BATCH_SIZE
    label = "heap INSERT" if is_heap else "direct insert"
    log(f"Inserting {actual_rows} rows in {num_batches} batches of {BATCH_SIZE} "
        f"({label})...")

    if is_heap:
        insert_sec = insert_heap(conn, table, actual_rows)
    else:
        insert_sec = insert_ducklake(conn, table, actual_rows)
    log(f"  inserted in {insert_sec:.1f}s ({actual_rows / insert_sec:.0f} rows/s)")

    # -- Queries (heap has no flush step) -----------------------------------
    if is_heap:
        log("Running queries...")
        query_times = run_queries(conn)
        log(f"  total: {sum(query_times):.3f}s")

        results = {
            "scenario": scenario,
            "settings": settings,
            "config": {
                "total_rows": actual_rows,
                "batch_size": BATCH_SIZE,
                "num_batches": num_batches,
            },
            "results": {
                "stage_data_sec": round(stage_sec, 3),
                "insert_sec": round(insert_sec, 3),
                "insert_rows_per_sec": round(actual_rows / insert_sec),
                "queries_sec": round(sum(query_times), 3),
                "queries_detail": [round(t, 3) for t in query_times],
            },
        }
    else:
        log("Running queries before flush...")
        pre_flush = run_queries(conn)
        log(f"  total: {sum(pre_flush):.3f}s")

        files_before = conn.execute(
            "SELECT COUNT(*) FROM ducklake.list_files('hits'::regclass)"
        ).fetchone()[0]
        log(f"  files before flush: {files_before}")

        log("Flushing inlined data...")
        t0 = time.monotonic()
        try:
            conn.execute("CALL ducklake.flush_inlined_data()")
        except psycopg.errors.WrongObjectType:
            conn.execute("SELECT * FROM ducklake.flush_inlined_data()").fetchall()
        flush_sec = time.monotonic() - t0

        files_after = conn.execute(
            "SELECT COUNT(*) FROM ducklake.list_files('hits'::regclass)"
        ).fetchone()[0]
        log(f"  flushed in {flush_sec:.3f}s, files after flush: {files_after}")

        log("Running queries after flush...")
        post_flush = run_queries(conn)
        log(f"  total: {sum(post_flush):.3f}s")

        results = {
            "scenario": scenario,
            "settings": settings,
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
                "files_before_flush": files_before,
                "flush_sec": round(flush_sec, 3),
                "files_after_flush": files_after,
                "queries_after_flush_sec": round(sum(post_flush), 3),
                "queries_after_flush_detail": [round(t, 3) for t in post_flush],
            },
        }

    print(json.dumps(results, indent=2))
    conn.execute("DROP TABLE IF EXISTS hits")
    conn.close()


if __name__ == "__main__":
    main()
