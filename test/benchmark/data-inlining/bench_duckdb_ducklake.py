#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.3", "pyarrow"]
# ///
#
# Data inlining benchmark for standalone DuckDB + DuckLake (PG catalog).
#
# Reads a ClickBench HITS partition via pyarrow, streams 10-row batches
# into a ducklake table, then measures aggregation query latency before
# and after flushing inlined data.
#
# Sibling script: bench_pg_ducklake.py (queries kept in sync via hits_schema.py).
#
# Environment variables:
#   BENCH_TOTAL_ROWS         - rows to load (default 300000)
#   BENCH_BATCH_SIZE         - rows per INSERT batch (default 10)
#   BENCH_PG_CATALOG_CONNSTR - libpq-style connstr for the catalog PG
#   BENCH_HITS_FILE          - path to hits parquet (default /tmp/hits_0.parquet)
#   BENCH_DATA_PATH          - local dir for DuckLake Parquet data files
#
# Progress -> stderr; JSON results -> stdout.

import json
import os
import shutil
import sys
import tempfile
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(__file__))
from hits_schema import (
    CREATE_TABLE_DUCKDB_SQL, QUERIES_DUCKDB,
    TIMESTAMP_COL_INDICES, DATE_COL_INDICES, BOOLEAN_COL_INDICES,
)

TOTAL_ROWS = int(os.environ.get("BENCH_TOTAL_ROWS", "300000"))
BATCH_SIZE = int(os.environ.get("BENCH_BATCH_SIZE", "10"))
PG_CATALOG_CONNSTR = os.environ.get(
    "BENCH_PG_CATALOG_CONNSTR",
    "dbname=postgres host=localhost port=5433 user=postgres password=duckdb",
)
HITS_FILE = os.environ.get("BENCH_HITS_FILE", "/tmp/hits_0.parquet")
DATA_PATH = os.environ.get("BENCH_DATA_PATH", "")



def log(msg):
    print(msg, file=sys.stderr, flush=True)


def run_queries(db):
    times = []
    for q in QUERIES_DUCKDB:
        t0 = time.monotonic()
        db.execute(q).fetchall()
        times.append(time.monotonic() - t0)
    return times


def coerce_parquet_types(table):
    """Convert raw parquet columns to proper Arrow types matching the DDL:
    binary -> string, int64 timestamps -> timestamp[s], uint16 dates -> date32,
    int16 booleans -> bool."""
    cols = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
            cols.append(col.cast(pa.string()))
        elif i in TIMESTAMP_COL_INDICES:
            cols.append(col.cast(pa.timestamp("s")))
        elif i in DATE_COL_INDICES:
            # days-since-epoch -> date32
            cols.append(col.cast(pa.int32()).cast(pa.date32()))
        elif i in BOOLEAN_COL_INDICES:
            cols.append(col.cast(pa.bool_()))
        else:
            cols.append(col)
    return pa.table(dict(zip(table.column_names, cols)))


def main():
    own_data_path = not DATA_PATH
    data_path = DATA_PATH or tempfile.mkdtemp(prefix="ducklake_data_")

    log(f"Reading {TOTAL_ROWS} rows from {HITS_FILE}...")
    t0 = time.monotonic()
    table = coerce_parquet_types(pq.read_table(HITS_FILE).slice(0, TOTAL_ROWS))
    stage_sec = time.monotonic() - t0
    actual_rows = len(table)
    log(f"  loaded {actual_rows} rows ({table.num_columns} cols) in {stage_sec:.1f}s")

    db = duckdb.connect()
    try:
        db.install_extension("ducklake")
        db.load_extension("ducklake")
        db.execute(
            f"ATTACH 'ducklake:{PG_CATALOG_CONNSTR}' AS lake "
            f"(DATA_PATH '{data_path}', OVERRIDE_DATA_PATH true)"
        )

        # Capture DuckDB settings
        settings = {}
        for name in ("memory_limit", "threads", "worker_threads"):
            val = db.execute(f"SELECT current_setting('{name}')").fetchone()[0]
            settings[name] = val
        log("Settings: " + ", ".join(f"{k}={v}" for k, v in settings.items()))

        db.execute("DROP TABLE IF EXISTS lake.main.hits")
        db.execute(CREATE_TABLE_DUCKDB_SQL)
        db.execute(
            f"CALL ducklake_set_option('lake', 'data_inlining_row_limit', "
            f"'{actual_rows + 1}')"
        )

        num_batches = (actual_rows + BATCH_SIZE - 1) // BATCH_SIZE
        log(f"Inserting {actual_rows} rows in {num_batches} batches of {BATCH_SIZE}...")
        t0 = time.monotonic()
        for i in range(0, actual_rows, BATCH_SIZE):
            batch = table.slice(i, min(BATCH_SIZE, actual_rows - i))
            db.register("_batch", batch)
            db.execute("INSERT INTO lake.main.hits SELECT * FROM _batch")
        insert_sec = time.monotonic() - t0
        log(f"  inserted in {insert_sec:.1f}s ({actual_rows / insert_sec:.0f} rows/s)")

        log("Running queries before flush...")
        pre_flush = run_queries(db)
        log(f"  total: {sum(pre_flush):.3f}s")

        files_before = db.execute(
            "SELECT COUNT(*) FROM ducklake_list_files('lake', 'hits', schema => 'main')"
        ).fetchone()[0]
        log(f"  files before flush: {files_before}")

        log("Flushing inlined data...")
        t0 = time.monotonic()
        db.execute("CALL ducklake_flush_inlined_data('lake')")
        flush_sec = time.monotonic() - t0

        files_after = db.execute(
            "SELECT COUNT(*) FROM ducklake_list_files('lake', 'hits', schema => 'main')"
        ).fetchone()[0]
        log(f"  flushed in {flush_sec:.3f}s, files after flush: {files_after}")

        log("Running queries after flush...")
        post_flush = run_queries(db)
        log(f"  total: {sum(post_flush):.3f}s")

        results = {
            "scenario": "duckdb_ducklake",
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

        db.execute("DROP TABLE IF EXISTS lake.main.hits")
    finally:
        db.close()
        if own_data_path:
            shutil.rmtree(data_path, ignore_errors=True)


if __name__ == "__main__":
    main()
