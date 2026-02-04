#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.8"
# dependencies = [
#     "psycopg[binary]",
#     "pyarrow",
#     "pandas",
#     "matplotlib",
# ]
# ///
import argparse
import itertools
import time
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import psycopg

script_dir = Path(__file__).parent

print("Loading ClickBench data ...")
CLICKBENCH_DF = pd.read_parquet(script_dir / "hits_0.parquet")

QUERIES = [
    q.strip() for q in (script_dir / "queries.sql").read_text().split("\n") if q.strip()
]

CREATE_TABLE_SQL = (script_dir / "create.sql").read_text()
print(f"create table sql: {CREATE_TABLE_SQL}")

# Type conversion
if True:
    # Convert Unix epoch timestamps to datetime objects
    timestamp_cols = ["EventTime", "ClientEventTime", "LocalEventTime"]
    for col in timestamp_cols:
        if col in CLICKBENCH_DF.columns and CLICKBENCH_DF[col].dtype in [
            "int64",
            "int32",
        ]:
            CLICKBENCH_DF[col] = pd.to_datetime(CLICKBENCH_DF[col], unit="s")

    # Convert date column (stored as days since epoch)
    if "EventDate" in CLICKBENCH_DF.columns and CLICKBENCH_DF["EventDate"].dtype in [
        "uint16",
        "int16",
        "int32",
    ]:
        CLICKBENCH_DF["EventDate"] = pd.to_datetime(
            CLICKBENCH_DF["EventDate"], unit="D", origin="unix"
        )

    # Convert byte columns to strings
    for col in CLICKBENCH_DF.columns:
        if CLICKBENCH_DF[col].dtype == "object":
            # Check if the first non-null value is bytes
            first_val = (
                CLICKBENCH_DF[col].dropna().iloc[0]
                if len(CLICKBENCH_DF[col].dropna()) > 0
                else None
            )
            if isinstance(first_val, bytes):
                CLICKBENCH_DF[col] = CLICKBENCH_DF[col].apply(
                    lambda x: x.decode("utf-8", errors="replace")
                    if isinstance(x, bytes)
                    else x
                )


COL_TYPES = {
    line.split()[0]: line.split()[1]
    for line in CREATE_TABLE_SQL.splitlines()
    if line and not line.startswith(("CREATE", "(", ")")) and len(line.split()) >= 2
}


def _bulk_insert_query():
    columns = CLICKBENCH_DF.columns.tolist()

    def unnest_arg(col):
        # Determine Postgres types
        pg_type = COL_TYPES.get(col, "TEXT")  # Default to TEXT if unknown

        # Array type for parameter casting
        if (
            "INT" in pg_type.upper()
            or "BIGINT" in pg_type.upper()
            or "SMALLINT" in pg_type.upper()
        ):
            array_type = pg_type + "[]"
        elif "TEXT" in pg_type.upper() or "CHAR" in pg_type.upper():
            array_type = "TEXT[]"
        elif "DATE" in pg_type.upper():
            array_type = "DATE[]"
        elif "TIMESTAMP" in pg_type.upper():
            array_type = "TIMESTAMP[]"
        else:
            array_type = "TEXT[]"  # Fallback

        # Construct UNNEST argument: $N::type[]
        # We use standard parameter binding.
        return f"%s::{array_type}"

    column_list = ", ".join(columns)
    unnest_clause = ", ".join(f"unnest({unnest_arg(col)})::{COL_TYPES.get(col, "TEXT")} as {col}" for col in columns)

    # use from unnest(...) which maps to pg_ruleutils logic we fixed
    return f"insert into hits ({column_list}) select {unnest_clause}"


BULK_INSERT_QUERY = _bulk_insert_query()
print(f"Using bulk insert query: {BULK_INSERT_QUERY}")


def execute_bulk_insert(cur, batch_size):
    """Executes a single INSERT statement using UNNEST for better performance"""
    if batch_size == 0:
        return

    df_batch = CLICKBENCH_DF.sample(n=min(batch_size, len(CLICKBENCH_DF)))

    columns = df_batch.columns.tolist()

    # Prepare data column by column for UNNEST
    params = [df_batch[col].tolist() for col in columns]

    cur.execute(BULK_INSERT_QUERY, params)


def measure_read_perf(cur):
    """Measure read performance using a selection of real queries"""
    start = time.perf_counter_ns()

    # Pick a random query from our set of "full scan" candidates
    # Q1: COUNT(*) -> Index 0
    # Q2: Filter scan -> Index 1
    # Q4: Aggregation -> Index 3
    # Use mod logic to be safe if file changes
    target_indices = [0, 1, 3]

    REPEAT = 3
    for query_idx in target_indices:
        query = QUERIES[query_idx]
        # Ensure table name is correct in query (queries.sql uses 'hits')
        # If the user changed the table name in the script, we replace 'hits'
        # But currently table_name is hardcoded to "hits" anyway.

        for _ in range(REPEAT):
            cur.execute(query)
            cur.fetchall()  # Fetch all to ensure execution completes

    latency = time.perf_counter_ns() - start
    return latency


def run_one_benchmark(conn_str, total_rows, batch_size, limit):
    print(f"\n>>> Starting Scenario: ducklake.data_inlining_row_limit = {limit}")

    with psycopg.connect(conn_str, autocommit=True) as conn, conn.cursor() as cur:
        # 1. Isolation: Reinstall Extension
        print("Reinstalling pg_duckdb extension for isolation...")
        cur.execute("DROP EXTENSION IF EXISTS pg_duckdb CASCADE")
        cur.execute("CREATE EXTENSION pg_duckdb")

        # 2. Setup Table (read from create.sql)
        cur.execute(CREATE_TABLE_SQL)
        # 3. Base Load (if requested)

        # 4. Set GUC
        cur.execute(f"SET ducklake.data_inlining_row_limit = {limit}")

        # 5. Stress Test
        n_batches = total_rows // batch_size
        print(f"Running {n_batches} randomized batches...")
        read_perf_before_flush = {}
        read_perf_after_flush = {}
        flush_durations = []

        total_ingest_time = 0
        inlined_row_count = 0
        total_inserted_rows = 0
        flush_count = 0

        for i in range(n_batches):
            # Measure ingestion (excluding measurements)
            t0 = time.perf_counter_ns()
            execute_bulk_insert(cur, batch_size)
            total_ingest_time += time.perf_counter_ns() - t0

            inlined_row_count += batch_size if batch_size < limit else 0
            total_inserted_rows += batch_size

            # Periodic Measurements
            should_flush = limit > 0 and inlined_row_count >= limit

            if should_flush or (limit == 0 and total_inserted_rows % 10000 == 0):
                # Read before flush
                read_perf_before_flush[total_inserted_rows] = measure_read_perf(cur)

            if should_flush:
                # Flush duration
                t_flush = time.perf_counter_ns()
                cur.execute("SELECT ducklake.flush_inlined_data('public', 'hits')")
                flush_durations.append(time.perf_counter_ns() - t_flush)
                flush_count += 1
                inlined_row_count = 0

                # Read after flush
                read_perf_after_flush[total_inserted_rows] = measure_read_perf(cur)

        # Final metrics
        cur.execute("""
            SELECT COUNT(*), SUM(file_size_bytes)
            FROM ducklake.ducklake_data_file
            WHERE table_id = (
                SELECT table_id FROM ducklake.ducklake_table
                WHERE table_name = 'hits' AND end_snapshot IS NULL
            ) AND end_snapshot IS NULL
        """)
        result = cur.fetchone()
        file_count = result[0]
        total_file_size = result[1]

        def avg(lst):
            return sum(lst) / len(lst) if lst else 0.0

        return {
            "ingest_time": total_ingest_time / 1_000_000,
            "read_perf_before_flush": read_perf_before_flush,
            "read_perf_after_flush": read_perf_after_flush,
            "avg_flush": avg(flush_durations) / 1_000_000,
            "flush_count": flush_count,
            "file_count": file_count,
            "file_size": total_file_size,
            "avg_file_size": total_file_size / file_count if file_count else 0.0,
        }


def format_size(size_bytes):
    size_bytes = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def print_report(results):
    # Sort keys for consistent display
    sorted_keys = sorted(results.keys())

    print("\n" + "=" * 100)
    print(
        f"{'Limit':<10} | {'Batch':<8} | {'Ingest(s)':<12} | {'Flush Ct':<10} | {'Avg Flush(ms)':<15} | {'Files':<8} | {'Total Size':<12}"
    )
    print("-" * 100)

    for limit, batch_size in sorted_keys:
        data = results[(limit, batch_size)]

        ingest_time_s = data.get("ingest_time", 0) / 1000.0
        flush_count = data.get("flush_count", 0)
        avg_flush_ms = data.get("avg_flush", 0)
        file_count = data.get("file_count", 0)
        file_size = data.get("file_size", 0)

        print(
            f"{limit:<10} | {batch_size:<8} | {ingest_time_s:<12.2f} | {flush_count:<10} | {avg_flush_ms:<15.2f} | {file_count:<8} | {format_size(file_size):<12}"
        )

    # Read Performance Analysis
    print("\n" + "=" * 100)
    print("Read Performance Analysis (Latency in ms)")
    print("-" * 100)


def run_benchmark(args):
    conn_str = args.connstr
    total_rows = args.total_rows
    limit_matrix = map(int, args.limit.split(","))
    batch_matrix = map(int, args.batch.split(","))

    print(f"""conn_str: {conn_str}
total_rows: {total_rows}
limit_matrix: {limit_matrix}
batch_matrix: {batch_matrix}
""")

    results = {}

    for limit, batch_size in itertools.product(limit_matrix, batch_matrix):
        result = run_one_benchmark(conn_str, total_rows, batch_size, limit)
        results[(limit, batch_size)] = result

    # Print formatted report
    print_report(results)

    output_path = script_dir / "read_perf_matrix_10000_100.png"
    plt.figure(figsize=(12, 7))

    def plot_read_perf(read_perf, label):
        x = []
        y = []
        for total_rows, latency in sorted(read_perf.items(), key=lambda e: e[0]):
            x.append(total_rows)
            y.append(latency / 1_000_000.0)
        # Plotting
        plt.plot(
            x,
            y,
            label=label,
            marker="x",
            linestyle="--",
            linewidth=1.5,
            alpha=0.8,
        )

    for (limit, batch_size), result in results.items():
        read_perf_before_flush = result.get("read_perf_before_flush", {})
        read_perf_after_flush = result.get("read_perf_after_flush", {})
        if read_perf_before_flush and len(read_perf_before_flush) > 0:
            plot_read_perf(
                read_perf_before_flush,
                f"Batch={batch_size}, Limit={limit} Before Flush",
            )
        if read_perf_after_flush and len(read_perf_after_flush) > 0:
            plot_read_perf(
                read_perf_after_flush, f"Batch={batch_size}, Limit={limit} After Flush"
            )

    plt.title("Read Performance Comparison: Small Writes")
    plt.xlabel("Total Rows Inserted")
    plt.ylabel("Query Latency (ms)")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)

    # Since baseline might be much slower, ensure we can see details of the fast path
    # Optional: Log scale if difference is huge
    # plt.yscale('log')

    print(f"Saving plot to {output_path}...")
    plt.savefig(output_path, dpi=100)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DuckLake Small Write Benchmark")
    parser.add_argument(
        "--connstr", default="dbname=postgres", help="Connection string for PostgreSQL"
    )
    parser.add_argument(
        "--total-rows",
        type=int,
        default=1_000_000,
        help="Total number of rows to insert",
    )
    parser.add_argument(
        "--batch", default="100", help="Batch size matris for insertions"
    )
    parser.add_argument(
        "--limit", default="0,10000", help="Limit size matrix for insertions"
    )
    run_benchmark(parser.parse_args())
