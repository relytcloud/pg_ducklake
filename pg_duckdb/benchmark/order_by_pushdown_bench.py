#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]>=3.2"]
# ///
"""Benchmark the ORDER BY / Top-N -> Postgres-scan pushdown optimization.

The optimizer pushes an ORDER BY (and, for Top-N, the LIMIT/OFFSET) into the
Postgres scan only when a btree index can produce the requested ordering. The
pushdown has no user-facing toggle, so this benchmark isolates its effect with
two tables holding identical data: one WITH a btree index on the order key
(pushdown fires) and one WITHOUT (baseline -- DuckDB sorts / Top-Ns itself).
Index presence is exactly the pushdown trigger, so the indexed-vs-not delta is
the optimization's effect.

Each scenario is EXPLAINed on both tables to confirm the indexed plan pushed the
order into the scan ("Order By:" inside PGDUCKDB_POSTGRES_SCAN) and the
non-indexed plan did not, so we are timing the two code paths and not a no-op.

Usage:
  uv run pg_duckdb/benchmark/order_by_pushdown_bench.py --dsn "postgresql://user@host/db"
  # or rely on libpq env (PGHOST/PGPORT/PGUSER/PGDATABASE):
  uv run pg_duckdb/benchmark/order_by_pushdown_bench.py --rows 5000000

Options:
  --dsn DSN        libpq connection string (default: env vars)
  --rows N         per-table row count (default 5_000_000)
  --repeats N      timed repeats per cell (default 7)
  --limit N        row count for the Top-N (LIMIT) scenarios (default 100)
  --keep           do not drop the fixture tables on exit
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time

import psycopg

IDX = "bench_order_idx"
NOIDX = "bench_order_noidx"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default="", help="libpq DSN; default uses PG* env vars")
    p.add_argument("--rows", type=int, default=5_000_000)
    p.add_argument("--repeats", type=int, default=7)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--keep", action="store_true")
    return p.parse_args()


def build_table(conn: psycopg.Connection, name: str, rows: int, indexed: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {name}")
        cur.execute(
            f"""
            CREATE TABLE {name} AS
            SELECT g AS k, repeat('x', 16) AS payload
            FROM generate_series(1, {rows}) g
            """
        )
        if indexed:
            cur.execute(f"CREATE INDEX ON {name} (k)")
        cur.execute(f"ANALYZE {name}")
    conn.commit()


def setup(conn: psycopg.Connection, rows: int) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_duckdb")
    build_table(conn, IDX, rows, indexed=True)
    build_table(conn, NOIDX, rows, indexed=False)


def explain_text(conn: psycopg.Connection, query: str) -> str:
    with conn.cursor() as cur:
        cur.execute("EXPLAIN " + query)
        return "\n".join(row[0] for row in cur.fetchall())


def scan_is_ordered(plan: str) -> bool:
    """True when the postgres scan node itself carries the pushed ORDER BY.

    Both the scan (when pushed) and a DuckDB ORDER_BY / TOP_N node print an
    "Order By:" label, so a plain substring test is ambiguous. The pushed label
    renders *inside* the scan box (after the scan header); an ORDER_BY/TOP_N node
    renders above it.
    """
    scan_pos = plan.find("PGDUCKDB_POSTGRES_SCAN")
    ob_pos = plan.find("Order By:")
    return scan_pos != -1 and ob_pos > scan_pos


def time_query(conn: psycopg.Connection, query: str, repeats: int) -> list[float]:
    samples = []
    with conn.cursor() as cur:
        for _ in range(repeats):
            t0 = time.perf_counter()
            cur.execute(query)
            cur.fetchall()
            samples.append((time.perf_counter() - t0) * 1000.0)
    return samples


def run_scenario(
    conn: psycopg.Connection, name: str, template: str, repeats: int
) -> dict:
    q_idx = template.format(table=IDX)
    q_noidx = template.format(table=NOIDX)

    if not scan_is_ordered(explain_text(conn, q_idx)):
        print(
            f"  WARNING: {name}: indexed plan did not push the order into the scan",
            file=sys.stderr,
        )
    if scan_is_ordered(explain_text(conn, q_noidx)):
        print(
            f"  WARNING: {name}: non-indexed plan unexpectedly pushed the order",
            file=sys.stderr,
        )

    opt = time_query(conn, q_idx, repeats)
    base = time_query(conn, q_noidx, repeats)
    return {
        "name": name,
        "opt_min": min(opt),
        "opt_med": statistics.median(opt),
        "base_min": min(base),
        "base_med": statistics.median(base),
    }


def main() -> int:
    args = parse_args()
    conn = psycopg.connect(args.dsn) if args.dsn else psycopg.connect()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET duckdb.force_execution = true")

    print(
        f"Building fixtures ({IDX}, {NOIDX}) with {args.rows:,} rows each ...",
        flush=True,
    )
    setup(conn, args.rows)

    scenarios = [
        ("ORDER BY k", "SELECT * FROM {table} ORDER BY k"),
        ("ORDER BY k DESC", "SELECT * FROM {table} ORDER BY k DESC"),
        (
            "ORDER BY k LIMIT n (Top-N)",
            f"SELECT * FROM {{table}} ORDER BY k LIMIT {args.limit}",
        ),
        (
            "ORDER BY k LIMIT n OFFSET n",
            f"SELECT * FROM {{table}} ORDER BY k LIMIT {args.limit} OFFSET {args.limit}",
        ),
    ]

    results = []
    for name, template in scenarios:
        print(f"Running: {name} ...", flush=True)
        results.append(run_scenario(conn, name, template, args.repeats))

    print()
    header = f"{'scenario':<30} {'indexed ms':>11} {'no-index ms':>12} {'speedup':>8}"
    print(header)
    print("-" * len(header))
    for r in results:
        speedup = r["base_med"] / r["opt_med"] if r["opt_med"] else float("nan")
        print(
            f"{r['name']:<30} {r['opt_med']:>11.1f} {r['base_med']:>12.1f} {speedup:>7.2f}x"
        )
    print()
    print(
        "(min ms, indexed/no-index) "
        + ", ".join(
            f"{r['name']}: {r['opt_min']:.1f}/{r['base_min']:.1f}" for r in results
        )
    )
    print()
    print("'indexed' = pushdown fires (PG returns the (limited) index-ordered rows);")
    print(
        "'no-index' = identical data without the index, so DuckDB sorts/Top-Ns itself."
    )
    print("Top-N is the large win: PG returns only the top-k rows via an index scan.")

    if not args.keep:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {IDX}")
            cur.execute(f"DROP TABLE IF EXISTS {NOIDX}")
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
