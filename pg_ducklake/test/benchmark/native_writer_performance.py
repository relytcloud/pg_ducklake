#!/usr/bin/env python3
"""Reproducible RFC 001 native-writer performance validation.

The harness uses a Unix-socket-only throwaway PostgreSQL cluster by default and
creates a fresh database for every measured case. It covers VALUES, prepared
UNNEST, COPY FROM STDIN, same- and different-table concurrency, forced snapshot
collisions, reservation head-of-line behavior, and backend-abort recovery.

The historical baseline is insert-then-client-retry: internal publication
retries are disabled, SQLSTATE 40001 aborts the transaction, and the client
replays the complete producer with the same operation-unique rows. Forced
collision cases hold an advisory lock from a snapshot trigger, after payload
prewrite and before snapshot claim, so all losing transactions replay their
producer. Native strategies retry metadata inside one transaction.

Run through the e2e environment after installing the extension:

  PG_CONFIG=$PWD/pg-18/bin/pg_config make install
  PG_CONFIG=$PWD/pg-18/bin/pg_config \
    uv run --project pg_ducklake/test/e2e -- \
    python pg_ducklake/test/benchmark/native_writer_performance.py --profile quick

An existing server can be used with --external-dsn. It must have pg_ducklake
installed and preloaded, expose loaded-module provenance, accept a superuser
connection, and be otherwise idle because writer counters are process-shared.
The harness never changes external server configuration and still creates and
drops only its uniquely named databases.

Profiles bound runtime. JSON is checkpointed after every case. A completed run
also writes a text summary and a .sha256 manifest containing exact hashes of the
JSON and text artifacts. Smoke and quick are shortened harness-validation
profiles, not release-quality performance evidence.
"""

import argparse
import asyncio
import hashlib
import json
import math
import os
import platform
import random
import re
import shlex
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import asyncpg


NO_MATERIAL_REGRESSION = {
    "minimum_steady_throughput_ratio": 0.90,
    "maximum_steady_p95_latency_ratio": 1.25,
    "maximum_collision_wal_ratio": 1.25,
}

PROFILES = {
    "smoke": {
        "repetitions": 1,
        "duration": 0.15,
        "warmup": 0.05,
        "clients": [1, 2],
        "batch_size": 4,
        "collision_sizes": [3],
        "collision_cycles": 1,
        "hol_samples": 1,
        "hol_delay_ms": 40,
    },
    "quick": {
        "repetitions": 1,
        "duration": 0.5,
        "warmup": 0.15,
        "clients": [1, 2, 4],
        "batch_size": 20,
        "collision_sizes": [10, 500],
        "collision_cycles": 1,
        "hol_samples": 1,
        "hol_delay_ms": 100,
    },
    "default": {
        "repetitions": 3,
        "duration": 3.0,
        "warmup": 1.0,
        "clients": [1, 2, 4, 8],
        "batch_size": 100,
        "collision_sizes": [10, 5000],
        "collision_cycles": 3,
        "hol_samples": 3,
        "hol_delay_ms": 300,
    },
}
STRATEGIES = (
    "historical_client_retry",
    "native_retry_queue_off",
    "native_retry_queue_on",
)
QUEUE_WAIT_CAP_MS = 10
SHIPPING_STRATEGY = "native_retry_queue_on"
PRODUCERS = ("values", "prepared_unnest", "copy")
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
WRITER_ID_STRIDE = 1_000_000_000_000


@dataclass(frozen=True)
class Case:
    scenario: str
    producer: str
    topology: str
    clients: int
    strategy: str
    batch_size: int


@dataclass
class OperationResult:
    operation_number: int
    end_to_end_ms: float
    attempt_latencies_ms: list[float]
    retries: int


class LocalCluster:
    mode = "local_unix_socket"

    def __init__(self, pg_config, max_connections, keep=False):
        self.pg_config = pg_config
        self.max_connections = max_connections
        self.keep = keep
        self.root = Path(tempfile.mkdtemp(prefix="rfc001_native_writer_"))
        self.pgdata = self.root / "data"
        self.log = self.root / "postgres.log"
        self.bindir = Path(capture([pg_config, "--bindir"]).strip())
        self.admin_dsn = {"host": str(self.root), "user": "postgres"}
        self.started = False

    def bin(self, name):
        return str(self.bindir / name)

    def start(self):
        run(
            [
                self.bin("initdb"),
                "--no-locale",
                "--encoding=UTF8",
                "--auth=trust",
                "--username=postgres",
                "-D",
                self.pgdata,
            ],
            stdout=subprocess.DEVNULL,
        )
        with (self.pgdata / "postgresql.conf").open("a", encoding="ascii") as conf:
            conf.write(
                "\n".join(
                    [
                        "shared_preload_libraries = 'pg_ducklake'",
                        "listen_addresses = ''",
                        f"unix_socket_directories = '{self.root}'",
                        f"max_connections = {self.max_connections}",
                        "ducklake.maintenance_enabled = off",
                        "ducklake.native_writer_reservation_queue_capacity = 256",
                        "autovacuum = off",
                        "fsync = off",
                        "synchronous_commit = off",
                        "log_min_messages = warning",
                        "logging_collector = off",
                        "",
                    ]
                )
            )
        try:
            run(
                [
                    self.bin("pg_ctl"),
                    "-D",
                    self.pgdata,
                    "-l",
                    self.log,
                    "-w",
                    "start",
                ],
                stdout=subprocess.DEVNULL,
            )
        except Exception:
            if self.log.exists():
                print(self.log.read_text(errors="replace"), file=sys.stderr)
            raise
        self.started = True

    def stop(self):
        if self.started:
            run(
                [self.bin("pg_ctl"), "-D", self.pgdata, "-m", "immediate", "stop"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self.started = False
        if self.keep:
            print(f"benchmark_cluster={self.root}", file=sys.stderr)
        else:
            shutil.rmtree(self.root, ignore_errors=True)

    async def connect(self, database="postgres", **kwargs):
        return await asyncpg.connect(database=database, **self.admin_dsn, **kwargs)


class ExternalCluster:
    mode = "external"

    def __init__(self, dsn):
        if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", dsn):
            self.dsn = dsn
            self.connect_args = None
        else:
            self.dsn = None
            self.connect_args = parse_keyword_dsn(dsn)

    def start(self):
        pass

    def stop(self):
        pass

    async def connect(self, database="postgres", **kwargs):
        if self.dsn is not None:
            return await asyncpg.connect(self.dsn, database=database, **kwargs)
        connect_args = {**self.connect_args, **kwargs, "database": database}
        return await asyncpg.connect(**connect_args)


class Reporter:
    def __init__(self, json_path, text_path, hash_path, report):
        self.json_path = json_path
        self.text_path = text_path
        self.hash_path = hash_path
        self.report = report

    def checkpoint(self):
        self.json_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.json_path.with_suffix(self.json_path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(self.report, indent=2, sort_keys=True) + "\n",
            encoding="ascii",
        )
        temporary.replace(self.json_path)

    def finish(self):
        self.report["status"] = "complete"
        self.report["completed_at"] = utc_now()
        self.report["summary"] = summarize(self.report["runs"])
        self.report["performance_gate"] = evaluate_performance_gate(
            self.report["summary"], self.report["config"]
        )
        self.report["release_evidence_valid"] = release_evidence_eligible(
            self.report["config"]
        )
        self.checkpoint()
        self.text_path.write_text(render_text(self.report), encoding="ascii")
        hashes = [
            (sha256_file(self.json_path), self.json_path.name),
            (sha256_file(self.text_path), self.text_path.name),
        ]
        self.hash_path.write_text(
            "".join(f"{digest}  {name}\n" for digest, name in hashes),
            encoding="ascii",
        )


def run(command, **kwargs):
    kwargs.setdefault("check", True)
    kwargs.setdefault("text", True)
    return subprocess.run([str(part) for part in command], **kwargs)


def capture(command):
    return run(command, stdout=subprocess.PIPE).stdout


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def sha256_file(path):
    return sha256_bytes(Path(path).read_bytes())


def percentile(values, fraction):
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def latency_metrics(values, prefix=""):
    rounded = [round(value, 3) for value in values]
    return {
        f"{prefix}latency_samples_ms": rounded,
        f"{prefix}p50_ms": round(percentile(values, 0.50), 3),
        f"{prefix}p95_ms": round(percentile(values, 0.95), 3),
        f"{prefix}p99_ms": round(percentile(values, 0.99), 3),
        f"{prefix}max_ms": round(max(values, default=0.0), 3),
    }


def strategy_settings(strategy):
    settings = {
        "ducklake.enable_metadata_sync": "off",
        "ducklake.native_writer_retry_wait_ms": "1ms",
        "ducklake.native_writer_retry_backoff": "1",
        "ducklake.native_writer_reservation_queue_wait_ms": f"{QUEUE_WAIT_CAP_MS}ms",
        "statement_timeout": "60s",
        "plan_cache_mode": "force_custom_plan",
        "ducklake.test_native_writer_force_client_retry_before_rebase": "off",
    }
    if strategy == "historical_client_retry":
        settings.update(
            {
                "ducklake.native_writer_reservation_queue": "off",
                "ducklake.native_writer_max_retry_count": "0",
                "ducklake.test_native_writer_force_client_retry_before_rebase": "on",
            }
        )
    else:
        settings.update(
            {
                "ducklake.native_writer_reservation_queue": (
                    "on" if strategy.endswith("queue_on") else "off"
                ),
                "ducklake.native_writer_max_retry_count": "1000",
            }
        )
    return settings


def strategy_descriptions():
    return {
        "historical_client_retry": (
            "Insert payload first, disable internal metadata retry, and replay the "
            "entire producer transaction after SQLSTATE 40001."
        ),
        "native_retry_queue_off": (
            "Insert payload once and retry only metadata publication in the original "
            "transaction; reservation queue disabled."
        ),
        "native_retry_queue_on": (
            "Insert payload once and retry only metadata publication in the original "
            f"transaction; reservation queue enabled with a {QUEUE_WAIT_CAP_MS}ms stalled-predecessor wait cap."
        ),
    }


def build_cases(config):
    if config["profile"] == "smoke":
        cases = [
            Case(
                "steady",
                producer,
                "same",
                1,
                "native_retry_queue_off",
                config["batch_size"],
            )
            for producer in PRODUCERS
        ]
        cases.extend(
            [
                Case(
                    "steady",
                    "values",
                    "same",
                    2,
                    "historical_client_retry",
                    config["batch_size"],
                ),
                Case(
                    "steady",
                    "values",
                    "different",
                    2,
                    "native_retry_queue_on",
                    config["batch_size"],
                ),
            ]
        )
        cases.extend(
            Case("collision", "values", "same", 2, strategy, config["collision_sizes"][0])
            for strategy in ("historical_client_retry", "native_retry_queue_off")
        )
        cases.extend(
            Case("hol", "values", "different", 2, strategy, 1)
            for strategy in ("native_retry_queue_off", "native_retry_queue_on")
        )
        cases.append(
            Case("backend_abort_recovery", "values", "same", 2, "native_retry_queue_on", config["batch_size"])
        )
        return cases

    cases = []
    for producer in PRODUCERS:
        for strategy in STRATEGIES:
            cases.append(Case("steady", producer, "same", 1, strategy, config["batch_size"]))
    for clients in config["clients"]:
        if clients == 1:
            continue
        for topology in ("same", "different"):
            for strategy in STRATEGIES:
                cases.append(Case("steady", "values", topology, clients, strategy, config["batch_size"]))
    collision_clients = max(2, min(4, max(config["clients"])))
    for batch_size in config["collision_sizes"]:
        for strategy in ("historical_client_retry", "native_retry_queue_off"):
            cases.append(Case("collision", "values", "same", collision_clients, strategy, batch_size))
    for topology in ("same", "different"):
        for strategy in ("native_retry_queue_off", "native_retry_queue_on"):
            cases.append(Case("hol", "values", topology, 2, strategy, 1))
    cases.append(
        Case("backend_abort_recovery", "values", "same", 2, "native_retry_queue_on", config["batch_size"])
    )
    return cases


def build_schedule(cases, repetitions, seed):
    schedule = []
    for pair in range((repetitions + 1) // 2):
        order = list(cases)
        random.Random(seed + pair).shuffle(order)
        for offset, arranged in enumerate((order, list(reversed(order)))):
            repetition = pair * 2 + offset + 1
            if repetition > repetitions:
                break
            for order_index, case in enumerate(arranged, 1):
                schedule.append((repetition, order_index, seed + pair, case))
    return schedule


def quote_identifier(name):
    if not IDENTIFIER.fullmatch(name):
        raise ValueError(f"unsafe generated identifier: {name}")
    return name


async def create_database(cluster, name):
    admin = await cluster.connect()
    try:
        await admin.execute(f"DROP DATABASE IF EXISTS {quote_identifier(name)} WITH (FORCE)")
        await admin.execute(f"CREATE DATABASE {quote_identifier(name)}")
    finally:
        await admin.close()
    conn = await cluster.connect(name)
    try:
        await conn.execute("CREATE EXTENSION pg_ducklake CASCADE")
        await conn.execute("CALL ducklake.set_option('data_inlining_row_limit', 10000000)")
    finally:
        await conn.close()


async def drop_database(cluster, name):
    admin = await cluster.connect()
    try:
        await admin.execute(f"DROP DATABASE IF EXISTS {quote_identifier(name)} WITH (FORCE)")
    finally:
        await admin.close()


async def create_tables(conn, names):
    for name in names:
        quote_identifier(name)
        await conn.execute(
            f"CREATE TABLE {name} (id bigint, writer_id int, payload text) USING ducklake"
        )
        await conn.fetchval(
            f"SELECT count(*) FROM ducklake.ensure_inlined_data_table('{name}'::regclass)"
        )


async def inline_table_names(conn, user_tables):
    return [
        await conn.fetchval(
            "SELECT it.table_name FROM ducklake.ducklake_inlined_data_tables it "
            "JOIN ducklake.ducklake_table t USING (table_id) "
            "WHERE t.table_name = $1 AND t.end_snapshot IS NULL "
            "ORDER BY it.schema_version DESC LIMIT 1",
            table,
        )
        for table in user_tables
    ]


class ProducerExecutor:
    def __init__(self, conn, producer, table, batch_size, writer_id, statement=None):
        self.conn = conn
        self.producer = producer
        self.table = table
        self.batch_size = batch_size
        self.writer_id = writer_id
        self.statement = statement
        self.pid = None
        self.application_name = None

    def records(self, operation_number):
        first_id = self.writer_id * WRITER_ID_STRIDE + operation_number * self.batch_size
        return [
            (row_id, self.writer_id, f"{row_id}:".ljust(64, "x"))
            for row_id in range(first_id, first_id + self.batch_size)
        ]

    async def execute(self, operation_number):
        records = self.records(operation_number)
        if self.producer == "values":
            tuples = ",".join(
                f"({row_id},{writer_id},'{payload}')"
                for row_id, writer_id, payload in records
            )
            await self.conn.execute(f"INSERT INTO {self.table} VALUES {tuples}")
        elif self.producer == "prepared_unnest":
            await self.statement.fetch(
                [record[0] for record in records],
                [record[1] for record in records],
                [record[2] for record in records],
            )
        elif self.producer == "copy":
            await self.conn.copy_records_to_table(self.table, records=records)
        else:
            raise ValueError(self.producer)


async def make_executor(conn, producer, table, batch_size, writer_id):
    quote_identifier(table)
    statement = None
    if producer == "values":
        pass
    elif producer == "prepared_unnest":
        statement = await conn.prepare(
            f"INSERT INTO {table} "
            "SELECT UNNEST($1::bigint[]), UNNEST($2::int[]), UNNEST($3::text[])"
        )
    elif producer != "copy":
        raise ValueError(producer)
    executor = ProducerExecutor(conn, producer, table, batch_size, writer_id, statement)
    executor.pid = await conn.fetchval("SELECT pg_backend_pid()")
    executor.application_name = await conn.fetchval("SHOW application_name")
    return executor


async def logical_operation(executor, strategy, operation_number):
    attempt_latencies = []
    retries = 0
    started = time.perf_counter()
    while True:
        attempt_started = time.perf_counter()
        try:
            await executor.execute(operation_number)
            attempt_latencies.append((time.perf_counter() - attempt_started) * 1000.0)
            return OperationResult(
                operation_number,
                (time.perf_counter() - started) * 1000.0,
                attempt_latencies,
                retries,
            )
        except asyncpg.SerializationError:
            attempt_latencies.append((time.perf_counter() - attempt_started) * 1000.0)
            if strategy != "historical_client_retry" or retries >= 1000:
                raise
            retries += 1
            await asyncio.sleep(0)


async def timed_workers(executors, strategy, duration):
    start = asyncio.Event()

    async def worker(executor):
        results = []
        operation_number = 0
        await start.wait()
        deadline = time.perf_counter() + duration
        while time.perf_counter() < deadline:
            results.append(await logical_operation(executor, strategy, operation_number))
            operation_number += 1
        return results

    tasks = [asyncio.create_task(worker(executor)) for executor in executors]
    started = time.perf_counter()
    start.set()
    grouped_results = await asyncio.gather(*tasks)
    return grouped_results, time.perf_counter() - started


async def reset_measurement(conn):
    await conn.execute("SELECT ducklake.reset_native_writer_stats()")
    return await conn.fetchval("SELECT pg_current_wal_insert_lsn()")


async def relation_tuple_estimates(conn, inline_tables):
    live = 0
    dead = 0
    relation_bytes = 0
    for table in inline_tables:
        quote_identifier(table)
        oid = f"ducklake.{table}"
        live += await conn.fetchval("SELECT pg_stat_get_live_tuples($1::regclass)", oid)
        dead += await conn.fetchval("SELECT pg_stat_get_dead_tuples($1::regclass)", oid)
        relation_bytes += await conn.fetchval("SELECT pg_total_relation_size($1::regclass)", oid)
    return live, dead, relation_bytes


async def wal_diff(conn, wal_end, wal_start):
    return await conn.fetchval(
        "SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)::bigint", wal_end, wal_start
    )


async def collect_measurement(conn, wal_start, inline_tables):
    wal_end = await conn.fetchval("SELECT pg_current_wal_insert_lsn()")
    stats = dict(await conn.fetch("SELECT event, count FROM ducklake.native_writer_stats()"))
    for table in inline_tables:
        quote_identifier(table)
        await conn.execute(f"ANALYZE ducklake.{table}")
    live_before, dead_before, bytes_before = await relation_tuple_estimates(conn, inline_tables)

    vacuum_wal_start = await conn.fetchval("SELECT pg_current_wal_insert_lsn()")
    vacuum_started = time.perf_counter()
    for table in inline_tables:
        await conn.execute(f"VACUUM (ANALYZE) ducklake.{quote_identifier(table)}")
    vacuum_elapsed_ms = (time.perf_counter() - vacuum_started) * 1000.0
    vacuum_wal_end = await conn.fetchval("SELECT pg_current_wal_insert_lsn()")
    live_after, dead_after, bytes_after = await relation_tuple_estimates(conn, inline_tables)

    return {
        "retagged_rows": stats.get("rows_retagged", 0),
        "snapshot_claim_conflicts": stats.get("snapshot_claim_conflicts", 0),
        "publication_attempts": stats.get("publication_attempts", 0),
        "payload_rows_inserted": stats.get("payload_rows", 0),
        "copy_rows_consumed": stats.get("copy_rows_consumed", 0),
        "retry_exhaustions": stats.get("retry_exhaustions", 0),
        "wal_bytes": await wal_diff(conn, wal_end, wal_start),
        "estimated_live_tuples_before_vacuum": live_before,
        "estimated_dead_tuples_before_vacuum": dead_before,
        "estimated_dead_tuples_after_vacuum": dead_after,
        "vacuum_reclaimed_dead_tuples": max(0, dead_before - dead_after),
        "inline_relation_bytes_before_vacuum": bytes_before,
        "inline_relation_bytes_after_vacuum": bytes_after,
        "vacuum_reclaimed_relation_bytes": max(0, bytes_before - bytes_after),
        "vacuum_elapsed_ms": round(vacuum_elapsed_ms, 3),
        "vacuum_wal_bytes": await wal_diff(conn, vacuum_wal_end, vacuum_wal_start),
    }


async def open_workers(cluster, database, case, tables, prefix):
    connections = []
    executors = []
    for client in range(case.clients):
        settings = strategy_settings(case.strategy)
        settings["application_name"] = f"rfc001_{prefix}_{client}"
        conn = await cluster.connect(database, server_settings=settings)
        connections.append(conn)
        table = tables[0] if case.topology == "same" else tables[client]
        executors.append(await make_executor(conn, case.producer, table, case.batch_size, client))
    return connections, executors


async def close_all(connections):
    await asyncio.gather(*(conn.close() for conn in connections), return_exceptions=True)


def expected_counts(executors, grouped_results):
    expected = {}
    for executor, results in zip(executors, grouped_results):
        expected.setdefault(executor.table, {})[executor.writer_id] = (
            len(results) * executor.batch_size
        )
    return expected


async def verify_unique_rows(conn, expected):
    total = 0
    for table, writers in expected.items():
        rows = await conn.fetch(
            f"SELECT writer_id, count(*) AS rows, count(DISTINCT id) AS distinct_rows, "
            f"min(id) AS min_id, max(id) AS max_id FROM {quote_identifier(table)} "
            "GROUP BY writer_id ORDER BY writer_id"
        )
        actual_writers = {row["writer_id"] for row in rows}
        if actual_writers != set(writers):
            raise AssertionError(
                f"{table} writers {sorted(actual_writers)} != expected {sorted(writers)}"
            )
        for row in rows:
            writer_id = row["writer_id"]
            count = writers[writer_id]
            first_id = writer_id * WRITER_ID_STRIDE
            expected_max = first_id + count - 1
            observed = (
                row["rows"],
                row["distinct_rows"],
                row["min_id"],
                row["max_id"],
            )
            wanted = (count, count, first_id, expected_max)
            if observed != wanted:
                raise AssertionError(f"{table} writer {writer_id}: {observed} != {wanted}")
            total += count
    return total


def result_metrics(
    case,
    grouped_results,
    elapsed,
    rows,
    measurement,
    extra_attempt_latencies=None,
    submitted_operations=None,
):
    operations = [result for group in grouped_results for result in group]
    operation_latencies = [result.end_to_end_ms for result in operations]
    attempt_latencies = [
        latency for result in operations for latency in result.attempt_latencies_ms
    ]
    if extra_attempt_latencies:
        attempt_latencies.extend(extra_attempt_latencies)
    client_retries = sum(result.retries for result in operations)
    committed_operations = len(operations)
    transaction_attempts = len(attempt_latencies)
    submitted = submitted_operations or committed_operations
    result = {
        **asdict(case),
        "session_gucs": strategy_settings(case.strategy),
        "submitted_operations": submitted,
        "committed_operations": committed_operations,
        "aborted_operations": submitted - committed_operations,
        "transaction_attempts": transaction_attempts,
        "extra_failed_attempt_latencies_ms": [
            round(latency, 3) for latency in (extra_attempt_latencies or [])
        ],
        "visible_rows": rows,
        "unique_rows_verified": True,
        "elapsed_s": round(elapsed, 6),
        "operations_per_s": round(committed_operations / elapsed, 3) if elapsed else 0.0,
        "rows_per_s": round(rows / elapsed, 3) if elapsed else 0.0,
        "client_retries": client_retries,
        **latency_metrics(operation_latencies, "operation_"),
        **latency_metrics(attempt_latencies, "attempt_"),
        **measurement,
    }
    result["transaction_attempts_per_committed_operation"] = (
        round(transaction_attempts / committed_operations, 6)
        if committed_operations
        else 0.0
    )
    result["wal_bytes_per_committed_row"] = (
        round(measurement["wal_bytes"] / rows, 6) if rows else 0.0
    )
    result["dead_tuples_per_committed_row_before_vacuum"] = (
        round(measurement["estimated_dead_tuples_before_vacuum"] / rows, 6)
        if rows
        else 0.0
    )
    result["vacuum_wal_bytes_per_committed_row"] = (
        round(measurement["vacuum_wal_bytes"] / rows, 6) if rows else 0.0
    )
    return result


async def run_steady(cluster, database, case, config):
    admin = await cluster.connect(database)
    measured_tables = (
        ["measured"]
        if case.topology == "same"
        else [f"measured_{client}" for client in range(case.clients)]
    )
    warm_tables = (
        ["warm"]
        if case.topology == "same"
        else [f"warm_{client}" for client in range(case.clients)]
    )
    connections = []
    try:
        await create_tables(admin, warm_tables)
        connections, warm_executors = await open_workers(
            cluster, database, case, warm_tables, "warm"
        )
        try:
            await timed_workers(warm_executors, case.strategy, config["warmup"])
        except asyncpg.FeatureNotSupportedError as error:
            direct_stats = await admin.fetch(
                "SELECT pattern, reason, count FROM ducklake.direct_insert_stats() "
                "WHERE count > 0 ORDER BY pattern, reason"
            )
            raise RuntimeError(
                f"{case.producer} native producer declined during warmup; "
                f"direct_insert_stats={list(map(tuple, direct_stats))}"
            ) from error
        for table in warm_tables:
            await admin.execute(f"DROP TABLE {quote_identifier(table)}")
        await create_tables(admin, measured_tables)
        measured_executors = []
        for client, conn in enumerate(connections):
            table = measured_tables[0] if case.topology == "same" else measured_tables[client]
            measured_executors.append(
                await make_executor(conn, case.producer, table, case.batch_size, client)
            )
        inline_tables = await inline_table_names(admin, measured_tables)
        wal_start = await reset_measurement(admin)
        grouped_results, elapsed = await timed_workers(
            measured_executors, case.strategy, config["duration"]
        )
        measurement = await collect_measurement(admin, wal_start, inline_tables)
        rows = await verify_unique_rows(admin, expected_counts(measured_executors, grouped_results))
        result = result_metrics(case, grouped_results, elapsed, rows, measurement)
        assert_standard_outcome(result)
        return result
    finally:
        await close_all(connections)
        await admin.close()


def advisory_lock_parts(key):
    unsigned = key & ((1 << 64) - 1)
    return unsigned >> 32, unsigned & 0xFFFFFFFF


async def exact_advisory_waiters(conn, pids, blocker_pid, key):
    classid, objid = advisory_lock_parts(key)
    return await conn.fetchval(
        "SELECT count(DISTINCT waiting.pid) "
        "FROM pg_locks waiting "
        "JOIN pg_locks holding ON holding.locktype = waiting.locktype "
        " AND holding.database IS NOT DISTINCT FROM waiting.database "
        " AND holding.classid IS NOT DISTINCT FROM waiting.classid "
        " AND holding.objid IS NOT DISTINCT FROM waiting.objid "
        " AND holding.objsubid IS NOT DISTINCT FROM waiting.objsubid "
        "JOIN pg_stat_activity activity ON activity.pid = waiting.pid "
        "WHERE waiting.locktype = 'advisory' AND NOT waiting.granted "
        " AND holding.granted AND holding.pid = $1 "
        " AND waiting.pid = ANY($2::int[]) "
        " AND waiting.classid = $3::oid AND waiting.objid = $4::oid "
        " AND waiting.objsubid = 1 "
        " AND activity.wait_event_type = 'Lock'",
        blocker_pid,
        pids,
        classid,
        objid,
    )


async def wait_for_exact_advisory_lock(
    conn, executors, blocker_pid, key, tasks, timeout=10
):
    deadline = time.monotonic() + timeout
    pids = [executor.pid for executor in executors]
    while time.monotonic() < deadline:
        for task in tasks:
            if task.done():
                await task
                raise AssertionError("writer completed before the exact advisory barrier")
        count = await exact_advisory_waiters(conn, pids, blocker_pid, key)
        if count == len(pids):
            return
        await asyncio.sleep(0.005)
    raise TimeoutError(f"only {count}/{len(pids)} writers reached the exact advisory barrier")


async def wait_for_extension_state(conn, executor, task, timeout=10):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        if task.done():
            await task
            raise AssertionError(
                f"{executor.application_name} completed before entering extension wait"
            )
        last = await conn.fetchrow(
            "SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE pid = $1",
            executor.pid,
        )
        if last and last["wait_event_type"] == "Extension":
            if last["wait_event"] != "Extension":
                raise AssertionError(f"unexpected extension wait event: {dict(last)}")
            return dict(last)
        await asyncio.sleep(0.005)
    raise TimeoutError(
        f"{executor.application_name} did not enter exact extension wait; last={last}"
    )


async def install_snapshot_barrier(conn, function_name, application_prefix, key, exact=False):
    comparison = "=" if exact else "LIKE"
    application = application_prefix if exact else application_prefix + "%"
    await conn.execute(
        f"""
        CREATE FUNCTION public.{quote_identifier(function_name)}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') {comparison} '{application}' THEN
            PERFORM pg_advisory_xact_lock({key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER {quote_identifier(function_name)}
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.{quote_identifier(function_name)}();
        """
    )


async def run_matched_control(admin, executors, inline_tables, case, cycles):
    wal_start = await reset_measurement(admin)
    grouped_results = [[] for _ in executors]
    started = time.perf_counter()
    for cycle in range(cycles):
        for index, executor in enumerate(executors):
            grouped_results[index].append(
                await logical_operation(executor, case.strategy, cycle)
            )
    elapsed = time.perf_counter() - started
    measurement = await collect_measurement(admin, wal_start, inline_tables)
    rows = await verify_unique_rows(admin, expected_counts(executors, grouped_results))
    return result_metrics(case, grouped_results, elapsed, rows, measurement)


async def run_collision(cluster, database, case, config):
    admin = await cluster.connect(database)
    blocker = await cluster.connect(database)
    connections = []
    key = 810001001
    prefix = "rfc001_collision"
    try:
        await create_tables(admin, ["collision_warm"])
        warm_case = Case(
            "steady", case.producer, "same", 1, case.strategy, min(case.batch_size, 20)
        )
        warm_connections, warm_executors = await open_workers(
            cluster, database, warm_case, ["collision_warm"], "collision_warm"
        )
        await timed_workers(warm_executors, case.strategy, config["warmup"])
        await close_all(warm_connections)
        await admin.execute("DROP TABLE collision_warm")

        control_table = "collision_control"
        measured_table = "collision_measured"
        await create_tables(admin, [control_table, measured_table])
        connections, control_executors = await open_workers(
            cluster, database, case, [control_table], "collision"
        )
        control_inline = await inline_table_names(admin, [control_table])
        control = await run_matched_control(
            admin,
            control_executors,
            control_inline,
            case,
            config["collision_cycles"],
        )

        await install_snapshot_barrier(
            admin, "rfc001_collision_barrier", prefix, key
        )
        measured_executors = [
            await make_executor(conn, case.producer, measured_table, case.batch_size, client)
            for client, conn in enumerate(connections)
        ]
        inline_tables = await inline_table_names(admin, [measured_table])
        wal_start = await reset_measurement(admin)
        blocker_pid = await blocker.fetchval("SELECT pg_backend_pid()")
        grouped_results = [[] for _ in measured_executors]
        started = time.perf_counter()
        for cycle in range(config["collision_cycles"]):
            await blocker.fetchval("SELECT pg_advisory_lock($1)", key)
            tasks = [
                asyncio.create_task(logical_operation(executor, case.strategy, cycle))
                for executor in measured_executors
            ]
            try:
                await wait_for_exact_advisory_lock(
                    admin, measured_executors, blocker_pid, key, tasks
                )
            finally:
                unlocked = await blocker.fetchval("SELECT pg_advisory_unlock($1)", key)
                if not unlocked:
                    raise AssertionError("collision blocker did not own the advisory lock")
            cycle_results = await asyncio.gather(*tasks)
            for index, operation in enumerate(cycle_results):
                grouped_results[index].append(operation)
        elapsed = time.perf_counter() - started
        measurement = await collect_measurement(admin, wal_start, inline_tables)
        rows = await verify_unique_rows(
            admin, expected_counts(measured_executors, grouped_results)
        )
        result = result_metrics(case, grouped_results, elapsed, rows, measurement)
        result["forced_collision_cycles"] = config["collision_cycles"]
        result["forced_barrier"] = {
            "relation": "ducklake.ducklake_snapshot",
            "timing": "BEFORE INSERT",
            "protocol_position": "after payload prewrite and before snapshot claim/rebase",
            "lock": "session-level pg_advisory_lock held by the measured blocker",
            "exact_waiters_observed": config["collision_cycles"] * case.clients,
        }
        result["matched_no_collision_control"] = control
        wal_delta = result["wal_bytes"] - control["wal_bytes"]
        result["estimated_collision_overhead_wal_bytes"] = wal_delta
        result["estimated_retag_wal_bytes"] = (
            wal_delta if result["retagged_rows"] else 0
        )
        result["estimated_retag_wal_bytes_per_retagged_row"] = (
            round(wal_delta / result["retagged_rows"], 6)
            if result["retagged_rows"]
            else 0.0
        )
        result["estimated_collision_dead_tuple_delta"] = (
            result["estimated_dead_tuples_before_vacuum"]
            - control["estimated_dead_tuples_before_vacuum"]
        )
        assert_collision_outcome(result)
        return result
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        await close_all(connections)
        await blocker.close()
        await admin.close()


async def run_hol(cluster, database, case, config):
    admin = await cluster.connect(database)
    blocker = await cluster.connect(database)
    key = 810001002
    settings = strategy_settings(case.strategy)
    head = await cluster.connect(
        database,
        server_settings={**settings, "application_name": "rfc001_hol_head"},
    )
    follower = await cluster.connect(
        database,
        server_settings={**settings, "application_name": "rfc001_hol_follower"},
    )
    grouped_results = [[], []]
    expected = {}
    tables = []
    extension_states = []
    try:
        for sample in range(config["hol_samples"]):
            head_table = f"hol_head_{sample}"
            follower_table = head_table if case.topology == "same" else f"hol_other_{sample}"
            sample_tables = sorted(set((head_table, follower_table)))
            tables.extend(sample_tables)
            await create_tables(admin, sample_tables)
        await install_snapshot_barrier(
            admin, "rfc001_hol_barrier", "rfc001_hol_head", key, exact=True
        )
        inline_tables = await inline_table_names(admin, tables)
        wal_start = await reset_measurement(admin)
        blocker_pid = await blocker.fetchval("SELECT pg_backend_pid()")
        started = time.perf_counter()
        for sample in range(config["hol_samples"]):
            head_table = f"hol_head_{sample}"
            follower_table = head_table if case.topology == "same" else f"hol_other_{sample}"
            head_executor = await make_executor(head, "values", head_table, 1, 0)
            follower_executor = await make_executor(follower, "values", follower_table, 1, 1)
            await blocker.fetchval("SELECT pg_advisory_lock($1)", key)
            head_task = asyncio.create_task(
                logical_operation(head_executor, case.strategy, 0)
            )
            await wait_for_exact_advisory_lock(
                admin, [head_executor], blocker_pid, key, [head_task]
            )
            follower_task = asyncio.create_task(
                logical_operation(follower_executor, case.strategy, 0)
            )
            sample_started = time.perf_counter()
            if case.strategy == "native_retry_queue_on":
                extension_states.append(
                    await wait_for_extension_state(admin, follower_executor, follower_task)
                )
                remaining = config["hol_delay_ms"] / 1000.0 - (
                    time.perf_counter() - sample_started
                )
                grouped_results[1].append(
                    await asyncio.wait_for(
                        asyncio.shield(follower_task),
                        timeout=max(2.0, config["hol_delay_ms"] / 1000.0),
                    )
                )
                remaining = config["hol_delay_ms"] / 1000.0 - (
                    time.perf_counter() - sample_started
                )
                if remaining > 0:
                    await asyncio.sleep(remaining)
                if head_task.done():
                    await head_task
                    raise AssertionError("HOL head completed before its forced release")
            else:
                timeout = max(2.0, config["hol_delay_ms"] / 1000.0)
                follower_result = await asyncio.wait_for(
                    asyncio.shield(follower_task), timeout=timeout
                )
                grouped_results[1].append(follower_result)
                remaining = config["hol_delay_ms"] / 1000.0 - (
                    time.perf_counter() - sample_started
                )
                if remaining > 0:
                    await asyncio.sleep(remaining)
                state = await admin.fetchrow(
                    "SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE pid = $1",
                    follower_executor.pid,
                )
                if state and state["wait_event_type"] == "Extension":
                    raise AssertionError("queue-off HOL follower entered extension wait")
            unlocked = await blocker.fetchval("SELECT pg_advisory_unlock($1)", key)
            if not unlocked:
                raise AssertionError("HOL blocker did not own the advisory lock")
            grouped_results[0].append(await head_task)
            expected.setdefault(head_table, {})[0] = 1
            expected.setdefault(follower_table, {})[1] = 1
        elapsed = time.perf_counter() - started
        measurement = await collect_measurement(admin, wal_start, inline_tables)
        rows = await verify_unique_rows(admin, expected)
        result = result_metrics(case, grouped_results, elapsed, rows, measurement)
        result["hol_delay_ms"] = config["hol_delay_ms"]
        result["hol_extension_wait_states"] = extension_states
        result["head_operations"] = len(grouped_results[0])
        result["follower_operations"] = len(grouped_results[1])
        result.update(
            latency_metrics(
                [operation.end_to_end_ms for operation in grouped_results[0]],
                "head_operation_",
            )
        )
        result.update(
            latency_metrics(
                [operation.end_to_end_ms for operation in grouped_results[1]],
                "follower_operation_",
            )
        )
        assert_hol_outcome(result, config)
        return result
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        await asyncio.gather(head.close(), follower.close(), return_exceptions=True)
        await blocker.close()
        await admin.close()


async def run_backend_abort_recovery(cluster, database, case, config):
    admin = await cluster.connect(database)
    blocker = await cluster.connect(database)
    key = 810001003
    settings = strategy_settings(case.strategy)
    settings.update(
        {
            "ducklake.native_writer_reservation_queue_wait_ms": "1000ms",
            "ducklake.native_writer_retry_wait_ms": "100ms",
            "ducklake.native_writer_retry_backoff": "1",
        }
    )
    head = await cluster.connect(
        database,
        server_settings={**settings, "application_name": "rfc001_recovery_head"},
    )
    successor = await cluster.connect(
        database,
        server_settings={**settings, "application_name": "rfc001_recovery_successor"},
    )
    table = "backend_abort_recovery"
    head_task = None
    successor_task = None
    try:
        await create_tables(admin, [table])
        await install_snapshot_barrier(
            admin, "rfc001_recovery_barrier", "rfc001_recovery_head", key, exact=True
        )
        head_executor = await make_executor(head, case.producer, table, case.batch_size, 0)
        successor_executor = await make_executor(
            successor, case.producer, table, case.batch_size, 1
        )
        inline_tables = await inline_table_names(admin, [table])
        wal_start = await reset_measurement(admin)
        blocker_pid = await blocker.fetchval("SELECT pg_backend_pid()")
        await blocker.fetchval("SELECT pg_advisory_lock($1)", key)
        started = time.perf_counter()
        head_started = time.perf_counter()
        head_task = asyncio.create_task(head_executor.execute(0))
        await wait_for_exact_advisory_lock(
            admin, [head_executor], blocker_pid, key, [head_task]
        )
        successor_task = asyncio.create_task(
            logical_operation(successor_executor, case.strategy, 0)
        )
        extension_state = await wait_for_extension_state(
            admin, successor_executor, successor_task
        )
        terminated = await admin.fetchval("SELECT pg_terminate_backend($1)", head_executor.pid)
        if not terminated:
            raise AssertionError("failed to terminate reservation owner backend")
        head_result = await asyncio.gather(head_task, return_exceptions=True)
        head_attempt_ms = (time.perf_counter() - head_started) * 1000.0
        if not isinstance(head_result[0], asyncpg.ConnectionDoesNotExistError):
            raise AssertionError(f"unexpected terminated head result: {head_result[0]!r}")
        successor_result = await asyncio.wait_for(successor_task, timeout=10)
        elapsed = time.perf_counter() - started
        measurement = await collect_measurement(admin, wal_start, inline_tables)
        rows = await verify_unique_rows(
            admin, {table: {successor_executor.writer_id: case.batch_size}}
        )
        result = result_metrics(
            case,
            [[], [successor_result]],
            elapsed,
            rows,
            measurement,
            extra_attempt_latencies=[head_attempt_ms],
            submitted_operations=2,
        )
        result["owner_backend_terminated"] = True
        result["successor_extension_wait_state"] = extension_state
        result["aborted_head_attempt_ms"] = round(head_attempt_ms, 3)
        assert_recovery_outcome(result)
        return result
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        for task in (head_task, successor_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        if not head.is_closed():
            await head.close()
        await successor.close()
        await blocker.close()
        await admin.close()


def assert_standard_outcome(result):
    if not result["unique_rows_verified"] or result["visible_rows"] <= 0:
        raise AssertionError("steady case did not verify committed unique rows")
    expected_payload = result["visible_rows"]
    if result["strategy"] == "historical_client_retry":
        expected_payload += result["client_retries"] * result["batch_size"]
    if result["payload_rows_inserted"] != expected_payload:
        raise AssertionError(
            f"payload rows {result['payload_rows_inserted']} != expected {expected_payload}"
        )
    if result["transaction_attempts"] != (
        result["committed_operations"] + result["client_retries"]
    ):
        raise AssertionError("transaction attempt accounting mismatch")
    if result["strategy"] != "historical_client_retry" and result["client_retries"]:
        raise AssertionError("native metadata retry unexpectedly reached the client")
    if result["strategy"] == "historical_client_retry":
        if result["retry_exhaustions"] != result["client_retries"]:
            raise AssertionError("historical retry exhaustion accounting mismatch")
        if result["retagged_rows"]:
            raise AssertionError("historical client-retry baseline performed a rebase")
    elif result["retry_exhaustions"]:
        raise AssertionError("native strategy exhausted an internal retry")
    if result["producer"] == "copy":
        if result["copy_rows_consumed"] != result["payload_rows_inserted"]:
            raise AssertionError("COPY producer consumption accounting mismatch")
    elif result["copy_rows_consumed"]:
        raise AssertionError("non-COPY producer reported consumed COPY rows")
    if result["estimated_dead_tuples_after_vacuum"] > result["estimated_dead_tuples_before_vacuum"]:
        raise AssertionError("VACUUM increased the estimated dead tuple count")
    if result["vacuum_reclaimed_dead_tuples"] != (
        result["estimated_dead_tuples_before_vacuum"]
        - result["estimated_dead_tuples_after_vacuum"]
    ):
        raise AssertionError("VACUUM reclamation accounting mismatch")


def assert_collision_outcome(result):
    assert_standard_outcome(result)
    if result["snapshot_claim_conflicts"] <= 0:
        raise AssertionError("forced collision produced no snapshot claim conflict")
    if result["strategy"] == "historical_client_retry":
        if result["client_retries"] <= 0:
            raise AssertionError("historical baseline did not replay a producer")
        expected_payload = result["visible_rows"] + result["client_retries"] * result["batch_size"]
        if result["payload_rows_inserted"] != expected_payload:
            raise AssertionError("historical baseline did not reinsert every losing batch")
    else:
        if result["client_retries"] != 0:
            raise AssertionError("native collision unexpectedly retried at the client")
        if result["retagged_rows"] <= 0:
            raise AssertionError("native collision produced no retagged rows")
        if result["payload_rows_inserted"] != result["visible_rows"]:
            raise AssertionError("native collision reinserted payload")
    control = result["matched_no_collision_control"]
    if control["visible_rows"] != result["visible_rows"]:
        raise AssertionError("matched WAL control committed a different row count")
    if control["transaction_attempts"] != control["committed_operations"]:
        raise AssertionError("matched no-collision control was not collision-free")


def assert_hol_outcome(result, config):
    samples = config["hol_samples"]
    if result["head_operations"] != samples or result["follower_operations"] != samples:
        raise AssertionError("HOL operation accounting omitted head or follower")
    if result["committed_operations"] != 2 * samples or result["visible_rows"] != 2 * samples:
        raise AssertionError("HOL transaction or row accounting mismatch")
    if result["strategy"] == "native_retry_queue_on":
        if len(result["hol_extension_wait_states"]) != samples:
            raise AssertionError("queued HOL follower missed extension wait")
        if result["follower_operation_p50_ms"] < QUEUE_WAIT_CAP_MS * 0.5:
            raise AssertionError("queued HOL follower did not consume its bounded wait")
        if result["follower_operation_p50_ms"] >= config["hol_delay_ms"] * 0.75:
            raise AssertionError("queued HOL follower did not fall back before head release")
    elif result["hol_extension_wait_states"]:
        raise AssertionError("queue-off HOL case recorded extension wait")


def assert_recovery_outcome(result):
    if not result["owner_backend_terminated"]:
        raise AssertionError("reservation owner backend was not terminated")
    if result["submitted_operations"] != 2 or result["committed_operations"] != 1:
        raise AssertionError("backend-abort operation accounting mismatch")
    if result["aborted_operations"] != 1 or result["transaction_attempts"] != 2:
        raise AssertionError("backend-abort attempt accounting mismatch")
    if result["visible_rows"] != result["batch_size"] or not result["unique_rows_verified"]:
        raise AssertionError("backend-abort recovery lost or duplicated successor rows")
    if result["payload_rows_inserted"] != 2 * result["batch_size"]:
        raise AssertionError("backend-abort recovery did not observe both prewrites")
    if result["successor_extension_wait_state"]["wait_event_type"] != "Extension":
        raise AssertionError("successor did not wait on the reservation queue")


async def execute_case(cluster, database_prefix, case, config, run_number):
    database = f"{database_prefix}_{run_number}"
    await create_database(cluster, database)
    try:
        if case.scenario == "steady":
            return await run_steady(cluster, database, case, config)
        if case.scenario == "collision":
            return await run_collision(cluster, database, case, config)
        if case.scenario == "hol":
            return await run_hol(cluster, database, case, config)
        if case.scenario == "backend_abort_recovery":
            return await run_backend_abort_recovery(cluster, database, case, config)
        raise ValueError(case.scenario)
    finally:
        await drop_database(cluster, database)


def summarize(runs):
    groups = {}
    for row in runs:
        key = tuple(
            row[field]
            for field in (
                "scenario",
                "producer",
                "topology",
                "clients",
                "strategy",
                "batch_size",
            )
        )
        groups.setdefault(key, []).append(row)
    summary = []
    for key, selected in sorted(groups.items()):
        throughputs = [row["rows_per_s"] for row in selected]
        mean = statistics.mean(throughputs)
        stdev = statistics.stdev(throughputs) if len(throughputs) > 1 else 0.0
        visible_rows = sum(row["visible_rows"] for row in selected)
        wal_bytes = sum(row["wal_bytes"] for row in selected)
        dead_tuples = sum(
            row["estimated_dead_tuples_before_vacuum"] for row in selected
        )
        attempts = sum(row["transaction_attempts"] for row in selected)
        operations = sum(row["committed_operations"] for row in selected)
        summary.append(
            {
                "scenario": key[0],
                "producer": key[1],
                "topology": key[2],
                "clients": key[3],
                "strategy": key[4],
                "batch_size": key[5],
                "runs": len(selected),
                "rows_per_s_mean": round(mean, 3),
                "rows_per_s_stdev": round(stdev, 3),
                "rows_per_s_cv_percent": round(100 * stdev / mean, 3) if mean else 0.0,
                "rows_per_s_min": round(min(throughputs), 3),
                "rows_per_s_max": round(max(throughputs), 3),
                "operation_p50_ms_mean": round(
                    statistics.mean(row["operation_p50_ms"] for row in selected), 3
                ),
                "operation_p95_ms_mean": round(
                    statistics.mean(row["operation_p95_ms"] for row in selected), 3
                ),
                "attempt_p50_ms_mean": round(
                    statistics.mean(row["attempt_p50_ms"] for row in selected), 3
                ),
                "attempt_p95_ms_mean": round(
                    statistics.mean(row["attempt_p95_ms"] for row in selected), 3
                ),
                "attempts": attempts,
                "attempts_per_committed_operation": round(attempts / operations, 6)
                if operations
                else 0.0,
                "retagged_rows": sum(row["retagged_rows"] for row in selected),
                "snapshot_claim_conflicts": sum(
                    row["snapshot_claim_conflicts"] for row in selected
                ),
                "client_retries": sum(row["client_retries"] for row in selected),
                "payload_rows_inserted": sum(
                    row["payload_rows_inserted"] for row in selected
                ),
                "payload_insertion_ratio": round(
                    sum(row["payload_rows_inserted"] for row in selected) / visible_rows,
                    6,
                )
                if visible_rows
                else 0.0,
                "wal_bytes_per_committed_row": round(wal_bytes / visible_rows, 6)
                if visible_rows
                else 0.0,
                "dead_tuples_per_committed_row_before_vacuum": round(
                    dead_tuples / visible_rows, 6
                )
                if visible_rows
                else 0.0,
                "vacuum_reclaimed_dead_tuples": sum(
                    row["vacuum_reclaimed_dead_tuples"] for row in selected
                ),
                "vacuum_wal_bytes_per_committed_row": round(
                    sum(row["vacuum_wal_bytes"] for row in selected) / visible_rows,
                    6,
                )
                if visible_rows
                else 0.0,
                "estimated_retag_wal_bytes": sum(
                    row.get("estimated_retag_wal_bytes", 0) for row in selected
                ),
            }
        )
    return summary


def evaluate_performance_gate(summary, config):
    threshold = dict(NO_MATERIAL_REGRESSION)
    if not release_evidence_eligible(config):
        return {
            "status": "NOT_EVALUATED",
            "threshold": threshold,
            "reason": "only the unchanged default profile is release evidence",
            "checks": [],
            "variability": {},
        }

    checks = []
    by_key = {
        (
            row["scenario"],
            row["producer"],
            row["topology"],
            row["clients"],
            row["batch_size"],
            row["strategy"],
        ): row
        for row in summary
    }

    def add_check(name, actual, limit, comparison, context):
        passed = actual >= limit if comparison == ">=" else actual <= limit
        checks.append(
            {
                "name": name,
                "context": context,
                "actual": round(actual, 6),
                "comparison": comparison,
                "limit": limit,
                "passed": passed,
            }
        )

    candidate_rows = [
        row
        for row in summary
        if row["scenario"] == "steady"
        and row["strategy"] == SHIPPING_STRATEGY
    ]
    for candidate in candidate_rows:
        context = (
            f"{candidate['producer']}/{candidate['topology']}/"
            f"c{candidate['clients']}/b{candidate['batch_size']}"
        )
        baseline = by_key.get(
            (
                "steady",
                candidate["producer"],
                candidate["topology"],
                candidate["clients"],
                candidate["batch_size"],
                "historical_client_retry",
            )
        )
        if baseline is None:
            checks.append(
                {
                    "name": "matched historical baseline",
                    "context": context,
                    "passed": False,
                    "reason": "missing",
                }
            )
            continue
        add_check(
            "steady mean throughput ratio",
            candidate["rows_per_s_mean"] / baseline["rows_per_s_mean"],
            threshold["minimum_steady_throughput_ratio"],
            ">=",
            context,
        )
        add_check(
            "steady mean operation p95 latency ratio",
            candidate["operation_p95_ms_mean"] / baseline["operation_p95_ms_mean"],
            threshold["maximum_steady_p95_latency_ratio"],
            "<=",
            context,
        )

    collision_rows = [
        row
        for row in summary
        if row["scenario"] == "collision"
        and row["strategy"] == "native_retry_queue_off"
    ]
    for candidate in collision_rows:
        context = f"forced-collision/c{candidate['clients']}/b{candidate['batch_size']}"
        baseline = by_key.get(
            (
                "collision",
                candidate["producer"],
                candidate["topology"],
                candidate["clients"],
                candidate["batch_size"],
                "historical_client_retry",
            )
        )
        if baseline is None:
            checks.append(
                {
                    "name": "matched collision baseline",
                    "context": context,
                    "passed": False,
                    "reason": "missing",
                }
            )
            continue
        add_check(
            "collision payload insertion ratio",
            candidate["payload_insertion_ratio"],
            1.0,
            "<=",
            context,
        )
        add_check(
            "collision WAL ratio",
            candidate["wal_bytes_per_committed_row"]
            / baseline["wal_bytes_per_committed_row"],
            threshold["maximum_collision_wal_ratio"],
            "<=",
            context,
        )

    required_scenarios = {"steady", "collision", "hol", "backend_abort_recovery"}
    observed_scenarios = {row["scenario"] for row in summary}
    required_clients = {1, 2, 4, 8}
    observed_clients = {row["clients"] for row in summary if row["scenario"] == "steady"}
    checks.extend(
        [
            {
                "name": "all RFC performance scenarios",
                "actual": sorted(observed_scenarios),
                "required": sorted(required_scenarios),
                "passed": required_scenarios <= observed_scenarios,
            },
            {
                "name": "required steady client levels",
                "actual": sorted(observed_clients),
                "required": sorted(required_clients),
                "passed": required_clients <= observed_clients,
            },
            {
                "name": "all native producers",
                "actual": sorted({row["producer"] for row in candidate_rows}),
                "required": sorted(PRODUCERS),
                "passed": set(PRODUCERS)
                <= {row["producer"] for row in candidate_rows},
            },
        ]
    )
    cvs = [row["rows_per_s_cv_percent"] for row in summary if row["runs"] > 1]
    high_variability = [
        {
            "scenario": row["scenario"],
            "producer": row["producer"],
            "topology": row["topology"],
            "clients": row["clients"],
            "strategy": row["strategy"],
            "batch_size": row["batch_size"],
            "rows_per_s_cv_percent": row["rows_per_s_cv_percent"],
        }
        for row in summary
        if row["runs"] > 1 and row["rows_per_s_cv_percent"] > 15.0
    ]
    return {
        "status": "PASS" if all(check["passed"] for check in checks) else "FAIL",
        "definition": (
            f"For the shipping {SHIPPING_STRATEGY} strategy, every matched steady case must retain "
            "at least 90% of historical mean throughput and mean operation p95 must be "
            "at most 125% of historical. Forced-collision native WAL per committed row "
            "must be at most 125% of replay baseline and payload insertion ratio at most 1.0."
        ),
        "threshold": threshold,
        "checks": checks,
        "variability": {
            "rows_per_s_cv_percent_median": round(statistics.median(cvs), 3)
            if cvs
            else 0.0,
            "rows_per_s_cv_percent_max": round(max(cvs), 3) if cvs else 0.0,
            "over_15_percent": high_variability,
            "caveat": (
                "CV is reported, not used to waive a failed threshold. Three bounded "
                "repetitions characterize practical variation but do not establish a "
                "high-confidence population estimate."
            ),
        },
    }


def render_text(report):
    config = report["config"]
    environment = report["environment"]
    lines = [
        "RFC 001 native writer performance validation",
        "============================================",
        f"status: {report['status']}",
        f"evidence_label: {config['evidence_label']}",
        f"started_at: {report['started_at']}",
        f"completed_at: {report.get('completed_at', 'n/a')}",
        f"git_commit: {environment['git_commit']}",
        f"source_manifest_sha256: {environment['source_manifest_sha256']}",
        f"harness_sha256: {environment['harness_sha256']}",
        f"extension_binary_sha256: {environment['extension_binary'].get('sha256', 'unavailable')}",
        f"postgres: {environment.get('postgres_version', 'unknown')}",
        f"extension_version: {environment.get('extension_version', 'unknown')}",
        f"cluster_mode: {environment['cluster_mode']}",
        f"machine: {environment['machine']}",
        f"profile: {config['profile']}",
        f"seed: {config['seed']}",
        f"repetitions: {config['repetitions']}",
        f"duration_s: {config['duration']}",
        f"warmup_s: {config['warmup']}",
        f"client_levels: {','.join(map(str, config['clients']))}",
        f"batch_size: {config['batch_size']}",
        f"collision_sizes: {','.join(map(str, config['collision_sizes']))}",
        f"collision_cycles: {config['collision_cycles']}",
        f"hol_samples: {config['hol_samples']}",
        f"hol_delay_ms: {config['hol_delay_ms']}",
        f"queue_wait_cap_ms: {config['queue_wait_cap_ms']}",
        f"reproduce: {environment['reproducible_command']}",
        "exact_result_hashes: see adjacent .sha256 manifest",
        f"release_evidence_valid: {str(report['release_evidence_valid']).lower()}",
        f"performance_gate: {report['performance_gate']['status']}",
        f"no_material_regression_definition: {report['performance_gate'].get('definition', report['performance_gate'].get('reason'))}",
        "",
        "Measured results:",
        "scenario producer topology clients strategy batch runs rows/s CV% op-p50 op-p95 attempt-p50 attempt-p95 attempts/op payloadx retags conflicts retries WAL/row dead/row vacuum/row retag-WAL-est",
    ]
    for row in report["summary"]:
        lines.append(
            f"{row['scenario']} {row['producer']} {row['topology']} "
            f"{row['clients']} {row['strategy']} {row['batch_size']} "
            f"{row['runs']} {row['rows_per_s_mean']:.3f} "
            f"{row['rows_per_s_cv_percent']:.3f} "
            f"{row['operation_p50_ms_mean']:.3f} {row['operation_p95_ms_mean']:.3f} "
            f"{row['attempt_p50_ms_mean']:.3f} {row['attempt_p95_ms_mean']:.3f} "
            f"{row['attempts_per_committed_operation']:.3f} "
            f"{row['payload_insertion_ratio']:.3f} {row['retagged_rows']} "
            f"{row['snapshot_claim_conflicts']} {row['client_retries']} "
            f"{row['wal_bytes_per_committed_row']:.3f} "
            f"{row['dead_tuples_per_committed_row_before_vacuum']:.6f} "
            f"{row['vacuum_wal_bytes_per_committed_row']:.3f} "
            f"{row['estimated_retag_wal_bytes']}"
        )
    gate = report["performance_gate"]
    failed_checks = [check for check in gate["checks"] if not check["passed"]]
    lines.extend(
        [
            "",
            f"Gate checks: {len(gate['checks']) - len(failed_checks)} passed, {len(failed_checks)} failed.",
            *[
                "FAILED: " + json.dumps(check, sort_keys=True, separators=(",", ":"))
                for check in failed_checks
            ],
            "Variability: " + json.dumps(gate["variability"], sort_keys=True, separators=(",", ":")),
            "",
            "Operation latency includes all client retries; attempt latency records every producer transaction attempt.",
            "WAL/row and dead/row are normalized by committed visible rows; vacuum/row is VACUUM WAL per committed row.",
            "Retag WAL is an estimate for native collision rows: collision WAL minus a matched, serial, no-collision control.",
            "Dead tuples are PostgreSQL estimates after ANALYZE; reclamation is measured again after VACUUM (ANALYZE).",
            "HOL metrics include both head and follower transactions and rows; role-specific samples are in JSON.",
            "Every committed operation uses unique IDs; contiguous per-writer ranges are checked for duplicates and losses.",
            "Each measured case used a fresh database and tables after an unmeasured warmup.",
            "The seeded schedule is randomized, with paired repetitions run in reverse order.",
            "Worker sessions use plan_cache_mode=force_custom_plan for sustained prepared UNNEST.",
            "Smoke, quick, or shortened overrides validate the harness and are not release-quality evidence.",
            "Per-strategy GUCs, matched controls, raw samples, vacuum metrics, and provenance are recorded in JSON.",
            "",
        ]
    )
    return "\n".join(lines)


def machine_name():
    if platform.system() == "Darwin":
        try:
            return capture(["sysctl", "-n", "machdep.cpu.brand_string"]).strip()
        except subprocess.SubprocessError:
            pass
    return platform.processor() or platform.machine()


def repository_root():
    return Path(__file__).resolve().parents[3]


def git_value(*args):
    try:
        return capture(["git", "-C", repository_root(), *args]).strip()
    except subprocess.SubprocessError:
        return "unknown"


def source_manifest_hash():
    root = repository_root()
    tracked = capture(
        [
            "git",
            "-C",
            root,
            "ls-files",
            "-z",
            "--",
            "Makefile",
            "libpgduckdb",
            "pg_ducklake",
        ]
    ).split("\0")
    paths = set()
    generated_results = "pg_ducklake/test/benchmark/results/"
    for relative in tracked:
        if not relative or relative.startswith(generated_results):
            continue
        path = root / relative
        if path.is_file():
            paths.add(relative)
    harness_relative = str(Path(__file__).resolve().relative_to(root))
    paths.add(harness_relative)
    manifest = b""
    patch_hashes = {}
    for relative in sorted(paths):
        digest = sha256_file(root / relative)
        manifest += f"{digest}  {relative}\n".encode("ascii")
        if relative.startswith("pg_ducklake/third_party/ducklake-") and relative.endswith(".patch"):
            patch_hashes[relative] = digest
    tracked_patches = {
        relative
        for relative in tracked
        if relative.startswith("pg_ducklake/third_party/ducklake-")
        and relative.endswith(".patch")
    }
    if set(patch_hashes) != tracked_patches:
        raise AssertionError("source manifest omitted a tracked DuckLake patch")
    return sha256_bytes(manifest), len(paths), patch_hashes


def extension_binary_provenance(pg_config, explicit_path=None):
    if explicit_path:
        path = Path(explicit_path).expanduser().resolve()
        basis = "--extension-binary"
    else:
        try:
            directory = Path(capture([pg_config, "--pkglibdir"]).strip())
            candidates = [directory / "pg_ducklake.so", directory / "pg_ducklake.dylib"]
            path = next((candidate for candidate in candidates if candidate.is_file()), candidates[0])
            path = path.resolve()
            basis = "PG_CONFIG --pkglibdir"
        except subprocess.SubprocessError:
            return {"basis": "unavailable", "path": None, "exists": False}
    result = {"basis": basis, "path": str(path), "exists": path.is_file()}
    if path.is_file():
        stat = path.stat()
        result.update(
            {
                "sha256": sha256_file(path),
                "size_bytes": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        )
    return result


def parse_keyword_dsn(dsn):
    parsed = {}
    position = 0
    while position < len(dsn):
        while position < len(dsn) and dsn[position].isspace():
            position += 1
        if position == len(dsn):
            break
        key_match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", dsn[position:])
        if key_match is None:
            raise ValueError("invalid key-value PostgreSQL DSN")
        key = key_match.group(0).lower()
        position += len(key_match.group(0))
        while position < len(dsn) and dsn[position].isspace():
            position += 1
        if position == len(dsn) or dsn[position] != "=":
            raise ValueError("invalid key-value PostgreSQL DSN")
        position += 1
        while position < len(dsn) and dsn[position].isspace():
            position += 1
        value = []
        quoted = position < len(dsn) and dsn[position] == "'"
        if quoted:
            position += 1
        while position < len(dsn):
            character = dsn[position]
            if character == "\\":
                position += 1
                if position == len(dsn):
                    raise ValueError("invalid trailing escape in PostgreSQL DSN")
                value.append(dsn[position])
                position += 1
            elif quoted and character == "'":
                position += 1
                break
            elif not quoted and character.isspace():
                break
            else:
                value.append(character)
                position += 1
        else:
            if quoted:
                raise ValueError("unterminated quoted value in PostgreSQL DSN")
        parsed[key] = "".join(value)

    aliases = {"dbname": "database", "sslmode": "ssl", "connect_timeout": "timeout"}
    allowed = {"database", "host", "password", "port", "ssl", "timeout", "user"}
    result = {}
    for key, value in parsed.items():
        target = aliases.get(key, key)
        if target not in allowed:
            raise ValueError(f"unsupported key-value PostgreSQL DSN option: {key}")
        if target == "port":
            value = int(value)
        elif target == "timeout":
            value = float(value)
        result[target] = value
    return result


def redact_dsn(dsn):
    uri = re.match(r"^(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*://)(?P<rest>.*)$", dsn, re.DOTALL)
    if uri:
        rest = uri.group("rest")
        authority_end = min(
            (index for index in (rest.find("/"), rest.find("?"), rest.find("#")) if index >= 0),
            default=len(rest),
        )
        authority = rest[:authority_end]
        suffix = rest[authority_end:]
        at = authority.rfind("@")
        if at >= 0:
            userinfo = authority[:at]
            colon = userinfo.find(":")
            if colon >= 0:
                authority = userinfo[: colon + 1] + "REDACTED" + authority[at:]
        suffix = re.sub(
            r"([?&;])([^=&#;]*)(=)([^&#;]*)",
            lambda match: (
                match.group(1)
                + match.group(2)
                + match.group(3)
                + (
                    "REDACTED"
                    if re.sub(
                        r"%([0-9A-Fa-f]{2})",
                        lambda item: chr(int(item.group(1), 16)),
                        match.group(2),
                    )
                    .lower()
                    .endswith("password")
                    else match.group(4)
                )
            ),
            suffix,
        )
        return uri.group("scheme") + authority + suffix

    pattern = re.compile(
        r"(?i)(?<![A-Za-z0-9_])([A-Za-z0-9_]*password)(\s*=\s*)"
    )
    output = []
    position = 0
    while True:
        match = pattern.search(dsn, position)
        if match is None:
            output.append(dsn[position:])
            break
        output.append(dsn[position : match.end()])
        value_start = match.end()
        if value_start >= len(dsn):
            output.append("REDACTED")
            break
        quote = dsn[value_start] if dsn[value_start] in "'\"" else None
        value_end = value_start
        if quote:
            value_end += 1
            while value_end < len(dsn):
                if dsn[value_end] == "\\" and value_end + 1 < len(dsn):
                    value_end += 2
                elif dsn[value_end] == quote:
                    value_end += 1
                    break
                else:
                    value_end += 1
        else:
            while value_end < len(dsn) and not dsn[value_end].isspace():
                value_end += 1
        output.append("REDACTED")
        position = value_end
    return "".join(output)


def reproducible_command(args):
    environment = []
    if not args.external_dsn:
        environment.append(f"PG_CONFIG={shlex.quote(str(Path(args.pg_config).resolve()))}")
    raw = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]]
    command = []
    redact_next = False
    for part in raw:
        if redact_next:
            command.append(redact_dsn(part))
            redact_next = False
        elif part == "--external-dsn":
            command.append(part)
            redact_next = True
        elif part.startswith("--external-dsn="):
            command.append("--external-dsn=" + redact_dsn(part.split("=", 1)[1]))
        else:
            command.append(part)
    return " ".join(environment + [shlex.join(command)])


def release_evidence_eligible(config):
    return config["profile"] == "default" and all(
        config[name] == value for name, value in PROFILES["default"].items()
    )


def evidence_label(config):
    defaults = PROFILES[config["profile"]]
    unchanged = all(config[name] == value for name, value in defaults.items())
    if config["profile"] == "smoke" and unchanged:
        return "smoke - shortened harness validation, not performance evidence"
    if config["profile"] == "quick" and unchanged:
        return "quick - shortened exploratory harness run, not release evidence"
    if config["profile"] == "default" and unchanged:
        return "release evidence - bounded RFC 001 no-material-regression gate"
    return f"customized {config['profile']} benchmark - not release evidence"


def self_test():
    assert percentile([4, 1, 3, 2], 0.50) == 2
    assert percentile([4, 1, 3, 2], 0.95) == 4
    assert advisory_lock_parts(810001001) == (0, 810001001)
    config = {**PROFILES["quick"], "profile": "quick"}
    cases = build_cases(config)
    assert set(PRODUCERS) <= {case.producer for case in cases}
    assert {"same", "different"} <= {case.topology for case in cases}
    assert set(STRATEGIES) <= {case.strategy for case in cases}
    assert "backend_abort_recovery" in {case.scenario for case in cases}
    assert set(config["collision_sizes"]) <= {
        case.batch_size for case in cases if case.scenario == "collision"
    }
    schedule = build_schedule(cases, 2, 238)
    first = [case for rep, _, _, case in schedule if rep == 1]
    second = [case for rep, _, _, case in schedule if rep == 2]
    assert second == list(reversed(first))
    sample = {
        **asdict(cases[0]),
        "rows_per_s": 10.0,
        "operation_p50_ms": 1.0,
        "operation_p95_ms": 2.0,
        "attempt_p50_ms": 1.0,
        "attempt_p95_ms": 2.0,
        "retagged_rows": 0,
        "snapshot_claim_conflicts": 0,
        "client_retries": 0,
        "transaction_attempts": 1,
        "committed_operations": 1,
        "payload_rows_inserted": 1,
        "visible_rows": 1,
        "wal_bytes": 10,
        "estimated_dead_tuples_before_vacuum": 0,
        "vacuum_reclaimed_dead_tuples": 0,
        "vacuum_wal_bytes": 0,
    }
    assert summarize([sample])[0]["rows_per_s_mean"] == 10.0
    assert strategy_settings("historical_client_retry")[
        "ducklake.native_writer_max_retry_count"
    ] == "0"
    assert strategy_settings(SHIPPING_STRATEGY)[
        "ducklake.native_writer_reservation_queue"
    ] == "on"
    redaction_samples = {
        "postgresql://alice:secret@db/x": "postgresql://alice:REDACTED@db/x",
        "postgres://alice:p%40ss@db/x?password=query-secret&x=1": "postgres://alice:REDACTED@db/x?password=REDACTED&x=1",
        "postgres://db/x?pass%77ord=secret;sslpassword=other": "postgres://db/x?pass%77ord=REDACTED;sslpassword=REDACTED",
        "host=db password='space and \\'quote' sslpassword=other user=alice": "host=db password=REDACTED sslpassword=REDACTED user=alice",
        'host=db PASSWORD="space value" user=alice': "host=db PASSWORD=REDACTED user=alice",
        "password=bare dbname=x": "password=REDACTED dbname=x",
    }
    for original, expected in redaction_samples.items():
        assert redact_dsn(original) == expected, (original, redact_dsn(original))
    assert parse_keyword_dsn(
        "host='/tmp/socket path' port=5432 dbname=db user=alice "
        "password='space and \\'quote'"
    ) == {
        "host": "/tmp/socket path",
        "port": 5432,
        "database": "db",
        "user": "alice",
        "password": "space and 'quote",
    }
    _, _, patch_hashes = source_manifest_hash()
    expected_patches = set(
        capture(
            [
                "git",
                "-C",
                repository_root(),
                "ls-files",
                "pg_ducklake/third_party/ducklake-*.patch",
            ]
        ).splitlines()
    )
    assert set(patch_hashes) == expected_patches
    print("native_writer_performance self-test: PASS")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=PROFILES, default="default")
    parser.add_argument("--pg-config", default=os.environ.get("PG_CONFIG", "pg_config"))
    parser.add_argument(
        "--external-dsn",
        help="existing PostgreSQL DSN; requires superuser and loaded-module provenance",
    )
    parser.add_argument(
        "--extension-binary",
        help="exact pg_ducklake shared-library path for provenance, especially with --external-dsn",
    )
    parser.add_argument("--output-stem", help="path without .json/.txt/.sha256 suffix")
    parser.add_argument("--seed", type=int, default=238)
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--duration", type=float)
    parser.add_argument("--warmup", type=float)
    parser.add_argument("--clients", help="comma-separated positive client counts")
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--collision-sizes", help="comma-separated positive row counts")
    parser.add_argument("--collision-cycles", type=int)
    parser.add_argument("--hol-samples", type=int)
    parser.add_argument("--hol-delay-ms", type=int)
    parser.add_argument("--keep-cluster", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def positive_list(value):
    parsed = [int(item) for item in value.split(",")]
    if not parsed or any(item <= 0 for item in parsed):
        raise ValueError("list values must be positive")
    return parsed


def make_config(args):
    config = {**PROFILES[args.profile], "profile": args.profile, "seed": args.seed}
    for name in (
        "repetitions",
        "duration",
        "warmup",
        "batch_size",
        "collision_cycles",
        "hol_samples",
        "hol_delay_ms",
    ):
        value = getattr(args, name)
        if value is not None:
            config[name] = value
    if args.clients:
        config["clients"] = positive_list(args.clients)
    if args.collision_sizes:
        config["collision_sizes"] = positive_list(args.collision_sizes)
    numeric_positive = (
        "repetitions",
        "duration",
        "batch_size",
        "collision_cycles",
        "hol_samples",
        "hol_delay_ms",
    )
    if any(config[name] <= 0 for name in numeric_positive) or config["warmup"] < 0:
        raise ValueError("durations, counts, and sizes must be positive")
    config["queue_wait_cap_ms"] = QUEUE_WAIT_CAP_MS
    config["evidence_label"] = evidence_label(config)
    return config


async def server_provenance(cluster, extension_binary, require_external_provenance=False):
    conn = await cluster.connect()
    try:
        current_user, is_superuser = await conn.fetchrow(
            "SELECT current_user, rolsuper FROM pg_roles WHERE rolname = current_user"
        )
        if not is_superuser:
            raise PermissionError("benchmark connection must be a PostgreSQL superuser")
        has_loaded_modules = await conn.fetchval(
            "SELECT to_regprocedure('pg_catalog.pg_get_loaded_modules()') IS NOT NULL"
        )
        loaded_module = None
        if has_loaded_modules:
            rows = await conn.fetch(
                "SELECT module_name, version, file_name FROM pg_get_loaded_modules()"
            )
            loaded_module = next(
                (
                    dict(row)
                    for row in rows
                    if row["module_name"] == "pg_ducklake"
                    or (
                        row["file_name"]
                        and Path(row["file_name"]).name.startswith("pg_ducklake")
                    )
                ),
                None,
            )
        provenance = {
            "postgres_version": await conn.fetchval("SELECT version()"),
            "extension_version": await conn.fetchval(
                "SELECT default_version FROM pg_available_extensions "
                "WHERE name = 'pg_ducklake'"
            ),
            "server_version_num": await conn.fetchval("SHOW server_version_num"),
            "server_user": current_user,
            "server_user_is_superuser": is_superuser,
            "server_loaded_extension_module": loaded_module,
        }
        if require_external_provenance:
            if loaded_module is None:
                raise RuntimeError(
                    "external server-loaded pg_ducklake binary provenance is unavailable; "
                    "pg_get_loaded_modules() must report it, refusing artifact generation"
                )
            loaded_path = Path(loaded_module["file_name"]).expanduser()
            expected_path = Path(extension_binary["path"])
            if loaded_path.is_absolute():
                matches = loaded_path.is_file() and os.path.samefile(
                    loaded_path, expected_path
                )
                match_basis = "absolute server path matched with samefile"
            else:
                matches = loaded_path.name == expected_path.name
                match_basis = "server-reported filename matched explicit local path"
            if not matches:
                raise RuntimeError(
                    "--extension-binary does not match the server-loaded pg_ducklake binary; "
                    "refusing artifact generation"
                )
            provenance["server_loaded_extension_binary_match_basis"] = match_basis
            provenance["server_loaded_extension_binary_sha256"] = sha256_file(
                expected_path
            )
        return provenance
    finally:
        await conn.close()


async def benchmark(args, config):
    cases = build_cases(config)
    schedule = build_schedule(cases, config["repetitions"], config["seed"])
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    default_stem = Path(__file__).resolve().parent / "results" / f"native-writer-{timestamp}"
    stem = Path(args.output_stem).resolve() if args.output_stem else default_stem
    json_path = stem.with_suffix(".json")
    text_path = stem.with_suffix(".txt")
    hash_path = stem.with_suffix(".sha256")
    source_hash, source_file_count, patch_hashes = source_manifest_hash()
    extension_binary = extension_binary_provenance(args.pg_config, args.extension_binary)
    if args.external_dsn:
        if not args.extension_binary:
            raise RuntimeError(
                "--external-dsn requires explicit --extension-binary provenance; "
                "refusing artifact generation"
            )
        if not extension_binary.get("exists"):
            raise RuntimeError(
                "--extension-binary is unavailable; refusing artifact generation"
            )

    cluster = (
        ExternalCluster(args.external_dsn)
        if args.external_dsn
        else LocalCluster(
            args.pg_config,
            max_connections=max(config["clients"]) + 20,
            keep=args.keep_cluster,
        )
    )
    report = None
    reporter = None
    database_prefix = f"rfc001_{os.getpid()}_{int(time.time())}"
    try:
        cluster.start()
        provenance = await server_provenance(
            cluster,
            extension_binary,
            require_external_provenance=bool(args.external_dsn),
        )
        report = {
            "schema_version": 3,
            "status": "running",
            "started_at": utc_now(),
            "config": config,
            "strategy_descriptions": strategy_descriptions(),
            "strategy_gucs": {
                strategy: strategy_settings(strategy) for strategy in STRATEGIES
            },
            "environment": {
                "git_commit": git_value("rev-parse", "HEAD"),
                "git_status_porcelain": git_value("status", "--short"),
                "source_manifest_sha256": source_hash,
                "source_manifest_file_count": source_file_count,
                "ducklake_patch_sha256": patch_hashes,
                "harness_sha256": sha256_file(__file__),
                "machine": machine_name(),
                "platform": platform.platform(),
                "python": platform.python_version(),
                "python_executable": sys.executable,
                "working_directory": str(Path.cwd()),
                "pg_config": str(Path(args.pg_config).resolve()),
                "cluster_mode": cluster.mode,
                "external_dsn": redact_dsn(args.external_dsn)
                if args.external_dsn
                else None,
                "extension_binary": extension_binary,
                "reproducible_command": reproducible_command(args),
                "duckdb_gitlink": git_value("rev-parse", "HEAD:duckdb"),
                "ducklake_gitlink": git_value(
                    "rev-parse", "HEAD:pg_ducklake/third_party/ducklake"
                ),
                **provenance,
            },
            "runs": [],
        }
        reporter = Reporter(json_path, text_path, hash_path, report)
        report["environment"]["cluster_settings"] = (
            {
                "listen_addresses": "",
                "transport": "Unix socket only",
                "fsync": "off",
                "synchronous_commit": "off",
                "autovacuum": "off",
                "maintenance": "off",
                "reservation_queue_capacity": 256,
                "reservation_queue_wait_ms": QUEUE_WAIT_CAP_MS,
            }
            if cluster.mode == "local_unix_socket"
            else {
                "managed_by_harness": False,
                "requirements": "idle server, superuser, pg_ducklake preloaded, loaded-module provenance",
            }
        )
        reporter.checkpoint()
        total = len(schedule)
        for run_number, (repetition, order_index, order_seed, case) in enumerate(
            schedule, 1
        ):
            print(
                f"[{run_number}/{total}] rep={repetition} order={order_index} "
                f"{case.scenario}/{case.producer}/{case.topology}/c{case.clients}/"
                f"{case.strategy}/b{case.batch_size}",
                flush=True,
            )
            result = await execute_case(
                cluster, database_prefix, case, config, run_number
            )
            result.update(
                {
                    "run_number": run_number,
                    "repetition": repetition,
                    "order_index": order_index,
                    "order_seed": order_seed,
                }
            )
            report["runs"].append(result)
            reporter.checkpoint()
        reporter.finish()
    except Exception as error:
        if report is not None and reporter is not None:
            report["status"] = "failed"
            report["failure"] = f"{type(error).__name__}: {error}"
            reporter.checkpoint()
        raise
    finally:
        cluster.stop()
    print(f"json_results={json_path}")
    print(f"text_summary={text_path}")
    print(f"sha256_manifest={hash_path}")


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return
    if args.external_dsn and args.keep_cluster:
        raise ValueError("--keep-cluster is not applicable with --external-dsn")
    config = make_config(args)
    asyncio.run(benchmark(args, config))


if __name__ == "__main__":
    main()
