# Interop with an external duckdb process attached to the same DuckLake
# catalog via ATTACH 'ducklake:postgres:...' (the README's "access your data
# with DuckDB" path). s3 read path: test_s3.py::test_duckdb_client_reads_s3_lake.

import asyncio
import os
import re
from datetime import date, timedelta
from decimal import Decimal

import asyncpg
import psycopg
import pytest

from conftest import Lake


@pytest.fixture
def local_lake(cluster, db):
    return Lake(cluster, db)


@pytest.fixture
async def pg(local_lake):
    c = await local_lake.connect()
    try:
        yield c
    finally:
        await c.close()


async def wait_for_backend_lock(conn, pid, task, timeout=10):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if task.done():
            await task
            raise AssertionError("statement completed before reaching the test lock")
        waiting = await conn.fetchval(
            "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = $1",
            pid,
        )
        if waiting:
            return
        await asyncio.sleep(0.01)
    raise TimeoutError(f"backend {pid} did not wait on a lock")


async def wait_for_backend_wait_event(conn, pid, event_type, task, timeout=10):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if task.done():
            await task
            raise AssertionError(
                f"statement completed before waiting on {event_type}"
            )
        waiting = await conn.fetchval(
            "SELECT wait_event_type = $2 FROM pg_stat_activity WHERE pid = $1",
            pid,
            event_type,
        )
        if waiting:
            return
        await asyncio.sleep(0.001)
    raise TimeoutError(f"backend {pid} did not wait on {event_type}")


async def hold_next_snapshot_claim(conn):
    await conn.execute("BEGIN")
    await conn.execute(
        """
        WITH latest AS MATERIALIZED (
          SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
          FROM ducklake.ducklake_snapshot
          ORDER BY snapshot_id DESC LIMIT 1
        ), claim AS (
          INSERT INTO ducklake.ducklake_snapshot
            (snapshot_id, snapshot_time, schema_version,
             next_catalog_id, next_file_id)
          SELECT snapshot_id + 1, now(), schema_version,
                 next_catalog_id, next_file_id + 1
          FROM latest
          RETURNING snapshot_id
        )
        INSERT INTO ducklake.ducklake_snapshot_changes
          (snapshot_id, changes_made, author, commit_message, commit_extra_info)
        SELECT snapshot_id, '', NULL, NULL, NULL FROM claim
        """
    )


async def logged_inline_stats_bytes(cluster, conn, pid):
    if cluster.log_path is None:
        pytest.skip("backend memory-context logs are unavailable")
    offset = cluster.log_path.stat().st_size
    assert await conn.fetchval("SELECT pg_log_backend_memory_contexts($1)", pid)
    deadline = asyncio.get_running_loop().time() + 10
    pattern = re.compile(r"InlineColStats: (\d+) total in")
    while asyncio.get_running_loop().time() < deadline:
        text = cluster.log_path.read_text()[offset:]
        match = pattern.search(text)
        if match:
            return int(match.group(1))
        await asyncio.sleep(0.01)
    raise TimeoutError(f"no InlineColStats memory dump for backend {pid}")


async def test_duckdb_reads_pg_writes(local_lake, pg):
    await pg.execute(
        "CREATE TABLE t (id int, name text, val double precision) USING ducklake"
    )
    await pg.execute(
        "INSERT INTO t VALUES (1, 'alice', 1.5), (2, 'bob', NULL)"
    )

    ddb = local_lake.duckdb(read_only=True)
    try:
        rows = ddb.execute(
            "SELECT id, name, val FROM lake.public.t ORDER BY id"
        ).fetchall()
        assert rows == [(1, "alice", 1.5), (2, "bob", None)]
        # aggregation pushes through duckdb's own engine over the same files
        assert ddb.execute(
            "SELECT count(*) FROM lake.public.t"
        ).fetchone() == (2,)
    finally:
        ddb.close()


async def test_duckdb_reads_native_inline_stats(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
    await pg.execute(
        "CREATE TABLE stats_t (id int, f real, d double precision) USING ducklake"
    )
    await pg.execute(
        "INSERT INTO stats_t SELECT g, g::real, g::double precision "
        "FROM generate_series(1, 200) AS g"
    )
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.fetchval(
        "SELECT count(*) FROM ducklake.ensure_inlined_data_table('stats_t'::regclass)"
    )

    # Both statements use the PostgreSQL-native writer. A fresh external
    # reader must not prune their rows using the older parquet bounds.
    await pg.execute("INSERT INTO stats_t VALUES (1000, 1000, 1000)")
    await pg.execute("INSERT INTO stats_t VALUES (NULL, NULL, NULL)")
    await pg.execute("INSERT INTO stats_t VALUES (1001, 'NaN', 'NaN')")
    await pg.execute("INSERT INTO stats_t VALUES (1002, 'Infinity', '-Infinity')")

    ddb = local_lake.duckdb(read_only=True)
    try:
        assert ddb.execute(
            "SELECT count(*) FROM lake.public.stats_t WHERE id = 1000"
        ).fetchone() == (1,)
        assert ddb.execute(
            "SELECT count(*) FROM lake.public.stats_t WHERE f = 1000"
        ).fetchone() == (1,)
        assert ddb.execute(
            "SELECT count(*) FROM lake.public.stats_t WHERE d = 1000"
        ).fetchone() == (1,)
        assert ddb.execute(
            "SELECT count(*) FROM lake.public.stats_t WHERE id IS NULL"
        ).fetchone() == (1,)
        assert ddb.execute(
            "SELECT count(*) FROM lake.public.stats_t WHERE isnan(f) AND isnan(d)"
        ).fetchone() == (1,)
        assert ddb.execute(
            "SELECT count(*) FROM lake.public.stats_t WHERE isinf(f) AND isinf(d)"
        ).fetchone() == (1,)
    finally:
        ddb.close()


async def test_duckdb_reads_native_copy_stats(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
    await pg.execute(
        "CREATE TABLE copy_stats (id int, dropped_col int, v int) USING ducklake"
    )
    await pg.execute(
        "INSERT INTO copy_stats SELECT g, g, g FROM generate_series(1, 200) AS g"
    )
    await pg.execute("ALTER TABLE copy_stats DROP COLUMN dropped_col")
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.fetchval(
        "SELECT count(*) FROM ducklake.ensure_inlined_data_table('copy_stats'::regclass)"
    )
    await pg.copy_records_to_table(
        "copy_stats", records=[(1000, 2000)], columns=["id", "v"]
    )

    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
    await pg.execute("CREATE TABLE copy_nested (id int, v int[]) USING ducklake")
    await pg.execute(
        "INSERT INTO copy_nested SELECT g, ARRAY[g, g + 1] "
        "FROM generate_series(1, 200) AS g"
    )
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.fetchval(
        "SELECT count(*) FROM ducklake.ensure_inlined_data_table('copy_nested'::regclass)"
    )
    await pg.copy_records_to_table("copy_nested", records=[(1000, [1000, 2000])])

    ddb = local_lake.duckdb(read_only=True)
    try:
        assert ddb.execute(
            "SELECT id, v FROM lake.public.copy_stats WHERE id = 1000"
        ).fetchall() == [(1000, 2000)]
        # Inspect descendant stats through the fresh external process's
        # PostgreSQL attachment; nested PostgreSQL inlined rows are not yet
        # transformable by the external DuckLake reader.
        password = os.environ.get("PGPASSWORD")
        password_option = f" password={password}" if password else ""
        ddb.execute(
            f"ATTACH 'host={local_lake.cluster.host} "
            f"port={local_lake.cluster.port} dbname={local_lake.dbname} "
            f"user=postgres{password_option}' AS catalog "
            "(TYPE postgres, READ_ONLY)"
        )
        assert ddb.execute(
            "SELECT bool_and(s.min_value IS NULL AND s.max_value IS NULL "
            "AND s.contains_null IS NULL AND s.contains_nan IS NULL "
            "AND s.extra_stats IS NULL) "
            "FROM catalog.ducklake.ducklake_table_column_stats s "
            "JOIN catalog.ducklake.ducklake_table t USING (table_id) "
            "JOIN catalog.ducklake.ducklake_column c USING (table_id, column_id) "
            "WHERE t.table_name = 'copy_nested' AND t.end_snapshot IS NULL "
            "AND c.end_snapshot IS NULL AND c.parent_column IS NOT NULL"
        ).fetchone() == (True,)
    finally:
        ddb.close()


async def test_native_copy_loses_snapshot_claim_without_replay(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
    await pg.execute(
        "CREATE TABLE copy_collision (id int, writer text) USING ducklake"
    )
    await pg.execute(
        "INSERT INTO copy_collision "
        "SELECT g, 'baseline' FROM generate_series(0, 199) AS g"
    )
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('copy_collision'::regclass)"
    )
    initial = await pg.fetchrow(
        "SELECT t.table_id, s.snapshot_id, ts.record_count, ts.next_row_id "
        "FROM ducklake.ducklake_table t "
        "CROSS JOIN LATERAL (SELECT max(snapshot_id) AS snapshot_id "
        "                    FROM ducklake.ducklake_snapshot) s "
        "JOIN ducklake.ducklake_table_stats ts USING (table_id) "
        "WHERE t.table_name = 'copy_collision' AND t.end_snapshot IS NULL"
    )

    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    # The trigger pauses only the COPY backend after it has read its candidate
    # and reached the actual snapshot INSERT. Another writer can then commit
    # that candidate first, making the COPY claim deterministically lose.
    advisory_key = 724_913_557
    await pg.execute(
        f"""
        CREATE FUNCTION public.block_copy_snapshot_claim() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = 'copy_snapshot_loser' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_copy_snapshot_claim
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_copy_snapshot_claim();
        """
    )

    class TrackingRows:
        def __init__(self):
            self.iterations = 0
            self.yielded = 0
            self.completed = 0

        def __iter__(self):
            self.iterations += 1
            assert self.iterations == 1
            for row in [(1000, "copy"), (1001, "copy"), (1002, "copy")]:
                self.yielded += 1
                yield row
            self.completed += 1

    blocker = await local_lake.connect()
    winner = await local_lake.connect()
    copy_conn = await local_lake.connect(
        server_settings={"application_name": "copy_snapshot_loser"}
    )
    rows = TrackingRows()
    copy_task = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        copy_pid = await copy_conn.fetchval("SELECT pg_backend_pid()")
        copy_task = asyncio.create_task(
            copy_conn.copy_records_to_table("copy_collision", records=rows)
        )
        await wait_for_backend_lock(pg, copy_pid, copy_task)
        assert (rows.iterations, rows.yielded, rows.completed) == (1, 3, 1)

        await winner.execute(
            "INSERT INTO copy_collision VALUES (900, 'winner'), (901, 'winner')"
        )
        assert await pg.fetchval(
            "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
        ) == initial["snapshot_id"] + 1

        assert await blocker.fetchval(
            "SELECT pg_advisory_unlock($1)", advisory_key
        )
        await copy_task
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        if copy_task is not None and not copy_task.done():
            copy_task.cancel()
            await asyncio.gather(copy_task, return_exceptions=True)
        await blocker.close()
        await winner.close()
        await copy_conn.close()

    inlined_table = await pg.fetchval(
        "SELECT it.table_name FROM ducklake.ducklake_inlined_data_tables it "
        "JOIN ducklake.ducklake_table t USING (table_id) "
        "WHERE t.table_name = 'copy_collision' AND t.end_snapshot IS NULL "
        "ORDER BY it.schema_version DESC LIMIT 1"
    )
    owned = await pg.fetch(
        f"SELECT convert_from(writer, 'UTF8') AS writer, count(*) AS rows, "
        f"min(row_id) AS min_row_id, max(row_id) AS max_row_id, "
        f"min(begin_snapshot) AS begin_snapshot, "
        f"count(DISTINCT begin_snapshot) AS snapshot_count, "
        f"count(DISTINCT xmin::text) AS owner_count "
        f"FROM ducklake.{inlined_table} GROUP BY writer ORDER BY writer"
    )
    assert [tuple(row) for row in owned] == [
        (
            "copy",
            3,
            initial["next_row_id"] + 2,
            initial["next_row_id"] + 4,
            initial["snapshot_id"] + 2,
            1,
            1,
        ),
        (
            "winner",
            2,
            initial["next_row_id"],
            initial["next_row_id"] + 1,
            initial["snapshot_id"] + 1,
            1,
            1,
        ),
    ]
    assert await pg.fetchval(
        f"SELECT count(DISTINCT xmin::text) = 2 FROM ducklake.{inlined_table}"
    )

    metadata = await pg.fetchrow(
        "SELECT ts.record_count, ts.next_row_id, "
        "       (SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot) AS snapshot_id, "
        "       (SELECT count(*) FROM ducklake.ducklake_snapshot "
        "        WHERE snapshot_id > $2) AS snapshots, "
        "       (SELECT count(*) FROM ducklake.ducklake_snapshot_changes "
        "        WHERE snapshot_id > $2) AS changes, "
        "       cs.min_value, cs.max_value "
        "FROM ducklake.ducklake_table_stats ts "
        "JOIN ducklake.ducklake_table t USING (table_id) "
        "JOIN ducklake.ducklake_column c USING (table_id) "
        "JOIN ducklake.ducklake_table_column_stats cs "
        "  USING (table_id, column_id) "
        "WHERE t.table_id = $1 AND c.column_name = 'id' "
        "  AND c.end_snapshot IS NULL",
        initial["table_id"],
        initial["snapshot_id"],
    )
    assert tuple(metadata) == (
        initial["record_count"] + 5,
        initial["next_row_id"] + 5,
        initial["snapshot_id"] + 2,
        2,
        2,
        "0",
        "1002",
    )
    writer_stats = dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    )
    assert writer_stats == {
        "payload_rows": 5,
        "publication_attempts": 3,
        "snapshot_claim_conflicts": 1,
        "rows_retagged": 5,
        "retry_exhaustions": 0,
        "copy_rows_consumed": 3,
    }

    changes = await pg.fetch(
        "SELECT snapshot_id, changes_made "
        "FROM ducklake.ducklake_snapshot_changes "
        "WHERE snapshot_id > $1 ORDER BY snapshot_id",
        initial["snapshot_id"],
    )
    assert [tuple(row) for row in changes] == [
        (initial["snapshot_id"] + 1, f"inlined_insert:{initial['table_id']}"),
        (initial["snapshot_id"] + 2, f"inlined_insert:{initial['table_id']}"),
    ]

    ddb = local_lake.duckdb(read_only=True)
    try:
        assert ddb.execute(
            "SELECT id, writer FROM lake.public.copy_collision "
            "WHERE writer IN ('copy', 'winner') ORDER BY id"
        ).fetchall() == [
            (900, "winner"),
            (901, "winner"),
            (1000, "copy"),
            (1001, "copy"),
            (1002, "copy"),
        ]
    finally:
        ddb.close()


async def test_native_reservation_queue_orders_and_avoids_retags(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute(
        "CREATE TABLE queue_order (id int, writer text) USING ducklake"
    )
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('queue_order'::regclass)"
    )
    inlined_table = await pg.fetchval(
        "SELECT it.table_name FROM ducklake.ducklake_inlined_data_tables it "
        "JOIN ducklake.ducklake_table t USING (table_id) "
        "WHERE t.table_name = 'queue_order' AND t.end_snapshot IS NULL "
        "ORDER BY it.schema_version DESC LIMIT 1"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    advisory_key = 724_913_564
    await pg.execute(
        f"""
        CREATE FUNCTION public.block_queue_order_head() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = 'queue_order_head' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_queue_order_head
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_queue_order_head();
        """
    )

    settings = {
        "ducklake.native_writer_reservation_queue": "on",
        "ducklake.native_writer_reservation_queue_wait_ms": "5000ms",
        "ducklake.native_writer_max_retry_count": "50",
        "ducklake.native_writer_retry_wait_ms": "100ms",
        "ducklake.native_writer_retry_backoff": "1",
    }
    blocker = await local_lake.connect()
    head = await local_lake.connect(
        server_settings={**settings, "application_name": "queue_order_head"}
    )
    successor = await local_lake.connect(server_settings=settings)
    source_started = asyncio.Event()
    source_release = asyncio.Event()

    async def copy_source():
        source_started.set()
        yield b"1\thead\n"
        await source_release.wait()
        yield b"2\thead\n"

    head_task = successor_task = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        head_pid = await head.fetchval("SELECT pg_backend_pid()")
        head_task = asyncio.create_task(
            head.copy_to_table("queue_order", source=copy_source())
        )
        await asyncio.wait_for(source_started.wait(), timeout=10)

        successor_pid = await successor.fetchval("SELECT pg_backend_pid()")
        successor_task = asyncio.create_task(
            successor.execute(
                "INSERT INTO queue_order VALUES (3, 'successor'), (4, 'successor')"
            )
        )
        # The follower cannot reserve row IDs until unknown-count COPY finishes.
        await wait_for_backend_wait_event(
            pg, successor_pid, "Extension", successor_task
        )
        source_release.set()
        await wait_for_backend_lock(pg, head_pid, head_task)
        assert await blocker.fetchval(
            "SELECT pg_advisory_unlock($1)", advisory_key
        )
        await asyncio.gather(head_task, successor_task)
    finally:
        source_release.set()
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        for task in (head_task, successor_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await blocker.close()
        await head.close()
        await successor.close()

    assert [
        tuple(row)
        for row in await pg.fetch(
            f"SELECT row_id, id, convert_from(writer, 'UTF8') "
            f"FROM ducklake.{inlined_table} ORDER BY row_id"
        )
    ] == [
        (0, 1, "head"),
        (1, 2, "head"),
        (2, 3, "successor"),
        (3, 4, "successor"),
    ]
    stats = dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    )
    assert stats["payload_rows"] == 4
    assert stats["publication_attempts"] == 2
    assert stats["snapshot_claim_conflicts"] == 0
    assert stats["rows_retagged"] == 0


@pytest.mark.parametrize("terminate", [False, True], ids=["cancel", "backend-exit"])
async def test_native_reservation_queue_recovers_missing_head(
    local_lake, pg, terminate
):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE queue_abort (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('queue_abort'::regclass)"
    )
    inlined_table = await pg.fetchval(
        "SELECT it.table_name FROM ducklake.ducklake_inlined_data_tables it "
        "JOIN ducklake.ducklake_table t USING (table_id) "
        "WHERE t.table_name = 'queue_abort' AND t.end_snapshot IS NULL "
        "ORDER BY it.schema_version DESC LIMIT 1"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    advisory_key = 724_913_565
    await pg.execute(
        f"""
        CREATE FUNCTION public.block_queue_abort_head() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = 'queue_abort_head' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_queue_abort_head
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_queue_abort_head();
        """
    )

    settings = {
        "ducklake.native_writer_reservation_queue": "on",
        "ducklake.native_writer_reservation_queue_wait_ms": "1000ms",
    }
    blocker = await local_lake.connect()
    head = await local_lake.connect(
        server_settings={**settings, "application_name": "queue_abort_head"}
    )
    successor = await local_lake.connect(server_settings=settings)
    head_task = successor_task = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        head_pid = await head.fetchval("SELECT pg_backend_pid()")
        head_task = asyncio.create_task(
            head.execute("INSERT INTO queue_abort VALUES (1), (2)")
        )
        await wait_for_backend_lock(pg, head_pid, head_task)

        successor_pid = await successor.fetchval("SELECT pg_backend_pid()")
        successor_task = asyncio.create_task(
            successor.execute("INSERT INTO queue_abort VALUES (3), (4)")
        )
        await wait_for_backend_wait_event(
            pg, successor_pid, "Extension", successor_task
        )

        if terminate:
            assert await pg.fetchval("SELECT pg_terminate_backend($1)", head_pid)
            result = await asyncio.gather(head_task, return_exceptions=True)
            assert isinstance(result[0], asyncpg.ConnectionDoesNotExistError)
        else:
            assert await pg.fetchval("SELECT pg_cancel_backend($1)", head_pid)
            with pytest.raises(asyncpg.QueryCanceledError):
                await head_task
        await asyncio.wait_for(successor_task, timeout=10)
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        for task in (head_task, successor_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await blocker.close()
        if not head.is_closed():
            await head.close()
        await successor.close()

    assert [tuple(row) for row in await pg.fetch("SELECT id FROM queue_abort ORDER BY id")] == [
        (3,),
        (4,),
    ]
    assert [
        tuple(row)
        for row in await pg.fetch(
            f"SELECT row_id, id FROM ducklake.{inlined_table} ORDER BY row_id"
        )
    ] == [(0, 3), (1, 4)]
    stats = dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    )
    assert stats["payload_rows"] == 4
    assert stats["publication_attempts"] == 2
    assert stats["snapshot_claim_conflicts"] == 0
    assert stats["rows_retagged"] == 2


async def test_native_reservation_queue_rebases_external_commit(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute(
        "CREATE TABLE queue_external (id int, writer text) USING ducklake"
    )
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('queue_external'::regclass)"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    advisory_key = 724_913_566
    await pg.execute(
        f"""
        CREATE FUNCTION public.block_queue_external_head() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = 'queue_external_head' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_queue_external_head
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_queue_external_head();
        """
    )

    settings = {
        "ducklake.native_writer_reservation_queue": "on",
        "ducklake.native_writer_reservation_queue_wait_ms": "1000ms",
    }
    blocker = await local_lake.connect()
    head = await local_lake.connect(
        server_settings={**settings, "application_name": "queue_external_head"}
    )
    successor = await local_lake.connect(server_settings=settings)
    head_task = successor_task = None
    ddb = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        head_pid = await head.fetchval("SELECT pg_backend_pid()")
        head_task = asyncio.create_task(
            head.execute(
                "INSERT INTO queue_external VALUES (10, 'head'), (11, 'head')"
            )
        )
        await wait_for_backend_lock(pg, head_pid, head_task)

        successor_pid = await successor.fetchval("SELECT pg_backend_pid()")
        successor_task = asyncio.create_task(
            successor.execute(
                "INSERT INTO queue_external VALUES "
                "(20, 'successor'), (21, 'successor')"
            )
        )
        await wait_for_backend_wait_event(
            pg, successor_pid, "Extension", successor_task
        )

        ddb = local_lake.duckdb()
        ddb.execute(
            "INSERT INTO lake.public.queue_external VALUES (1, 'external')"
        )
        assert await blocker.fetchval(
            "SELECT pg_advisory_unlock($1)", advisory_key
        )
        await asyncio.gather(head_task, successor_task)
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        for task in (head_task, successor_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        if ddb is not None:
            ddb.close()
        await blocker.close()
        await head.close()
        await successor.close()

    assert [
        tuple(row)
        for row in await pg.fetch(
            "SELECT id, writer FROM queue_external ORDER BY id"
        )
    ] == [
        (1, "external"),
        (10, "head"),
        (11, "head"),
        (20, "successor"),
        (21, "successor"),
    ]
    stats = dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    )
    assert stats["payload_rows"] == 4
    assert stats["snapshot_claim_conflicts"] >= 1
    assert stats["publication_attempts"] == 2 + stats["snapshot_claim_conflicts"]
    assert stats["rows_retagged"] == 4

    reader = local_lake.duckdb(read_only=True)
    try:
        assert reader.execute(
            "SELECT count(*), count(DISTINCT id) "
            "FROM lake.public.queue_external"
        ).fetchone() == (5, 5)
    finally:
        reader.close()


@pytest.mark.parametrize("same_table", [False, True], ids=["cross-table", "same-table"])
async def test_native_reservation_queue_stalled_head_deadline(
    local_lake, pg, same_table
):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE queue_deadline_a (id int) USING ducklake")
    await pg.execute("CREATE TABLE queue_deadline_b (id int) USING ducklake")
    for table in ("queue_deadline_a", "queue_deadline_b"):
        await pg.fetchval(
            "SELECT count(*) FROM "
            f"ducklake.ensure_inlined_data_table('{table}'::regclass)"
        )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    advisory_key = 724_913_567 + int(same_table)
    await pg.execute(
        f"""
        CREATE FUNCTION public.block_queue_deadline_head() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = 'queue_deadline_head' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_queue_deadline_head
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_queue_deadline_head();
        """
    )

    settings = {
        "ducklake.native_writer_reservation_queue": "on",
        "ducklake.native_writer_reservation_queue_wait_ms": "10ms",
        "ducklake.native_writer_max_retry_count": "1",
        "ducklake.native_writer_retry_wait_ms": "500ms",
        "ducklake.native_writer_retry_backoff": "1",
    }
    blocker = await local_lake.connect()
    head = await local_lake.connect(
        server_settings={**settings, "application_name": "queue_deadline_head"}
    )
    follower = await local_lake.connect(server_settings=settings)
    head_task = follower_task = None
    follower_table = "queue_deadline_a" if same_table else "queue_deadline_b"
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        head_pid = await head.fetchval("SELECT pg_backend_pid()")
        head_task = asyncio.create_task(
            head.execute("INSERT INTO queue_deadline_a VALUES (1), (2)")
        )
        await wait_for_backend_lock(pg, head_pid, head_task)

        follower_pid = await follower.fetchval("SELECT pg_backend_pid()")
        started = asyncio.get_running_loop().time()
        follower_task = asyncio.create_task(
            follower.execute(f"INSERT INTO {follower_table} VALUES (3), (4)")
        )
        await wait_for_backend_wait_event(
            pg, follower_pid, "Extension", follower_task
        )
        await asyncio.wait_for(follower_task, timeout=3)
        elapsed = asyncio.get_running_loop().time() - started
        assert 0.005 <= elapsed < 0.25
        assert not head_task.done()

        assert await blocker.fetchval(
            "SELECT pg_advisory_unlock($1)", advisory_key
        )
        await head_task
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        for task in (head_task, follower_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await blocker.close()
        await head.close()
        await follower.close()

    assert await pg.fetchval("SELECT count(*) FROM queue_deadline_a") == (
        4 if same_table else 2
    )
    assert await pg.fetchval("SELECT count(*) FROM queue_deadline_b") == (
        0 if same_table else 2
    )
    changes = await pg.fetch(
        "SELECT changes_made FROM ducklake.ducklake_snapshot_changes "
        "ORDER BY snapshot_id DESC LIMIT 2"
    )
    table_ids = {
        row["table_name"]: row["table_id"]
        for row in await pg.fetch(
            "SELECT table_name, table_id FROM ducklake.ducklake_table "
            "WHERE table_name IN ('queue_deadline_a', 'queue_deadline_b') "
            "AND end_snapshot IS NULL"
        )
    }
    assert changes[0]["changes_made"] == f"inlined_insert:{table_ids['queue_deadline_a']}"
    assert changes[1]["changes_made"] == f"inlined_insert:{table_ids[follower_table]}"
    stats = dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    )
    assert stats["snapshot_claim_conflicts"] == 1
    assert stats["rows_retagged"] == 4


async def test_native_reservation_queue_wait_is_interruptible(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE queue_interrupt (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('queue_interrupt'::regclass)"
    )

    advisory_key = 724_913_569
    await pg.execute(
        f"""
        CREATE FUNCTION public.block_queue_interrupt_head() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = 'queue_interrupt_head' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_queue_interrupt_head
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_queue_interrupt_head();
        """
    )

    settings = {
        "ducklake.native_writer_reservation_queue": "on",
        "ducklake.native_writer_reservation_queue_wait_ms": "5000ms",
        "ducklake.native_writer_max_retry_count": "100",
        "ducklake.native_writer_retry_wait_ms": "100ms",
        "ducklake.native_writer_retry_backoff": "1",
    }
    blocker = await local_lake.connect()
    head = await local_lake.connect(
        server_settings={**settings, "application_name": "queue_interrupt_head"}
    )
    follower = await local_lake.connect(server_settings=settings)
    head_task = follower_task = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        head_pid = await head.fetchval("SELECT pg_backend_pid()")
        head_task = asyncio.create_task(
            head.execute("INSERT INTO queue_interrupt VALUES (1)")
        )
        await wait_for_backend_lock(pg, head_pid, head_task)

        follower_pid = await follower.fetchval("SELECT pg_backend_pid()")
        follower_task = asyncio.create_task(
            follower.execute("INSERT INTO queue_interrupt VALUES (2)")
        )
        await wait_for_backend_wait_event(pg, follower_pid, "Extension", follower_task)
        assert await pg.fetchval("SELECT pg_cancel_backend($1)", follower_pid)
        with pytest.raises(asyncpg.QueryCanceledError):
            await asyncio.wait_for(follower_task, timeout=1)
        assert not head_task.done()

        assert await blocker.fetchval("SELECT pg_advisory_unlock($1)", advisory_key)
        await head_task
        await follower.execute("INSERT INTO queue_interrupt VALUES (3)")
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        for task in (head_task, follower_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await blocker.close()
        await head.close()
        await follower.close()

    assert await pg.fetchval(
        "SELECT array_agg(id ORDER BY id) FROM queue_interrupt"
    ) == [1, 3]


async def test_native_reservation_queue_rejects_stale_committed_anchor(
    local_lake, pg
):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE queue_anchor_a (id int) USING ducklake")
    await pg.execute("CREATE TABLE queue_anchor_b (id int) USING ducklake")
    for table in ("queue_anchor_a", "queue_anchor_b"):
        await pg.fetchval(
            "SELECT count(*) FROM "
            f"ducklake.ensure_inlined_data_table('{table}'::regclass)"
        )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    queued = await local_lake.connect(
        server_settings={"ducklake.native_writer_reservation_queue": "on"}
    )
    ordinary = await local_lake.connect(
        server_settings={"ducklake.native_writer_reservation_queue": "off"}
    )
    try:
        await queued.execute("INSERT INTO queue_anchor_a VALUES (1)")
        await ordinary.execute("INSERT INTO queue_anchor_b VALUES (2)")
        await queued.execute("INSERT INTO queue_anchor_a VALUES (3)")
    finally:
        await queued.close()
        await ordinary.close()

    assert await pg.fetchval(
        "SELECT array_agg(id ORDER BY id) FROM queue_anchor_a"
    ) == [1, 3]
    snapshots = await pg.fetch(
        "SELECT snapshot_id FROM ducklake.ducklake_snapshot_changes "
        "ORDER BY snapshot_id DESC LIMIT 3"
    )
    assert [row["snapshot_id"] for row in snapshots] == list(
        range(snapshots[0]["snapshot_id"], snapshots[0]["snapshot_id"] - 3, -1)
    )
    stats = dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    )
    assert stats["snapshot_claim_conflicts"] == 0
    assert stats["rows_retagged"] == 0


async def test_native_reservation_queue_full_reclaims_stale_catalog(local_lake, pg):
    capacity = int(
        await pg.fetchval("SHOW ducklake.native_writer_reservation_queue_capacity")
    )
    if capacity > 16:
        pytest.skip("queue capacity is too large for bounded connection coverage")

    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    tables = [f"queue_capacity_{index}" for index in range(capacity + 2)]
    for table in tables:
        await pg.execute(f"CREATE TABLE {table} (id int) USING ducklake")
        await pg.fetchval(
            "SELECT count(*) FROM "
            f"ducklake.ensure_inlined_data_table('{table}'::regclass)"
        )

    settings = {
        "ducklake.native_writer_reservation_queue": "on",
        "ducklake.native_writer_max_retry_count": "100",
        "ducklake.native_writer_retry_wait_ms": "5ms",
        "ducklake.native_writer_retry_backoff": "1",
    }
    connections = []
    tasks = []
    releases = []
    pids = []

    async def start_held_copy(table, value):
        connection = await local_lake.connect(server_settings=settings)
        started = asyncio.Event()
        release = asyncio.Event()

        async def source():
            started.set()
            yield f"{value}\n".encode()
            await release.wait()

        pid = await connection.fetchval("SELECT pg_backend_pid()")
        task = asyncio.create_task(connection.copy_to_table(table, source=source()))
        await asyncio.wait_for(started.wait(), timeout=10)
        connections.append(connection)
        tasks.append(task)
        releases.append(release)
        pids.append(pid)

    try:
        for index in range(capacity):
            await start_held_copy(tables[index], index)

        # The capacity miss cannot wait behind the held streams.
        overflow = await local_lake.connect(server_settings=settings)
        try:
            await asyncio.wait_for(
                overflow.execute(
                    f"INSERT INTO {tables[capacity]} VALUES ({capacity})"
                ),
                timeout=2,
            )
        finally:
            await overflow.close()

        assert await pg.fetchval(f"SELECT count(*) FROM {tables[capacity]}") == 1
        assert await pg.fetchval("SELECT pg_terminate_backend($1)", pids[0])
        releases[0].set()
        await asyncio.gather(tasks[0], return_exceptions=True)

        # The non-queued commit made the remaining catalog reservations stale;
        # a new writer must reclaim them rather than observe a full queue.
        await start_held_copy(tables[capacity + 1], capacity + 1)
    finally:
        for release in releases:
            release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        for connection in connections:
            if not connection.is_closed():
                await connection.close()

    assert await pg.fetchval(f"SELECT count(*) FROM {tables[capacity + 1]}") == 1


async def test_native_reservation_queue_survives_clean_restart(local_lake, pg, cluster):
    if not hasattr(cluster, "pgdata"):
        pytest.skip("restart coverage requires the local throwaway cluster")

    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE queue_restart (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('queue_restart'::regclass)"
    )
    await pg.execute("SET ducklake.native_writer_reservation_queue = on")
    await pg.execute("INSERT INTO queue_restart VALUES (1)")
    await pg.close()

    cluster.stop()
    cluster.start()
    restarted = await local_lake.connect(
        server_settings={"ducklake.native_writer_reservation_queue": "on"}
    )
    try:
        await restarted.execute("INSERT INTO queue_restart VALUES (2)")
        assert await restarted.fetchval(
            "SELECT array_agg(id ORDER BY id) FROM queue_restart"
        ) == [1, 2]
    finally:
        await restarted.close()


async def test_large_native_unnest_bounds_converted_batches(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 25000)")
    await pg.execute("CREATE TABLE unnest_batch (d date) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('unnest_batch'::regclass)"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    statement = await pg.prepare(
        "INSERT INTO unnest_batch SELECT UNNEST($1::date[])"
    )
    start = date(1970, 1, 1)
    await statement.fetch([start + timedelta(days=i) for i in range(20000)])

    assert await pg.fetchval("SELECT count(*) FROM unnest_batch") == 20000
    bounds = await pg.fetchrow("SELECT min(d), max(d) FROM unnest_batch")
    assert tuple(bounds) == (start, start + timedelta(days=19999))
    stats = dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    )
    assert stats["payload_rows"] == 20000
    assert stats["publication_attempts"] == 1
    assert stats["snapshot_claim_conflicts"] == 0


async def test_large_native_unnest_retags_once_after_collision(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 5000)")
    await pg.execute("CREATE TABLE unnest_collision (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('unnest_collision'::regclass)"
    )
    initial_snapshot = await pg.fetchval(
        "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    advisory_key = 724_913_559
    await pg.execute(
        f"""
        CREATE FUNCTION public.block_unnest_snapshot_claim() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = 'unnest_snapshot_loser' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_unnest_snapshot_claim
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_unnest_snapshot_claim();
        """
    )

    blocker = await local_lake.connect()
    winner = await local_lake.connect()
    loser = await local_lake.connect(
        server_settings={"application_name": "unnest_snapshot_loser"}
    )
    loser_task = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        loser_task = asyncio.create_task(
            loser.execute(
                "INSERT INTO unnest_collision SELECT UNNEST($1::int[])",
                list(range(1000, 3500)),
            )
        )
        await wait_for_backend_lock(pg, loser_pid, loser_task, timeout=30)
        await winner.execute("INSERT INTO unnest_collision VALUES (1), (2)")
        assert await blocker.fetchval(
            "SELECT pg_advisory_unlock($1)", advisory_key
        )
        await loser_task
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        if loser_task is not None and not loser_task.done():
            loser_task.cancel()
            await asyncio.gather(loser_task, return_exceptions=True)
        await blocker.close()
        await winner.close()
        await loser.close()

    result = await pg.fetchrow(
        "SELECT count(*), min(id), max(id) FROM unnest_collision"
    )
    assert tuple(result) == (2502, 1, 3499)
    stats = dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    )
    assert stats == {
        "payload_rows": 2502,
        "publication_attempts": 3,
        "snapshot_claim_conflicts": 1,
        "rows_retagged": 2502,
        "retry_exhaustions": 0,
        "copy_rows_consumed": 0,
    }
    assert await pg.fetchval(
        "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
    ) == initial_snapshot + 2

    ddb = local_lake.duckdb(read_only=True)
    try:
        assert ddb.execute(
            "SELECT count(*), count(DISTINCT id) "
            "FROM lake.public.unnest_collision"
        ).fetchone() == (2502, 2502)
    finally:
        ddb.close()


async def test_explicit_transaction_uses_duckdb_fallback(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE explicit_batch (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('explicit_batch'::regclass)"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")
    snapshot_before = await pg.fetchval(
        "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
    )

    await pg.execute("BEGIN")
    await pg.execute("INSERT INTO explicit_batch VALUES (1)")
    await pg.execute("INSERT INTO explicit_batch VALUES (2), (3)")
    assert await pg.fetchval("SELECT count(*) FROM explicit_batch") == 3
    await pg.execute("COMMIT")

    assert await pg.fetchval(
        "SELECT count FROM ducklake.native_writer_stats() "
        "WHERE event = 'payload_rows'"
    ) == 0
    assert await pg.fetchval(
        "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
    ) == snapshot_before + 1
    ddb = local_lake.duckdb(read_only=True)
    try:
        assert ddb.execute(
            "SELECT id FROM lake.public.explicit_batch ORDER BY id"
        ).fetchall() == [(1,), (2,), (3,)]
    finally:
        ddb.close()


async def test_parameterized_unnest_rejects_duckdb_fallback(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE unnest_fallback (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('unnest_fallback'::regclass)"
    )
    statement = await pg.prepare(
        "INSERT INTO unnest_fallback SELECT UNNEST($1::int[])"
    )
    await statement.fetch([1])

    error = "parameterized UNNEST is not supported by the DuckDB fallback"
    await pg.execute("SET ducklake.enable_direct_insert = false")
    with pytest.raises(asyncpg.FeatureNotSupportedError, match=error):
        await statement.fetch([2])

    await pg.execute("SET ducklake.enable_direct_insert = true")
    await pg.execute("BEGIN")
    try:
        with pytest.raises(asyncpg.FeatureNotSupportedError, match=error):
            await statement.fetch([3])
    finally:
        await pg.execute("ROLLBACK")

    assert await pg.fetchval("SELECT array_agg(id) FROM unnest_fallback") == [1]


async def test_native_copy_rejects_unsupported_semantics_before_consuming(
    local_lake, pg
):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE copy_tx (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM ducklake.ensure_inlined_data_table('copy_tx'::regclass)"
    )

    async def connect_copy(user="postgres"):
        connection = await psycopg.AsyncConnection.connect(
            host=local_lake.cluster.host,
            port=local_lake.cluster.port,
            dbname=local_lake.dbname,
            user=user,
            password=os.environ.get("PGPASSWORD"),
        )
        await connection.set_autocommit(True)
        return connection

    async def assert_rejected_before_source(connection, query, error):
        source_started = False
        with pytest.raises(error):
            async with connection.cursor().copy(query) as copy:
                source_started = True
                await copy.write(b"1\n")
        assert not source_started

    await pg.execute("SELECT ducklake.reset_native_writer_stats()")
    copy_conn = await connect_copy()
    try:
        await copy_conn.execute("BEGIN")
        await assert_rejected_before_source(
            copy_conn,
            "COPY copy_tx FROM STDIN",
            psycopg.errors.FeatureNotSupported,
        )
        await copy_conn.execute("ROLLBACK")
    finally:
        await copy_conn.close()
    assert await pg.fetchval("SELECT count(*) FROM copy_tx") == 0
    assert await pg.fetchval(
        "SELECT count FROM ducklake.native_writer_stats() "
        "WHERE event = 'copy_rows_consumed'"
    ) == 0

    await pg.execute("CREATE TABLE copy_constrained (id int NOT NULL) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('copy_constrained'::regclass)"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")
    copy_conn = await connect_copy()
    try:
        await assert_rejected_before_source(
            copy_conn,
            "COPY copy_constrained FROM STDIN",
            psycopg.errors.FeatureNotSupported,
        )
    finally:
        await copy_conn.close()
    assert await pg.fetchval("SELECT count(*) FROM copy_constrained") == 0
    assert await pg.fetchval(
        "SELECT count FROM ducklake.native_writer_stats() "
        "WHERE event = 'copy_rows_consumed'"
    ) == 0

    await pg.execute("CREATE ROLE copy_no_insert LOGIN")
    await pg.execute("GRANT USAGE ON SCHEMA public TO copy_no_insert")
    await pg.execute("GRANT SELECT ON copy_tx TO copy_no_insert")
    denied = await connect_copy(user="copy_no_insert")
    try:
        await assert_rejected_before_source(
            denied,
            "COPY copy_tx FROM STDIN",
            psycopg.errors.InsufficientPrivilege,
        )
    finally:
        await denied.close()
    await pg.execute("REVOKE ALL ON copy_tx FROM copy_no_insert")
    await pg.execute("REVOKE ALL ON SCHEMA public FROM copy_no_insert")
    await pg.execute("DROP ROLE copy_no_insert")


async def test_native_copy_counts_partial_consumption_and_prewrite(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 2000)")
    await pg.execute("CREATE TABLE partial_copy (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('partial_copy'::regclass)"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    async def source():
        yield "".join(f"{value}\n" for value in range(1001)).encode()
        yield b"not-an-integer\n"

    with pytest.raises(asyncpg.InvalidTextRepresentationError):
        await pg.copy_to_table("partial_copy", source=source())

    assert await pg.fetchval("SELECT count(*) FROM partial_copy") == 0
    assert dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    ) == {
        "payload_rows": 1000,
        "publication_attempts": 0,
        "snapshot_claim_conflicts": 0,
        "rows_retagged": 0,
        "retry_exhaustions": 0,
        "copy_rows_consumed": 1001,
    }


async def test_external_append_wins_before_native_claim(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute(
        "CREATE TABLE external_claim (id int, writer text) USING ducklake"
    )
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('external_claim'::regclass)"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    advisory_key = 724_913_558
    await pg.execute(
        f"""
        CREATE FUNCTION public.block_native_snapshot_claim() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = 'external_claim_loser' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_native_snapshot_claim
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_native_snapshot_claim();
        """
    )

    blocker = await local_lake.connect()
    loser = await local_lake.connect(
        server_settings={"application_name": "external_claim_loser"}
    )
    loser_task = None
    ddb = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        loser_task = asyncio.create_task(
            loser.execute(
                "INSERT INTO external_claim VALUES "
                "(10, 'native'), (11, 'native')"
            )
        )
        await wait_for_backend_lock(pg, loser_pid, loser_task)

        ddb = local_lake.duckdb()
        ddb.execute("INSERT INTO lake.public.external_claim VALUES (20, 'external')")

        assert await blocker.fetchval(
            "SELECT pg_advisory_unlock($1)", advisory_key
        )
        await loser_task
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        if loser_task is not None and not loser_task.done():
            loser_task.cancel()
            await asyncio.gather(loser_task, return_exceptions=True)
        if ddb is not None:
            ddb.close()
        await blocker.close()
        await loser.close()

    assert [
        tuple(row)
        for row in await pg.fetch(
            "SELECT id, writer FROM external_claim ORDER BY id"
        )
    ] == [(10, "native"), (11, "native"), (20, "external")]
    assert tuple(
        await pg.fetchrow(
            "SELECT ts.record_count, ts.next_row_id "
            "FROM ducklake.ducklake_table_stats ts "
            "JOIN ducklake.ducklake_table t USING (table_id) "
            "WHERE t.table_name = 'external_claim' AND t.end_snapshot IS NULL"
        )
    ) == (3, 3)

    writer_stats = dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    )
    assert writer_stats == {
        "payload_rows": 2,
        "publication_attempts": 2,
        "snapshot_claim_conflicts": 1,
        "rows_retagged": 2,
        "retry_exhaustions": 0,
        "copy_rows_consumed": 0,
    }


@pytest.mark.parametrize(
    "queue_enabled", [False, True], ids=["queue-off", "queue-on"]
)
@pytest.mark.parametrize(
    ("change", "external_sql"),
    [
        ("alter", "ALTER TABLE lake.public.{table} ADD COLUMN added INTEGER"),
        ("delete", "DELETE FROM lake.public.{table} WHERE id = 1"),
        ("drop", "DROP TABLE lake.public.{table}"),
    ],
)
async def test_external_table_change_rejects_prewritten_native_append(
    local_lake, pg, change, external_sql, queue_enabled
):
    table = f"external_{change}_race"
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute(f"CREATE TABLE {table} (id int) USING ducklake")
    await pg.fetchval(
        f"SELECT count(*) FROM ducklake.ensure_inlined_data_table('{table}'::regclass)"
    )
    await pg.execute(f"INSERT INTO {table} VALUES (1)")
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    advisory_key = {"alter": 724_913_559, "delete": 724_913_560, "drop": 724_913_561}[
        change
    ]
    application_name = f"external_{change}_loser"
    await pg.execute(
        f"""
        CREATE FUNCTION public.block_{change}_snapshot_claim() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = '{application_name}' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_{change}_snapshot_claim
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_{change}_snapshot_claim();
        """
    )

    blocker = await local_lake.connect()
    loser = await local_lake.connect(
        server_settings={
            "application_name": application_name,
            "ducklake.native_writer_reservation_queue": (
                "on" if queue_enabled else "off"
            ),
        }
    )
    loser_task = None
    ddb = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        loser_task = asyncio.create_task(
            loser.execute(f"INSERT INTO {table} VALUES (10)")
        )
        await wait_for_backend_lock(pg, loser_pid, loser_task)

        ddb = local_lake.duckdb()
        ddb.execute(external_sql.format(table=table))
        assert await blocker.fetchval(
            "SELECT pg_advisory_unlock($1)", advisory_key
        )
        with pytest.raises(
            asyncpg.SerializationError,
            match="conflicting change|target table changed",
        ):
            await loser_task
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        if loser_task is not None and not loser_task.done():
            loser_task.cancel()
            await asyncio.gather(loser_task, return_exceptions=True)
        if ddb is not None:
            ddb.close()
        await blocker.close()
        await loser.close()

    assert dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    ) == {
        "payload_rows": 1,
        "publication_attempts": 1,
        "snapshot_claim_conflicts": 1,
        "rows_retagged": 0,
        "retry_exhaustions": 0,
        "copy_rows_consumed": 0,
    }
    if change == "delete":
        assert await pg.fetchval(f"SELECT count(*) FROM {table}") == 0


@pytest.mark.parametrize(
    "queue_enabled", [False, True], ids=["queue-off", "queue-on"]
)
async def test_native_prewrite_races_flush_bounded(local_lake, pg, queue_enabled):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE flush_race (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('flush_race'::regclass)"
    )
    await pg.execute("INSERT INTO flush_race VALUES (1), (2)")
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    advisory_key = 724_913_562
    await pg.execute(
        f"""
        CREATE FUNCTION public.block_flush_race_claim() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = 'flush_race_loser' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_flush_race_claim
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_flush_race_claim();
        """
    )

    blocker = await local_lake.connect()
    loser = await local_lake.connect(
        server_settings={
            "application_name": "flush_race_loser",
            "ducklake.native_writer_reservation_queue": (
                "on" if queue_enabled else "off"
            ),
        }
    )
    maintenance = await local_lake.connect()
    loser_task = None
    maintenance_task = None
    maintenance_pid = await maintenance.fetchval("SELECT pg_backend_pid()")
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        loser_task = asyncio.create_task(
            loser.execute("INSERT INTO flush_race VALUES (10)")
        )
        await wait_for_backend_lock(pg, loser_pid, loser_task)

        await maintenance.execute("SET statement_timeout = '15s'")
        maintenance_task = asyncio.create_task(
            maintenance.fetchval(
                "SELECT count(*) FROM "
                "ducklake.flush_inlined_data('flush_race'::regclass)"
            )
        )
        try:
            assert await asyncio.wait_for(maintenance_task, timeout=20) >= 1
        except TimeoutError:
            await pg.fetchval("SELECT pg_terminate_backend($1)", maintenance_pid)
            await asyncio.gather(maintenance_task, return_exceptions=True)
            raise

        assert await pg.fetchval(
            "SELECT changes_made LIKE 'inline_flush:%' "
            "FROM ducklake.ducklake_snapshot_changes "
            "ORDER BY snapshot_id DESC LIMIT 1"
        )
        assert await blocker.fetchval(
            "SELECT pg_advisory_unlock($1)", advisory_key
        )
        await loser_task
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        for task in (loser_task, maintenance_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await blocker.close()
        await loser.close()
        if not maintenance.is_closed():
            await maintenance.close(timeout=5)

    assert await pg.fetchval("SELECT count(*) FROM flush_race") == 3
    assert await pg.fetchval("SELECT count(*) FROM flush_race WHERE id = 10") == 1
    assert await pg.fetchval(
        "SELECT count(*) FROM ducklake.ducklake_data_file d "
        "JOIN ducklake.ducklake_table t USING (table_id) "
        "WHERE t.table_name = 'flush_race' AND t.end_snapshot IS NULL "
        "AND d.end_snapshot IS NULL"
    ) >= 1
    assert dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    ) == {
        "payload_rows": 1,
        "publication_attempts": 2,
        "snapshot_claim_conflicts": 1,
        "rows_retagged": 1,
        "retry_exhaustions": 0,
        "copy_rows_consumed": 0,
    }


@pytest.mark.parametrize(
    "queue_enabled", [False, True], ids=["queue-off", "queue-on"]
)
async def test_native_prewrite_races_compaction_bounded(
    local_lake, pg, queue_enabled
):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE compaction_race (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('compaction_race'::regclass)"
    )
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
    for value in range(1, 6):
        await pg.execute(f"INSERT INTO compaction_race VALUES ({value})")
    files_before = await pg.fetchval(
        "SELECT count(*) FROM ducklake.ducklake_data_file d "
        "JOIN ducklake.ducklake_table t USING (table_id) "
        "WHERE t.table_name = 'compaction_race' AND t.end_snapshot IS NULL "
        "AND d.end_snapshot IS NULL"
    )
    assert files_before >= 5
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    advisory_key = 724_913_563
    await pg.execute(
        f"""
        CREATE FUNCTION public.block_compaction_race_claim() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = 'compaction_race_loser' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_compaction_race_claim
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_compaction_race_claim();
        """
    )

    blocker = await local_lake.connect()
    loser = await local_lake.connect(
        server_settings={
            "application_name": "compaction_race_loser",
            "ducklake.native_writer_reservation_queue": (
                "on" if queue_enabled else "off"
            ),
        }
    )
    maintenance = await local_lake.connect()
    loser_task = None
    maintenance_task = None
    maintenance_pid = await maintenance.fetchval("SELECT pg_backend_pid()")
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        loser_task = asyncio.create_task(
            loser.execute("INSERT INTO compaction_race VALUES (10)")
        )
        await wait_for_backend_lock(pg, loser_pid, loser_task)

        await maintenance.execute("SET statement_timeout = '15s'")
        maintenance_task = asyncio.create_task(
            maintenance.fetchval(
                "SELECT count(*) FROM "
                "ducklake.merge_adjacent_files('compaction_race'::regclass)"
            )
        )
        try:
            assert await asyncio.wait_for(maintenance_task, timeout=20) >= 1
        except TimeoutError:
            await pg.fetchval("SELECT pg_terminate_backend($1)", maintenance_pid)
            await asyncio.gather(maintenance_task, return_exceptions=True)
            raise

        files_after = await pg.fetchval(
            "SELECT count(*) FROM ducklake.ducklake_data_file d "
            "JOIN ducklake.ducklake_table t USING (table_id) "
            "WHERE t.table_name = 'compaction_race' AND t.end_snapshot IS NULL "
            "AND d.end_snapshot IS NULL"
        )
        assert files_after < files_before

        assert await blocker.fetchval(
            "SELECT pg_advisory_unlock($1)", advisory_key
        )
        await loser_task
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        for task in (loser_task, maintenance_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await blocker.close()
        await loser.close()
        if not maintenance.is_closed():
            await maintenance.close(timeout=5)

    assert await pg.fetchval("SELECT count(*) FROM compaction_race") == 6
    assert await pg.fetchval(
        "SELECT count(*) FROM compaction_race WHERE id = 10"
    ) == 1
    assert dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    ) == {
        "payload_rows": 1,
        "publication_attempts": 2,
        "snapshot_claim_conflicts": 1,
        "rows_retagged": 1,
        "retry_exhaustions": 0,
        "copy_rows_consumed": 0,
    }


async def test_cancel_native_writer_during_backoff(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE cancel_backoff (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('cancel_backoff'::regclass)"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    holder = await local_lake.connect()
    loser = await local_lake.connect()
    loser_task = None
    try:
        await hold_next_snapshot_claim(holder)
        await loser.execute("SET ducklake.native_writer_max_retry_count = 10")
        await loser.execute("SET ducklake.native_writer_retry_wait_ms = '60s'")
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        loser_task = asyncio.create_task(
            loser.execute("INSERT INTO cancel_backoff VALUES (1)")
        )
        await wait_for_backend_lock(pg, loser_pid, loser_task)

        await holder.execute("COMMIT")
        await wait_for_backend_wait_event(
            pg, loser_pid, "Extension", loser_task
        )
        assert (
            await pg.fetchval(
                "SELECT count = 1 FROM ducklake.native_writer_stats() "
                "WHERE event = 'snapshot_claim_conflicts'"
            )
            is True
        )
        assert await pg.fetchval("SELECT pg_cancel_backend($1)", loser_pid)
        with pytest.raises(asyncpg.QueryCanceledError):
            await loser_task
    finally:
        await holder.execute("ROLLBACK")
        if loser_task is not None and not loser_task.done():
            loser_task.cancel()
            await asyncio.gather(loser_task, return_exceptions=True)
        await holder.close()
        await loser.close()

    assert await pg.fetchval("SELECT count(*) FROM cancel_backoff") == 0
    assert dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    ) == {
        "payload_rows": 1,
        "publication_attempts": 1,
        "snapshot_claim_conflicts": 1,
        "rows_retagged": 0,
        "retry_exhaustions": 0,
        "copy_rows_consumed": 0,
    }


async def test_terminate_native_writer_after_prewrite(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE terminate_prewrite (id int) USING ducklake")
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('terminate_prewrite'::regclass)"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    holder = await local_lake.connect()
    loser = await local_lake.connect()
    loser_task = None
    try:
        await hold_next_snapshot_claim(holder)
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        loser_task = asyncio.create_task(
            loser.execute("INSERT INTO terminate_prewrite VALUES (1)")
        )
        await wait_for_backend_lock(pg, loser_pid, loser_task)

        assert await pg.fetchval("SELECT pg_terminate_backend($1)", loser_pid)
        result = await asyncio.gather(loser_task, return_exceptions=True)
        assert isinstance(result[0], asyncpg.ConnectionDoesNotExistError)
        await holder.execute("ROLLBACK")
    finally:
        await holder.execute("ROLLBACK")
        if loser_task is not None and not loser_task.done():
            loser_task.cancel()
            await asyncio.gather(loser_task, return_exceptions=True)
        await holder.close()
        await loser.close()

    assert await pg.fetchval("SELECT count(*) FROM terminate_prewrite") == 0
    assert dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    ) == {
        "payload_rows": 1,
        "publication_attempts": 1,
        "snapshot_claim_conflicts": 0,
        "rows_retagged": 0,
        "retry_exhaustions": 0,
        "copy_rows_consumed": 0,
    }


async def test_large_native_copy_keeps_stats_memory_bounded(local_lake, pg):
    if local_lake.cluster.log_path is None:
        pytest.skip("backend memory-context logs are unavailable")
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute(
        "CREATE TABLE copy_large (id int, n numeric, v text) USING ducklake"
    )
    await pg.fetchval(
        "SELECT count(*) FROM ducklake.ensure_inlined_data_table('copy_large'::regclass)"
    )

    blocker = await local_lake.connect()
    copy_conn = await local_lake.connect()
    copy_task = None
    try:
        await blocker.execute("BEGIN")
        await blocker.execute("LOCK ducklake.ducklake_snapshot IN SHARE MODE")
        copy_pid = await copy_conn.fetchval("SELECT pg_backend_pid()")
        records = (
            (i, Decimal(i), f"{i:08d}-" + "x" * 512) for i in range(20000)
        )
        copy_task = asyncio.create_task(
            copy_conn.copy_records_to_table("copy_large", records=records)
        )
        await wait_for_backend_lock(pg, copy_pid, copy_task, timeout=30)

        # The old implementation retained every replaced Datum/text bound in
        # this context (over 10 MB for this stream). The fixed accumulator
        # reuses a small number of chunks regardless of row count.
        stats_bytes = await logged_inline_stats_bytes(
            local_lake.cluster, pg, copy_pid
        )
        assert stats_bytes < 1_000_000
        await blocker.execute("COMMIT")
        await copy_task
    finally:
        await blocker.execute("ROLLBACK")
        if copy_task is not None and not copy_task.done():
            copy_task.cancel()
            await asyncio.gather(copy_task, return_exceptions=True)
        await blocker.close()
        await copy_conn.close()

    ddb = local_lake.duckdb(read_only=True)
    try:
        assert ddb.execute(
            "SELECT count(*), min(id), max(id) FROM lake.public.copy_large"
        ).fetchone() == (20000, 0, 19999)
    finally:
        ddb.close()


async def test_duckdb_writes_pg_reads(local_lake, pg):
    if os.environ.get("E2E_PG_HOST"):
        pytest.skip(
            "external duckdb client writes parquet to the lake's local data "
            "path, which lives inside the container and is not shared with the "
            "host running this client; s3 storage (shared bucket) is the write "
            "interop path under external-cluster mode"
        )
    await pg.execute("CREATE TABLE t (id int, name text) USING ducklake")
    await pg.execute("INSERT INTO t VALUES (1, 'from_pg')")

    ddb = local_lake.duckdb()
    try:
        ddb.execute("INSERT INTO lake.public.t VALUES (2, 'from_duckdb')")
        ddb.execute("INSERT INTO lake.public.t VALUES (3, 'doomed')")
        ddb.execute("UPDATE lake.public.t SET name = 'updated' WHERE id = 1")
        ddb.execute("DELETE FROM lake.public.t WHERE id = 3")
    finally:
        ddb.close()

    rows = await pg.fetch("SELECT id, name FROM t ORDER BY id")
    assert [tuple(r) for r in rows] == [(1, "updated"), (2, "from_duckdb")]


async def test_schema_changes_visible_to_duckdb(local_lake, pg):
    await pg.execute("CREATE TABLE t (id int) USING ducklake")
    await pg.execute("INSERT INTO t VALUES (1)")
    await pg.execute("ALTER TABLE t ADD COLUMN tag text")
    await pg.execute("INSERT INTO t VALUES (2, 'y')")

    # a fresh attach sees the evolved schema
    ddb = local_lake.duckdb(read_only=True)
    try:
        rows = ddb.execute(
            "SELECT id, tag FROM lake.public.t ORDER BY id"
        ).fetchall()
        assert rows == [(1, None), (2, "y")]
    finally:
        ddb.close()


async def test_duckdb_time_travel_at_version(local_lake, pg):
    await pg.execute("CREATE TABLE t (id int) USING ducklake")
    v0 = await pg.fetchval(
        "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
    )
    await pg.execute("INSERT INTO t VALUES (1)")
    await pg.execute("INSERT INTO t VALUES (2)")

    ddb = local_lake.duckdb(read_only=True)
    try:
        rows = ddb.execute(
            f"SELECT id FROM lake.public.t AT (VERSION => {v0 + 1}) ORDER BY id"
        ).fetchall()
        assert rows == [(1,)]
        rows = ddb.execute(
            "SELECT id FROM lake.public.t ORDER BY id"
        ).fetchall()
        assert rows == [(1,), (2,)]
    finally:
        ddb.close()


async def test_concurrent_pg_and_duckdb_readers(local_lake, pg):
    """Both clients read the lake at the same time with consistent
    results; writes from PG become visible to an already-attached duckdb
    client on its next query (snapshot metadata is re-read per query)."""
    await pg.execute("CREATE TABLE t (id int) USING ducklake")
    await pg.execute("INSERT INTO t SELECT g FROM generate_series(1, 10) AS g(g)")

    ddb = local_lake.duckdb(read_only=True)
    try:
        assert ddb.execute("SELECT count(*) FROM lake.public.t").fetchone() == (10,)
        await pg.execute("INSERT INTO t VALUES (11)")
        assert await pg.fetchval("SELECT count(*) FROM t") == 11
        assert ddb.execute("SELECT count(*) FROM lake.public.t").fetchone() == (11,)
    finally:
        ddb.close()
