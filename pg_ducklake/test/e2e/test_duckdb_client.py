# Interop with an external duckdb process attached to the same DuckLake
# catalog via ATTACH 'ducklake:postgres:...' (the README's "access your data
# with DuckDB" path). s3 read path: test_s3.py::test_duckdb_client_reads_s3_lake.

import os

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


async def test_duckdb_filters_over_direct_insert_stats(local_lake, pg):
    # Table-level min/max persisted after a direct (inlined) insert must cover
    # its rows; duckdb trusts them to fold filters that look provably false.
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute("CREATE TABLE t (id int) USING ducklake")
    await pg.execute("INSERT INTO t VALUES (1), (2)")
    await pg.execute("INSERT INTO t SELECT i FROM generate_series(1000, 1199) i")
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")

    ddb = local_lake.duckdb(read_only=True)
    try:
        assert ddb.execute(
            "SELECT count(*) FROM lake.public.t WHERE id < 500"
        ).fetchone() == (2,)
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
