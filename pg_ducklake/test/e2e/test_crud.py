# CRUD e2e tests for ducklake tables; the `lake` fixture runs each test
# against local and s3 storage. asyncpg speaks the extended query protocol,
# covering the protocol surface that pg_regress (simple protocol) cannot.

import io

import asyncpg
import pytest


async def test_create_insert_select(conn):
    await conn.execute(
        "CREATE TABLE t (id int, name text, val double precision) USING ducklake"
    )
    tag = await conn.execute(
        "INSERT INTO t VALUES (1, 'alice', 1.5), (2, 'bob', 2.5), (3, NULL, NULL)"
    )
    assert tag == "INSERT 0 3"
    rows = await conn.fetch("SELECT id, name, val FROM t ORDER BY id")
    assert [tuple(r) for r in rows] == [
        (1, "alice", 1.5),
        (2, "bob", 2.5),
        (3, None, None),
    ]
    assert await conn.fetchval("SELECT count(*) FROM t WHERE name IS NULL") == 1


async def test_ctas_and_insert_select(conn):
    await conn.execute(
        "CREATE TABLE src (id int, val double precision) USING ducklake"
    )
    tag = await conn.execute(
        "INSERT INTO src SELECT g, g * 1.5 FROM generate_series(1, 100) AS g(g)"
    )
    assert tag == "INSERT 0 100"

    await conn.execute(
        "CREATE TABLE dst USING ducklake AS SELECT * FROM src WHERE id <= 10"
    )
    assert await conn.fetchval("SELECT count(*) FROM dst") == 10

    tag = await conn.execute("INSERT INTO dst SELECT * FROM src WHERE id > 90")
    assert tag == "INSERT 0 10"
    assert await conn.fetchval("SELECT max(id) FROM dst") == 100


async def test_update(conn):
    await conn.execute("CREATE TABLE t (id int, val int) USING ducklake")
    await conn.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")

    tag = await conn.execute("UPDATE t SET val = val + 1 WHERE id >= 2")
    assert tag == "UPDATE 2"
    rows = await conn.fetch("SELECT id, val FROM t ORDER BY id")
    assert [tuple(r) for r in rows] == [(1, 10), (2, 21), (3, 31)]

    tag = await conn.execute("UPDATE t SET val = 0")
    assert tag == "UPDATE 3"
    assert await conn.fetchval("SELECT sum(val)::int FROM t") == 0


async def test_delete(conn):
    await conn.execute("CREATE TABLE t (id int) USING ducklake")
    await conn.execute("INSERT INTO t SELECT g FROM generate_series(1, 10) AS g(g)")

    tag = await conn.execute("DELETE FROM t WHERE id % 2 = 0")
    assert tag == "DELETE 5"
    assert await conn.fetchval("SELECT count(*) FROM t") == 5

    await conn.execute("TRUNCATE t")
    assert await conn.fetchval("SELECT count(*) FROM t") == 0

    await conn.execute("INSERT INTO t VALUES (1), (2), (3)")
    tag = await conn.execute("DELETE FROM t")
    assert tag == "DELETE 3"
    assert await conn.fetchval("SELECT count(*) FROM t") == 0


async def test_merge(conn):
    await conn.execute("CREATE TABLE tgt (id int, val text) USING ducklake")
    await conn.execute("CREATE TABLE src (id int, val text) USING ducklake")
    await conn.execute("INSERT INTO tgt VALUES (1, 'one'), (2, 'two')")
    await conn.execute("INSERT INTO src VALUES (2, 'TWO'), (3, 'three')")
    # skip after the inserts so the s3 bucket canary in the lake fixture
    # still sees objects on PG 14
    if conn.get_server_version().major < 15:
        pytest.skip("MERGE requires PostgreSQL 15+")

    tag = await conn.execute(
        """
        MERGE INTO tgt t USING src s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET val = s.val
        WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)
        """
    )
    assert tag == "MERGE 2"
    rows = await conn.fetch("SELECT id, val FROM tgt ORDER BY id")
    assert [tuple(r) for r in rows] == [(1, "one"), (2, "TWO"), (3, "three")]


async def test_transactions(conn):
    await conn.execute("CREATE TABLE t (id int) USING ducklake")

    async with conn.transaction():
        await conn.execute("INSERT INTO t VALUES (1)")
        await conn.execute("INSERT INTO t VALUES (2)")
    assert await conn.fetchval("SELECT count(*) FROM t") == 2

    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with conn.transaction():
            await conn.execute("INSERT INTO t VALUES (3)")
            # uncommitted rows are visible inside the transaction
            assert await conn.fetchval("SELECT count(*) FROM t") == 3
            raise Boom()
    assert await conn.fetchval("SELECT count(*) FROM t") == 2


async def test_alter_table(conn):
    await conn.execute("CREATE TABLE t (id int, name text) USING ducklake")
    await conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

    await conn.execute("ALTER TABLE t ADD COLUMN score int DEFAULT 7")
    rows = await conn.fetch("SELECT id, name, score FROM t ORDER BY id")
    assert [tuple(r) for r in rows] == [(1, "a", 7), (2, "b", 7)]

    await conn.execute("ALTER TABLE t RENAME COLUMN name TO label")
    assert await conn.fetchval("SELECT label FROM t WHERE id = 1") == "a"

    await conn.execute("ALTER TABLE t ALTER COLUMN id TYPE bigint")
    assert await conn.fetchval("SELECT id FROM t WHERE label = 'b'") == 2

    await conn.execute("ALTER TABLE t DROP COLUMN score")
    rows = await conn.fetch("SELECT * FROM t ORDER BY id")
    assert [tuple(r) for r in rows] == [(1, "a"), (2, "b")]

    await conn.execute("ALTER TABLE t RENAME TO t2")
    assert await conn.fetchval("SELECT count(*) FROM t2") == 2


async def _id_column_minmax(conn, table="t"):
    """Table-level min/max recorded for the single 'id' column -- the
    deterministic, observation-insensitive oracle for the ALTER TYPE
    stats regressions below (query results alone are cache-sensitive)."""
    row = await conn.fetchrow(
        """
        SELECT cs.min_value, cs.max_value
        FROM ducklake.ducklake_table_column_stats cs
        JOIN ducklake.ducklake_table t ON t.table_id = cs.table_id
        WHERE t.table_name = $1 AND t.end_snapshot IS NULL AND cs.column_id = 1
        """,
        table,
    )
    return (row["min_value"], row["max_value"])


async def test_alter_column_type_preserves_stats(conn):
    """Regression for an upstream DuckLake stats-cache defect (ducklake patch
    007 half 1): the cache was keyed without schema_version, nulling table
    min/max after ALTER TYPE. The pre-ALTER SELECT warms the cache that
    triggers the bug."""
    await conn.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
    await conn.execute("CREATE TABLE t (id int) USING ducklake")
    await conn.execute("INSERT INTO t VALUES (1)")
    assert await conn.fetchval("SELECT id FROM t") == 1  # warms the stats cache

    await conn.execute("ALTER TABLE t ALTER COLUMN id TYPE bigint")
    await conn.execute("INSERT INTO t VALUES (4)")
    assert await _id_column_minmax(conn) == ("1", "4")
    await conn.execute("INSERT INTO t VALUES (5)")
    assert await _id_column_minmax(conn) == ("1", "5")
    assert [r[0] for r in await conn.fetch("SELECT id FROM t ORDER BY id")] == [1, 4, 5]


async def test_alter_column_type_in_txn_preserves_stats(conn):
    """Intra-transaction variant (ALTER TYPE + INSERT commit together): the
    stats seed legitimately reads the pre-ALTER type, so only merge-side type
    reconciliation (ducklake patch 007 half 2) keeps the min/max."""
    await conn.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
    await conn.execute("CREATE TABLE t (id int) USING ducklake")
    await conn.execute("INSERT INTO t VALUES (1)")
    async with conn.transaction():
        await conn.execute("ALTER TABLE t ALTER COLUMN id TYPE bigint")
        await conn.execute("INSERT INTO t VALUES (4)")
    assert await _id_column_minmax(conn) == ("1", "4")
    assert [r[0] for r in await conn.fetch("SELECT id FROM t ORDER BY id")] == [1, 4]


async def test_time_travel(conn, lake):
    await conn.execute("CREATE TABLE tt (id int, name text) USING ducklake")
    v0 = await conn.fetchval(
        "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
    )
    await conn.execute("INSERT INTO tt VALUES (1, 'alice')")
    await conn.execute("INSERT INTO tt VALUES (2, 'bob')")

    # A view pins the snapshot's concrete column set, so it stays
    # describable under the extended protocol.
    await conn.execute(
        f"CREATE VIEW tt_v1 AS SELECT * FROM ducklake.time_travel('tt', {v0 + 1})"
    )
    rows = await conn.fetch("SELECT id, name FROM tt_v1 ORDER BY id")
    assert [tuple(r) for r in rows] == [(1, "alice")]

    # count(*) over the function scan has a concrete result type as well
    assert (
        await conn.fetchval(
            f"SELECT count(*) FROM ducklake.time_travel('tt', {v0 + 2})"
        )
        == 2
    )
    assert (
        await conn.fetchval(
            "SELECT count(*) FROM ducklake.time_travel('tt', now())"
        )
        == 2
    )


@pytest.mark.xfail(
    reason=(
        "extended-protocol seam: SELECT * over SETOF duckdb_row functions "
        "(time_travel, duckdb_query, ...) loses column types at Describe "
        "time and the values arrive text-typed ('1' instead of 1); wrap in "
        "a view (see test_time_travel) for typed results"
    ),
    strict=False,
)
async def test_time_travel_direct_fetch(conn):
    """SELECT * directly from ducklake.time_travel() through the extended
    protocol: the SETOF duckdb_row result shape should be resolvable at
    Describe time for wire clients like asyncpg."""
    await conn.execute("CREATE TABLE tt (id int) USING ducklake")
    await conn.execute("INSERT INTO tt VALUES (1)")
    v = await conn.fetchval(
        "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
    )
    rows = await conn.fetch(f"SELECT * FROM ducklake.time_travel('tt', {v})")
    assert [tuple(r) for r in rows] == [(1,)]


async def test_parameterized_dml(conn):
    """Extended-protocol bind parameters, the bread and butter of every
    driver built on PREPARE (asyncpg, JDBC, ...)."""
    await conn.execute("CREATE TABLE t (id int, name text) USING ducklake")

    tag = await conn.execute("INSERT INTO t VALUES ($1, $2)", 1, "alice")
    assert tag == "INSERT 0 1"
    tag = await conn.execute("INSERT INTO t VALUES ($1, $2)", 2, "bob")
    assert tag == "INSERT 0 1"
    await conn.execute("INSERT INTO t VALUES ($1, $2)", 3, "carol")

    assert (
        await conn.fetchval("SELECT name FROM t WHERE id = $1", 2) == "bob"
    )

    tag = await conn.execute("UPDATE t SET name = $2 WHERE id = $1", 3, "carl")
    assert tag == "UPDATE 1"
    tag = await conn.execute("DELETE FROM t WHERE name = $1", "alice")
    assert tag == "DELETE 1"
    rows = await conn.fetch("SELECT id, name FROM t ORDER BY id")
    assert [tuple(r) for r in rows] == [(2, "bob"), (3, "carl")]


async def test_executemany_pipeline(conn):
    """asyncpg's executemany pipelines Bind/Execute in one implicit
    transaction, so the DuckLake commit runs at Sync-time PRE_COMMIT with no
    portal or active snapshot; this used to SIGABRT the backend."""
    await conn.execute("CREATE TABLE t (id int, name text) USING ducklake")
    await conn.executemany(
        "INSERT INTO t VALUES ($1, $2)", [(1, "a"), (2, "b"), (3, "c")]
    )
    assert await conn.fetchval("SELECT count(*) FROM t") == 3
    rows = await conn.fetch("SELECT id, name FROM t ORDER BY id")
    assert [tuple(r) for r in rows] == [(1, "a"), (2, "b"), (3, "c")]


async def test_prepared_statement_reuse(conn):
    await conn.execute("CREATE TABLE t (id int) USING ducklake")
    await conn.execute("INSERT INTO t SELECT g FROM generate_series(1, 5) AS g(g)")
    stmt = await conn.prepare("SELECT count(*) FROM t WHERE id <= $1")
    assert await stmt.fetchval(3) == 3
    assert await stmt.fetchval(5) == 5
    # the plan survives more data arriving
    await conn.execute("INSERT INTO t VALUES (6)")
    assert await stmt.fetchval(100) == 6


async def test_copy_from_stdin(conn, lake):
    """COPY ... FROM STDIN goes through the inlined-data path: it requires
    data inlining enabled plus the inlined data table; flushing afterwards
    materializes parquet files on the lake storage."""
    # Ordering is load-bearing: enable inlining and recycle the DuckDB
    # instance BEFORE creating the table. A table created while inlining is
    # off never reads inlined rows back (COPY succeeds, SELECT returns nothing).
    await conn.execute("CALL ducklake.set_option('data_inlining_row_limit', 1000)")
    await conn.execute("CALL ducklake.recycle_ddb()")
    if lake.s3:
        await lake.configure_s3(conn)  # recycle dropped the secret
    await conn.execute(
        "CREATE TABLE t (id int, name text, val double precision) USING ducklake"
    )
    await conn.fetch("SELECT count(*) FROM ducklake.ensure_inlined_data_table('t'::regclass)")

    await conn.copy_to_table(
        "t",
        source=io.BytesIO(b"1,alice,1.5\n2,bob,2.5\n3,,3.5\n"),
        format="csv",
        null="",
    )
    rows = await conn.fetch("SELECT id, name, val FROM t ORDER BY id")
    assert [tuple(r) for r in rows] == [
        (1, "alice", 1.5),
        (2, "bob", 2.5),
        (3, None, 3.5),
    ]
    # the rows really are inlined: no data files exist yet
    assert (
        await conn.fetchval("SELECT count(*) FROM ducklake.list_files('public', 't')")
        == 0
    )

    # flushing materializes the inlined rows as parquet on the lake storage
    # without changing query results
    await conn.fetch("SELECT count(*) FROM ducklake.flush_inlined_data('t'::regclass)")
    assert (
        await conn.fetchval("SELECT count(*) FROM ducklake.list_files('public', 't')")
        > 0
    )
    rows = await conn.fetch("SELECT id, name, val FROM t ORDER BY id")
    assert len(rows) == 3 and tuple(rows[0]) == (1, "alice", 1.5)


async def test_aggregates(conn):
    """Aggregates whose result type PG and DuckDB agree on."""
    await conn.execute(
        "CREATE TABLE t (id int, val double precision) USING ducklake"
    )
    await conn.execute(
        "INSERT INTO t SELECT g, g * 0.5 FROM generate_series(1, 1000) AS g(g)"
    )
    assert await conn.fetchval("SELECT count(*) FROM t") == 1000
    assert await conn.fetchval("SELECT min(id) FROM t") == 1
    assert await conn.fetchval("SELECT max(id) FROM t") == 1000
    assert await conn.fetchval("SELECT sum(val) FROM t") == pytest.approx(250250.0)
    assert await conn.fetchval("SELECT avg(val) FROM t") == pytest.approx(250.25)
    rows = await conn.fetch(
        "SELECT id % 3 AS g, count(*) AS n FROM t GROUP BY 1 ORDER BY 1"
    )
    assert [tuple(r) for r in rows] == [(0, 333), (1, 334), (2, 333)]


@pytest.mark.xfail(
    reason=(
        "known kernel seam: under the extended protocol the cached plan "
        "carries PG parser result types (numeric for avg/sum(int)) while "
        "DuckDB produces double/hugeint, corrupting or erroring results; "
        "pg_regress does not catch this because it uses the simple protocol"
    ),
    strict=False,
)
async def test_aggregates_numeric_seam(conn):
    await conn.execute("CREATE TABLE t (id int) USING ducklake")
    await conn.execute("INSERT INTO t SELECT g FROM generate_series(1, 100) AS g(g)")
    # PG types these numeric; DuckDB computes hugeint / double
    assert await conn.fetchval("SELECT sum(id) FROM t") == 5050
    assert float(await conn.fetchval("SELECT avg(id) FROM t")) == pytest.approx(50.5)
    assert float(
        await conn.fetchval("SELECT round(avg(id) / 2.0, 2) FROM t")
    ) == pytest.approx(25.25)


async def test_add_column_with_text_default(conn):
    """PG deparses string DEFAULTs with a cast ('x'::text), which DuckLake
    rejected as non-literal; ducklake patch 008 constant-folds
    cast-over-constant defaults."""
    await conn.execute("CREATE TABLE t (id int) USING ducklake")
    await conn.execute("INSERT INTO t VALUES (1)")
    await conn.execute("ALTER TABLE t ADD COLUMN tag text DEFAULT 'x'")
    assert await conn.fetchval("SELECT tag FROM t WHERE id = 1") == "x"
    await conn.execute("ALTER TABLE t ALTER COLUMN tag SET DEFAULT 'y'")
    await conn.execute("INSERT INTO t (id) VALUES (2)")
    rows = await conn.fetch("SELECT id, tag FROM t ORDER BY id")
    assert [tuple(r) for r in rows] == [(1, "x"), (2, "y")]


async def test_column_default_literal_edge_cases(conn):
    """Default-literal edge cases through deparse + patch 008: 'NaN' must
    arrive quoted (bare NaN is a column reference in DuckDB), E'...' backslash
    strings, and bytea must deparse per-byte (whole-string hex silently
    decodes wrong through DuckDB's VARCHAR->BLOB cast)."""
    import math

    await conn.execute("CREATE TABLE t (id int) USING ducklake")
    await conn.execute("INSERT INTO t VALUES (1)")

    await conn.execute("ALTER TABLE t ADD COLUMN v varchar DEFAULT 'x'::text")
    assert await conn.fetchval("SELECT v FROM t WHERE id = 1") == "x"

    await conn.execute("ALTER TABLE t ADD COLUMN f float8 DEFAULT 'NaN'")
    assert math.isnan(await conn.fetchval("SELECT f FROM t WHERE id = 1"))

    await conn.execute(r"ALTER TABLE t ADD COLUMN s text DEFAULT 'a\b'")
    assert await conn.fetchval("SELECT s FROM t WHERE id = 1") == "a\\b"

    await conn.execute(r"ALTER TABLE t ADD COLUMN bin bytea DEFAULT '\x68656c6c6f'")
    assert await conn.fetchval("SELECT bin FROM t WHERE id = 1") == b"hello"

    # new rows pick up the same defaults through PG's own expansion
    await conn.execute("INSERT INTO t (id) VALUES (2)")
    row = await conn.fetchrow("SELECT v, s, bin FROM t WHERE id = 2")
    assert tuple(row) == ("x", "a\\b", b"hello")


async def test_drop_and_recreate(conn):
    await conn.execute("CREATE TABLE t (id int) USING ducklake")
    await conn.execute("INSERT INTO t VALUES (1)")
    await conn.execute("DROP TABLE t")
    with pytest.raises(asyncpg.PostgresError):
        await conn.fetch("SELECT * FROM t")
    # same name is immediately reusable, with a different shape
    await conn.execute("CREATE TABLE t (a text, b int) USING ducklake")
    await conn.execute("INSERT INTO t VALUES ('x', 1)")
    assert await conn.fetchval("SELECT a FROM t") == "x"


async def test_bulk_roundtrip(conn):
    """A larger scan: enough rows to span row groups and exercise real
    columnar reads rather than tiny single-batch results."""
    await conn.execute(
        "CREATE TABLE big (id bigint, label text, val double precision) USING ducklake"
    )
    await conn.execute(
        """
        INSERT INTO big
        SELECT g, 'row_' || g, g * 0.25
        FROM generate_series(1, 50000) AS g(g)
        """
    )
    assert await conn.fetchval("SELECT count(*) FROM big") == 50000
    row = await conn.fetchrow(
        "SELECT label, val FROM big WHERE id = $1", 31337
    )
    assert tuple(row) == ("row_31337", 31337 * 0.25)
    assert (
        await conn.fetchval("SELECT count(*) FROM big WHERE val > $1", 12000.0)
        == 2000
    )
