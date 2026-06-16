# S3-specific e2e tests: pg_ducklake writing table data to a MinIO bucket via
# ducklake.default_table_path plus a DuckDB S3 secret. Skips without S3 infra
# (unless E2E_REQUIRE_S3=1); storage-agnostic behavior lives in test_crud.py.

import os

import asyncpg
import pytest

from conftest import Lake, skip_or_fail


@pytest.fixture
def s3lake(cluster, db, s3):
    return Lake(cluster, db, s3)


@pytest.fixture
async def s3conn(s3lake):
    c = await s3lake.connect()
    # Disable inlining (a persistent catalog option) so writes land in the
    # bucket immediately and object assertions are deterministic; the inlined
    # s3 path is still covered by test_crud.py::test_copy_from_stdin[s3].
    await c.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
    try:
        yield c
    finally:
        await c.close()


async def _load_httpfs(conn):
    # httpfs is not bundled; INSTALL downloads it on first use. Skip offline.
    try:
        await conn.execute("SELECT ducklake.raw_query('INSTALL httpfs')")
    except asyncpg.PostgresError as e:
        skip_or_fail(f"backend cannot INSTALL httpfs (offline?): {e}")
    await conn.execute("SELECT ducklake.raw_query('LOAD httpfs')")


async def test_parquet_objects_land_in_bucket(s3lake, s3conn, s3):
    # data inlining is disabled by default, so INSERT writes parquet
    # straight to the bucket
    await s3conn.execute("CREATE TABLE t (id int, name text) USING ducklake")
    await s3conn.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

    objects = s3.object_names()
    assert objects, "no objects appeared in the bucket after INSERT"
    assert any(name.endswith(".parquet") for name in objects)
    # rows read back from s3, not from any local cache of the insert
    await s3conn.execute("CALL ducklake.recycle_ddb()")
    await s3lake.configure_s3(s3conn)  # recycle dropped the secret
    rows = await s3conn.fetch("SELECT id, name FROM t ORDER BY id")
    assert [tuple(r) for r in rows] == [(1, "alice"), (2, "bob")]


async def test_metadata_reports_s3_paths(s3lake, s3conn):
    await s3conn.execute("CREATE TABLE t (id int) USING ducklake")
    await s3conn.execute("INSERT INTO t VALUES (1)")

    assert (
        await s3conn.fetchval(
            "SELECT count(*) FROM ducklake.list_files('public', 't')"
        )
        > 0
    )
    # the query escape hatch can inspect file paths; its ducklake.row
    # result shape needs the simple protocol, so go through psql
    out = s3lake.psql(
        f"""SELECT * FROM ducklake.query($q$
              SELECT bool_and(starts_with(data_file, '{s3lake.table_path}'))
              FROM ducklake_list_files('pgducklake', 't', schema => 'public')
            $q$)"""
    )
    assert out.strip().splitlines()[-1] == "t"


async def test_fresh_backend_needs_secret(s3lake, s3conn):
    await s3conn.execute("CREATE TABLE t (id int) USING ducklake")
    await s3conn.execute("INSERT INTO t VALUES (1), (2)")

    # A brand-new backend has a brand-new DuckDB instance with no S3
    # secret: reading s3-backed data must fail rather than silently
    # succeed off some cache (AWS_* env is scrubbed by the harness).
    bare = await s3lake.connect(configure=False)
    try:
        # match the bucket name so an unrelated error cannot satisfy the
        # test: the failure must come from the s3 data-file access
        with pytest.raises(asyncpg.PostgresError, match=s3lake.s3.bucket):
            await bare.fetch("SELECT * FROM t")
        await s3lake.configure_s3(bare)
        assert await bare.fetchval("SELECT count(*) FROM t") == 2
    finally:
        await bare.close()


async def test_recycle_ddb_drops_secret(s3lake, s3conn):
    await s3conn.execute("CREATE TABLE t (id int) USING ducklake")
    await s3conn.execute("INSERT INTO t VALUES (1)")
    assert await s3conn.fetchval("SELECT count(*) FROM t") == 1

    # recycle_ddb tears down the per-backend DuckDB instance; plain
    # (non-persistent) secrets die with it
    await s3conn.execute("CALL ducklake.recycle_ddb()")
    with pytest.raises(asyncpg.PostgresError, match=s3lake.s3.bucket):
        await s3conn.fetch("SELECT * FROM t")

    await s3lake.configure_s3(s3conn)
    assert await s3conn.fetchval("SELECT count(*) FROM t") == 1


async def test_update_delete_against_s3(s3conn, s3):
    await s3conn.execute("CREATE TABLE t (id int, val int) USING ducklake")
    await s3conn.execute(
        "INSERT INTO t SELECT g, g * 10 FROM generate_series(1, 100) AS g(g)"
    )
    objects_before = set(s3.object_names())

    tag = await s3conn.execute("UPDATE t SET val = -val WHERE id <= 10")
    assert tag == "UPDATE 10"
    tag = await s3conn.execute("DELETE FROM t WHERE id > 90")
    assert tag == "DELETE 10"

    # update/delete on DuckLake are copy-on-write: new data/delete files
    # must have been written to the bucket, never in-place mutation
    objects_after = set(s3.object_names())
    assert objects_before < objects_after

    assert await s3conn.fetchval("SELECT count(*) FROM t") == 90
    assert await s3conn.fetchval("SELECT min(val) FROM t") == -100
    assert await s3conn.fetchval("SELECT max(id) FROM t") == 90


async def test_time_travel_on_s3(s3conn):
    await s3conn.execute("CREATE TABLE t (id int) USING ducklake")
    v0 = await s3conn.fetchval(
        "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
    )
    await s3conn.execute("INSERT INTO t VALUES (1)")
    await s3conn.execute("INSERT INTO t VALUES (2)")
    assert (
        await s3conn.fetchval(
            f"SELECT count(*) FROM ducklake.time_travel('t', {v0 + 1})"
        )
        == 1
    )


async def test_create_s3_secret_round_trip(s3lake, s3):
    # Provision the S3 secret via the ducklake.create_s3_secret() wrapper
    # (a FOREIGN SERVER + USER MAPPING on the ducklake_secret FDW) instead of a
    # raw CREATE SECRET, then round-trip a table whose data lives in the bucket.
    conn = await s3lake.connect(configure=False)
    try:
        # httpfs is not bundled; install it explicitly to avoid offline flakiness
        try:
            await conn.execute("SELECT ducklake.raw_query('INSTALL httpfs')")
        except asyncpg.PostgresError as e:
            skip_or_fail(f"backend cannot INSTALL httpfs (offline?): {e}")
        await conn.execute("SELECT ducklake.raw_query('LOAD httpfs')")

        srv = s3lake.s3.server
        server_name = await conn.fetchval(
            "SELECT ducklake.create_s3_secret('s3', $1, $2, "
            "endpoint => $3, url_style => 'path', use_ssl => 'false')",
            srv.access_key,
            srv.secret_key,
            srv.endpoint,
        )
        assert server_name == "simple_s3_secret"

        await conn.execute(
            f"SET ducklake.default_table_path = '{s3lake.table_path}'"
        )
        # disable inlining so the INSERT writes parquet to the bucket immediately
        await conn.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
        await conn.execute("CREATE TABLE t (id int, name text) USING ducklake")
        await conn.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

        assert any(name.endswith(".parquet") for name in s3.object_names())
        rows = await conn.fetch("SELECT id, name FROM t ORDER BY id")
        assert [tuple(r) for r in rows] == [(1, "alice"), (2, "bob")]
    finally:
        await conn.close()


async def test_create_s3_secret_named_args(s3lake, s3):
    # create_s3_secret accepts named-arg notation for the optional params and
    # still round-trips (complements the positional-arg test above).
    conn = await s3lake.connect(configure=False)
    try:
        await _load_httpfs(conn)
        srv = s3lake.s3.server
        name = await conn.fetchval(
            "SELECT ducklake.create_s3_secret(type => 's3', key_id => $1, "
            "secret => $2, endpoint => $3, url_style => 'path', "
            "use_ssl => 'false', region => 'us-east-1')",
            srv.access_key,
            srv.secret_key,
            srv.endpoint,
        )
        assert name == "simple_s3_secret"
        await conn.execute(
            f"SET ducklake.default_table_path = '{s3lake.table_path}'"
        )
        await conn.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
        await conn.execute("CREATE TABLE t (id int) USING ducklake")
        await conn.execute("INSERT INTO t VALUES (1), (2)")
        assert await conn.fetchval("SELECT count(*) FROM t") == 2
    finally:
        await conn.close()


async def test_two_s3_secrets_get_unique_names(s3lake):
    # FindServerName: two create_s3_secret calls of the same type yield distinct
    # server names rather than colliding.
    conn = await s3lake.connect(configure=False)
    try:
        await _load_httpfs(conn)
        srv = s3lake.s3.server

        async def mk():
            return await conn.fetchval(
                "SELECT ducklake.create_s3_secret('s3', $1, $2, endpoint => $3, "
                "url_style => 'path', use_ssl => 'false')",
                srv.access_key,
                srv.secret_key,
                srv.endpoint,
            )

        assert await mk() == "simple_s3_secret"
        assert await mk() == "simple_s3_secret_1"
    finally:
        await conn.close()


async def test_drop_server_removes_secret(s3lake):
    # DROP SERVER invalidates the cached secrets; the next statement's
    # GetConnection drops the DuckDB secret, so s3 reads then fail.
    conn = await s3lake.connect(configure=False)
    try:
        await _load_httpfs(conn)
        srv = s3lake.s3.server
        await conn.fetchval(
            "SELECT ducklake.create_s3_secret('s3', $1, $2, endpoint => $3, "
            "url_style => 'path', use_ssl => 'false')",
            srv.access_key,
            srv.secret_key,
            srv.endpoint,
        )
        await conn.execute(
            f"SET ducklake.default_table_path = '{s3lake.table_path}'"
        )
        await conn.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
        await conn.execute("CREATE TABLE t (id int) USING ducklake")
        await conn.execute("INSERT INTO t VALUES (1)")
        assert await conn.fetchval("SELECT count(*) FROM t") == 1

        await conn.execute("DROP SERVER simple_s3_secret CASCADE")
        with pytest.raises(asyncpg.PostgresError, match=s3lake.s3.bucket):
            await conn.fetch("SELECT * FROM t")
    finally:
        await conn.close()


async def test_helper_secret_reloads_after_recycle(s3lake):
    # The PG-catalog SERVER+MAPPING persist across ducklake.recycle_ddb(); the
    # fresh DuckDB instance must reload the secret on the next GetConnection
    # (regression for DuckDBManager::Reset not clearing secrets_valid_).
    conn = await s3lake.connect(configure=False)
    try:
        await _load_httpfs(conn)
        srv = s3lake.s3.server
        await conn.fetchval(
            "SELECT ducklake.create_s3_secret('s3', $1, $2, endpoint => $3, "
            "url_style => 'path', use_ssl => 'false')",
            srv.access_key,
            srv.secret_key,
            srv.endpoint,
        )
        await conn.execute(
            f"SET ducklake.default_table_path = '{s3lake.table_path}'"
        )
        await conn.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
        await conn.execute("CREATE TABLE t (id int) USING ducklake")
        await conn.execute("INSERT INTO t VALUES (1), (2)")

        await conn.execute("CALL ducklake.recycle_ddb()")
        # No re-configuration: the secret must be re-emitted from the catalog.
        assert await conn.fetchval("SELECT count(*) FROM t") == 2
    finally:
        await conn.close()


async def test_redact_key_on_server_rejected(s3lake):
    # A secret option (redact_key) placed on the SERVER instead of the USER
    # MAPPING is rejected, and the secret VALUE never appears in the error.
    conn = await s3lake.connect(configure=False)
    try:
        await _load_httpfs(conn)
        with pytest.raises(asyncpg.PostgresError) as ei:
            await conn.execute(
                "CREATE SERVER leak_srv TYPE 's3' FOREIGN DATA WRAPPER ducklake_secret "
                "OPTIONS (secret 'TOPSECRETVALUE')"
            )
        msg = str(ei.value)
        assert "USER MAPPING" in msg
        assert "TOPSECRETVALUE" not in msg
    finally:
        await conn.close()


async def test_table_path_option_writes_to_s3_prefix(s3conn, s3):
    # CREATE TABLE ... WITH (ducklake.table_path = 's3://...') overrides
    # default_table_path per table; data files land under that prefix.
    prefix = f"s3://{s3.bucket}/custom_tp/"
    await s3conn.execute(
        f"CREATE TABLE tp (id int, v text) USING ducklake "
        f"WITH (ducklake.table_path = '{prefix}')"
    )
    await s3conn.execute("INSERT INTO tp VALUES (1, 'a'), (2, 'b')")

    objs = s3.object_names()
    assert any(n.startswith("custom_tp/") and n.endswith(".parquet") for n in objs), objs
    # and nothing leaked under the default lake/ prefix for this table
    rows = await s3conn.fetch("SELECT id, v FROM tp ORDER BY id")
    assert [tuple(r) for r in rows] == [(1, "a"), (2, "b")]


@pytest.mark.skipif(
    bool(os.environ.get("E2E_PG_HOST")),
    reason="local table path needs the PG backend on the test host",
)
async def test_local_and_s3_tables_in_one_catalog(s3conn, s3, tmp_path):
    # One catalog, mixed storage: a table pinned to a local path and a table
    # routed to the bucket by default_table_path, both readable.
    local_dir = tmp_path / "local_lake"
    local_dir.mkdir(parents=True, exist_ok=True)
    await s3conn.execute(
        f"CREATE TABLE loc (id int) USING ducklake "
        f"WITH (ducklake.table_path = '{local_dir}/')"
    )
    await s3conn.execute("INSERT INTO loc VALUES (1), (2)")
    await s3conn.execute("CREATE TABLE rem (id int) USING ducklake")  # -> bucket
    await s3conn.execute("INSERT INTO rem VALUES (10), (20)")

    assert any(local_dir.rglob("*.parquet")), f"no local parquet under {local_dir}"
    assert any(n.endswith(".parquet") for n in s3.object_names())
    assert await s3conn.fetchval("SELECT count(*) FROM loc") == 2
    assert await s3conn.fetchval("SELECT count(*) FROM rem") == 2


async def test_two_buckets_one_secret(s3conn, s3, s3_second_bucket):
    # A single endpoint secret covers every bucket on that MinIO; a per-table
    # path can route to a different bucket than default_table_path.
    await s3conn.execute("CREATE TABLE raw (id int) USING ducklake")  # default bucket
    await s3conn.execute("INSERT INTO raw VALUES (1), (2)")

    b2 = f"s3://{s3_second_bucket.bucket}/curated/"
    await s3conn.execute(
        f"CREATE TABLE curated (id int) USING ducklake "
        f"WITH (ducklake.table_path = '{b2}')"
    )
    await s3conn.execute("INSERT INTO curated SELECT id * 100 FROM raw")

    assert any(n.endswith(".parquet") for n in s3.object_names())
    assert any(
        n.startswith("curated/") and n.endswith(".parquet")
        for n in s3_second_bucket.object_names()
    )
    # cross-bucket join, one secret
    assert (
        await s3conn.fetchval(
            "SELECT count(*) FROM raw r JOIN curated c ON c.id = r.id * 100"
        )
        == 2
    )


async def test_inline_then_flush_to_s3(s3lake, s3):
    # Inlining is ON by default: INSERT lands in PG, not the bucket, until
    # flush_inlined_data writes parquet out to S3.
    conn = await s3lake.connect()  # configured; inlining left ON
    try:
        await conn.execute("CREATE TABLE inl (id int, name text) USING ducklake")
        await conn.execute("INSERT INTO inl VALUES (1, 'a'), (2, 'b')")
        assert not any(n.endswith(".parquet") for n in s3.object_names())

        await conn.fetchval("SELECT count(*) FROM ducklake.flush_inlined_data('inl'::regclass)")
        assert any(n.endswith(".parquet") for n in s3.object_names())

        rows = await conn.fetch("SELECT id, name FROM inl ORDER BY id")
        assert [tuple(r) for r in rows] == [(1, "a"), (2, "b")]
    finally:
        await conn.close()


async def test_merge_adjacent_files_on_s3(s3conn):
    # Small-file compaction: many single-row inserts produce many parquet files;
    # merge_adjacent_files compacts them without losing rows.
    await s3conn.execute("CREATE TABLE m (a int, b text) USING ducklake")
    for i in range(1, 6):
        await s3conn.execute(f"INSERT INTO m VALUES ({i}, 'v{i}')")
    n_before = await s3conn.fetchval("SELECT count(*) FROM ducklake.list_files('public', 'm')")
    assert n_before >= 5

    await s3conn.fetchval("SELECT count(*) FROM ducklake.merge_adjacent_files('m'::regclass)")
    n_after = await s3conn.fetchval("SELECT count(*) FROM ducklake.list_files('public', 'm')")
    assert n_after <= n_before
    assert await s3conn.fetchval("SELECT count(*) FROM m") == 5


async def test_wrong_creds_fail_at_s3_access(cluster, db, s3, s3_wrong_creds):
    # create_s3_secret validates shape only (on a throwaway connection); bad
    # credentials must surface at S3 access time, not at creation.
    lake = Lake(cluster, db, s3)
    conn = await lake.connect(configure=False)
    try:
        await _load_httpfs(conn)
        await conn.fetchval(
            "SELECT ducklake.create_s3_secret('s3', $1, $2, endpoint => $3, "
            "url_style => 'path', use_ssl => 'false')",
            s3_wrong_creds.access_key,
            s3_wrong_creds.secret_key,
            s3_wrong_creds.endpoint,
        )
        await conn.execute(f"SET ducklake.default_table_path = '{lake.table_path}'")
        await conn.execute("CALL ducklake.set_option('data_inlining_row_limit', 0)")
        await conn.execute("CREATE TABLE t (id int) USING ducklake")
        # the INSERT is the first real write to S3
        with pytest.raises(asyncpg.PostgresError):
            await conn.execute("INSERT INTO t VALUES (1)")
    finally:
        await conn.close()


async def test_duckdb_client_reads_s3_lake(s3lake, s3conn):
    await s3conn.execute("CREATE TABLE t (id int, name text) USING ducklake")
    await s3conn.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")

    # an external duckdb process needs its own S3 secret to read the data
    # files; the metadata comes over the postgres protocol
    ddb = s3lake.duckdb(read_only=True)
    try:
        rows = ddb.execute(
            "SELECT id, name FROM lake.public.t ORDER BY id"
        ).fetchall()
        assert rows == [(1, "alice"), (2, "bob")]
    finally:
        ddb.close()
