# RFC 001 validation against the statically linked, pinned DuckLake writer.

import asyncio
import uuid

import asyncpg
import pytest

from conftest import Lake


@pytest.fixture
def local_lake(cluster, db):
    return Lake(cluster, db)


@pytest.fixture
async def pg(local_lake):
    connection = await local_lake.connect()
    try:
        yield connection
    finally:
        await connection.close()


async def wait_for_backend_event(conn, pid, task, event_type, timeout=10):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if task.done():
            await task
            raise AssertionError(
                f"statement completed before waiting on {event_type}"
            )
        if await conn.fetchval(
            "SELECT wait_event_type = $2 FROM pg_stat_activity WHERE pid = $1",
            pid,
            event_type,
        ):
            return
        await asyncio.sleep(0.01)
    raise TimeoutError(f"backend {pid} did not wait on {event_type}")


async def wait_for_backend_lock(conn, pid, task, timeout=10):
    await wait_for_backend_event(conn, pid, task, "Lock", timeout)


async def run_pinned_writer(conn, query):
    # raw_query runs through the DuckLake extension built from the pinned
    # third_party/ducklake source, rather than the native protocol writer.
    await conn.execute("SELECT ducklake.raw_query($1)", query)


async def snapshot_head(conn, table):
    head = dict(
        await conn.fetchrow(
            "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id "
            "FROM ducklake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1"
        )
    )
    head["next_row_id"] = await conn.fetchval(
        "SELECT COALESCE(ts.next_row_id, 0) "
        "FROM ducklake.ducklake_table t "
        "LEFT JOIN ducklake.ducklake_table_stats ts USING (table_id) "
        "WHERE t.table_name = $1 AND t.end_snapshot IS NULL",
        table,
    )
    return head


async def normalized_write_snapshot(conn, table, baseline):
    table_id = await conn.fetchval(
        "SELECT table_id FROM ducklake.ducklake_table "
        "WHERE table_name = $1 AND end_snapshot IS NULL",
        table,
    )
    snapshots = await conn.fetch(
        "SELECT snapshot_id, snapshot_time IS NOT NULL AS has_time, "
        "schema_version, next_catalog_id, next_file_id "
        "FROM ducklake.ducklake_snapshot WHERE snapshot_id > $1 "
        "ORDER BY snapshot_id",
        baseline["snapshot_id"],
    )
    normalized_snapshots = [
        (
            row["snapshot_id"] - baseline["snapshot_id"],
            row["has_time"],
            row["schema_version"] - baseline["schema_version"],
            row["next_catalog_id"] - baseline["next_catalog_id"],
            row["next_file_id"] - baseline["next_file_id"],
        )
        for row in snapshots
    ]

    changes = await conn.fetch(
        "SELECT snapshot_id, changes_made, author, commit_message, "
        "commit_extra_info FROM ducklake.ducklake_snapshot_changes "
        "WHERE snapshot_id > $1 ORDER BY snapshot_id",
        baseline["snapshot_id"],
    )
    normalized_changes = [
        (
            row["snapshot_id"] - baseline["snapshot_id"],
            (
                "inlined_insert:<table>"
                if row["changes_made"] == f"inlined_insert:{table_id}"
                else row["changes_made"]
            ),
            row["author"],
            row["commit_message"],
            row["commit_extra_info"],
        )
        for row in changes
    ]

    table_stats = tuple(
        await conn.fetchrow(
            "SELECT record_count, next_row_id, file_size_bytes "
            "FROM ducklake.ducklake_table_stats WHERE table_id = $1",
            table_id,
        )
    )
    column_stats = [
        tuple(row)
        for row in await conn.fetch(
            "SELECT c.column_name, s.contains_null, s.contains_nan, "
            "s.min_value, s.max_value, s.extra_stats "
            "FROM ducklake.ducklake_table_column_stats s "
            "JOIN ducklake.ducklake_column c USING (table_id, column_id) "
            "WHERE s.table_id = $1 AND c.end_snapshot IS NULL "
            "ORDER BY c.column_id",
            table_id,
        )
    ]
    inline_table = await conn.fetchval(
        "SELECT it.table_name FROM ducklake.ducklake_inlined_data_tables it "
        "WHERE it.table_id = $1 ORDER BY it.schema_version DESC LIMIT 1",
        table_id,
    )
    physical_fields = [
        (
            row["row_id"] - baseline["next_row_id"],
            row["begin_snapshot"] - baseline["snapshot_id"],
            (
                None
                if row["end_snapshot"] is None
                else row["end_snapshot"] - baseline["snapshot_id"]
            ),
        )
        for row in await conn.fetch(
            f"SELECT row_id, begin_snapshot, end_snapshot "
            f"FROM ducklake.{inline_table} WHERE begin_snapshot > $1 ORDER BY row_id",
            baseline["snapshot_id"],
        )
    ]
    visible_data = [
        tuple(row)
        for row in await conn.fetch(f"SELECT id, label, score FROM {table} ORDER BY id")
    ]
    return {
        "snapshots": normalized_snapshots,
        "table_stats": table_stats,
        "column_stats": column_stats,
        "changes": normalized_changes,
        "physical_fields": physical_fields,
        "visible_data": visible_data,
    }


async def prepare_conformance_tables(conn):
    await conn.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await conn.execute(
        "CREATE TABLE conformance_reference "
        "(id int, label text, score double precision) USING ducklake; "
        "CREATE TABLE conformance_native "
        "(id int, label text, score double precision) USING ducklake"
    )
    for table in ("conformance_reference", "conformance_native"):
        await conn.fetchval(
            "SELECT count(*) FROM "
            f"ducklake.ensure_inlined_data_table('{table}'::regclass)"
        )
        await run_pinned_writer(
            conn,
            f"INSERT INTO pgducklake.public.{table} VALUES (0, 'seed', 0.0)",
        )


async def make_server_copy_file(conn):
    data_directory, database_oid = await conn.fetchrow(
        "SELECT current_setting('data_directory'), oid "
        "FROM pg_database WHERE datname = current_database()"
    )
    # Put the fixture in the server's database directory. This works when the
    # test runner and PostgreSQL have different filesystems (Docker/external
    # mode), and DROP DATABASE removes the file with the directory.
    path = (
        f"{data_directory}/base/{database_oid}/"
        f"native_writer_{uuid.uuid4().hex}.csv"
    )
    escaped_path = path.replace("'", "''")
    await conn.execute(
        "COPY (VALUES (1, 'alpha', 1.5::double precision), "
        "(2, NULL, -2.25::double precision), "
        "(3, 'omega', NULL::double precision)) "
        f"TO '{escaped_path}' (FORMAT CSV, HEADER true)"
    )
    return path


async def write_reference_batch(conn, operation, copy_path=None):
    if operation == "values":
        await run_pinned_writer(
            conn,
            "INSERT INTO pgducklake.public.conformance_reference VALUES "
            "(1, 'alpha', 1.5), (2, NULL, -2.25), (3, 'omega', NULL)",
        )
    elif operation == "prepared_unnest":
        await run_pinned_writer(
            conn,
            "PREPARE conformance_reference_insert AS "
            "INSERT INTO pgducklake.public.conformance_reference "
            "SELECT UNNEST($1::INTEGER[]), UNNEST($2::VARCHAR[]), "
            "UNNEST($3::DOUBLE[]); "
            "EXECUTE conformance_reference_insert("
            "[1, 2, 3], ['alpha', NULL, 'omega'], [1.5, -2.25, NULL]); "
            "DEALLOCATE conformance_reference_insert",
        )
    else:
        assert copy_path is not None
        escaped_path = copy_path.replace("'", "''")
        await run_pinned_writer(
            conn,
            "COPY pgducklake.public.conformance_reference "
            f"FROM '{escaped_path}' "
            "(FORMAT CSV, HEADER true, NULL '')",
        )


async def write_native_batch(conn, operation):
    if operation == "values":
        await conn.execute(
            "INSERT INTO conformance_native VALUES "
            "(1, 'alpha', 1.5), (2, NULL, -2.25), (3, 'omega', NULL)"
        )
    elif operation == "prepared_unnest":
        statement = await conn.prepare(
            "INSERT INTO conformance_native "
            "SELECT UNNEST($1::int[]), UNNEST($2::text[]), "
            "UNNEST($3::double precision[])"
        )
        await statement.fetch([1, 2, 3], ["alpha", None, "omega"], [1.5, -2.25, None])
    else:
        await conn.copy_records_to_table(
            "conformance_native",
            records=[(1, "alpha", 1.5), (2, None, -2.25), (3, "omega", None)],
        )


@pytest.mark.parametrize("operation", ["values", "prepared_unnest", "copy"])
async def test_native_writer_matches_pinned_writer(local_lake, pg, operation):
    await prepare_conformance_tables(pg)
    copy_path = await make_server_copy_file(pg) if operation == "copy" else None

    reference_baseline = await snapshot_head(pg, "conformance_reference")
    await write_reference_batch(pg, operation, copy_path)
    reference = await normalized_write_snapshot(
        pg, "conformance_reference", reference_baseline
    )

    native_baseline = await snapshot_head(pg, "conformance_native")
    await write_native_batch(pg, operation)
    native = await normalized_write_snapshot(pg, "conformance_native", native_baseline)

    assert reference == native
    expected = [
        (0, "seed", 0.0),
        (1, "alpha", 1.5),
        (2, None, -2.25),
        (3, "omega", None),
    ]
    assert native["visible_data"] == expected

    fresh = local_lake.duckdb(read_only=True)
    try:
        for table in ("conformance_reference", "conformance_native"):
            assert (
                fresh.execute(
                    f"SELECT id, label, score FROM lake.public.{table} ORDER BY id"
                ).fetchall()
                == expected
            )
    finally:
        fresh.close()


async def test_prepared_unnest_generic_plans_bind_at_execution(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await pg.execute(
        "CREATE TABLE generic_unnest (id int, label text) USING ducklake"
    )
    await pg.fetchval(
        "SELECT count(*) FROM "
        "ducklake.ensure_inlined_data_table('generic_unnest'::regclass)"
    )

    await pg.execute("SET plan_cache_mode = force_generic_plan")
    await pg.execute(
        "PREPARE force_generic_unnest (int[], text[]) AS "
        "INSERT INTO generic_unnest SELECT unnest($1), unnest($2)"
    )
    before_empty = await pg.fetchval(
        "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
    )
    await pg.execute(
        "EXECUTE force_generic_unnest(ARRAY[]::int[], ARRAY[]::text[])"
    )
    await pg.execute("EXECUTE force_generic_unnest(NULL::int[], NULL::text[])")
    assert (
        await pg.fetchval("SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot")
        == before_empty
    )
    await pg.execute("EXECUTE force_generic_unnest(ARRAY[1, 2, 3], ARRAY['a'])")
    await pg.execute(
        "EXECUTE force_generic_unnest(ARRAY[]::int[], ARRAY['x', 'y'])"
    )
    await pg.execute("EXECUTE force_generic_unnest(NULL::int[], ARRAY['z'])")
    assert tuple(
        await pg.fetchrow(
            "SELECT generic_plans, custom_plans FROM pg_prepared_statements "
            "WHERE name = 'force_generic_unnest'"
        )
    ) == (5, 0)
    await pg.execute("DEALLOCATE force_generic_unnest")

    await pg.execute("SET plan_cache_mode = auto")
    await pg.execute(
        "PREPARE auto_generic_unnest (int[], text[]) AS "
        "INSERT INTO generic_unnest SELECT unnest($1), unnest($2)"
    )
    executions = [
        "(ARRAY[10], ARRAY['ten'])",
        "(ARRAY[20, 21], ARRAY['twenty'])",
        "(ARRAY[]::int[], ARRAY['thirty'])",
        "(NULL::int[], ARRAY['forty', 'forty-one'])",
        "(ARRAY[50], NULL::text[])",
        "(ARRAY[60, 61, 62], ARRAY['sixty', 'sixty-one'])",
    ]
    for arguments in executions:
        await pg.execute(f"EXECUTE auto_generic_unnest{arguments}")
    before_auto_empty = await pg.fetchval(
        "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
    )
    await pg.execute(
        "EXECUTE auto_generic_unnest(ARRAY[]::int[], ARRAY[]::text[])"
    )
    assert (
        await pg.fetchval("SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot")
        == before_auto_empty
    )
    generic_plans, custom_plans = await pg.fetchrow(
        "SELECT generic_plans, custom_plans FROM pg_prepared_statements "
        "WHERE name = 'auto_generic_unnest'"
    )
    assert generic_plans >= 1
    assert custom_plans == 5
    await pg.execute("DEALLOCATE auto_generic_unnest")
    await pg.execute("RESET plan_cache_mode")

    expected = [
        (1, "a"),
        (2, None),
        (3, None),
        (10, "ten"),
        (20, "twenty"),
        (21, None),
        (50, None),
        (60, "sixty"),
        (61, "sixty-one"),
        (62, None),
        (None, "forty"),
        (None, "forty-one"),
        (None, "thirty"),
        (None, "x"),
        (None, "y"),
        (None, "z"),
    ]
    rows = await pg.fetch(
        "SELECT id, label FROM generic_unnest ORDER BY id NULLS LAST, label"
    )
    assert [tuple(row) for row in rows] == expected

    fresh = local_lake.duckdb(read_only=True)
    try:
        assert fresh.execute(
            "SELECT id, label FROM lake.public.generic_unnest "
            "ORDER BY id NULLS LAST, label"
        ).fetchall() == expected
    finally:
        fresh.close()


async def test_native_writer_initializes_first_write_stats(local_lake, pg):
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    for table in ("first_write_reference", "first_write_native"):
        await pg.execute(
            f"CREATE TABLE {table} "
            "(id int, label text, score double precision) USING ducklake"
        )
        await pg.fetchval(
            "SELECT count(*) FROM "
            f"ducklake.ensure_inlined_data_table('{table}'::regclass)"
        )
        assert await pg.fetchval(
            "SELECT count(*) FROM ducklake.ducklake_table_stats s "
            "JOIN ducklake.ducklake_table t USING (table_id) "
            "WHERE t.table_name = $1 AND t.end_snapshot IS NULL",
            table,
        ) == 0
        assert await pg.fetchval(
            "SELECT count(*) FROM ducklake.ducklake_table_column_stats s "
            "JOIN ducklake.ducklake_table t USING (table_id) "
            "WHERE t.table_name = $1 AND t.end_snapshot IS NULL",
            table,
        ) == 0

    reference_baseline = await snapshot_head(pg, "first_write_reference")
    await run_pinned_writer(
        pg,
        "INSERT INTO pgducklake.public.first_write_reference VALUES "
        "(1, 'alpha', 1.5), (2, NULL, -2.25), (3, 'omega', NULL)",
    )
    reference = await normalized_write_snapshot(
        pg, "first_write_reference", reference_baseline
    )

    native_baseline = await snapshot_head(pg, "first_write_native")
    await pg.execute(
        "INSERT INTO first_write_native VALUES "
        "(1, 'alpha', 1.5), (2, NULL, -2.25), (3, 'omega', NULL)"
    )
    native = await normalized_write_snapshot(
        pg, "first_write_native", native_baseline
    )

    assert native == reference
    assert native["table_stats"] == (3, 3, 0)
    assert native["physical_fields"] == [
        (0, 1, None),
        (1, 1, None),
        (2, 1, None),
    ]
    assert [row[0] for row in native["column_stats"]] == ["id", "label", "score"]
    assert all(row[3] is not None and row[4] is not None for row in native["column_stats"])
    fresh = local_lake.duckdb(read_only=True)
    try:
        assert fresh.execute(
            "SELECT id, label, score FROM lake.public.first_write_native ORDER BY id"
        ).fetchall() == [
            (1, "alpha", 1.5),
            (2, None, -2.25),
            (3, "omega", None),
        ]
    finally:
        fresh.close()


async def install_reference_claim_witness(conn, table, application_name):
    table_row = await conn.fetchrow(
        "SELECT t.table_id, it.table_name AS inline_table "
        "FROM ducklake.ducklake_table t "
        "JOIN ducklake.ducklake_inlined_data_tables it USING (table_id) "
        "WHERE t.table_name = $1 AND t.end_snapshot IS NULL "
        "ORDER BY it.schema_version DESC LIMIT 1",
        table,
    )
    table_id = table_row["table_id"]
    inline_table = table_row["inline_table"]
    await conn.execute(
        f"""
        CREATE FUNCTION public.reference_claim_protocol_state() RETURNS jsonb
        LANGUAGE sql AS $state$
          SELECT jsonb_build_object(
            'snapshots', (SELECT jsonb_agg(to_jsonb(x) ORDER BY snapshot_id)
              FROM (SELECT snapshot_id, snapshot_time, schema_version,
                           next_catalog_id, next_file_id
                    FROM ducklake.ducklake_snapshot) AS x),
            'changes', (SELECT jsonb_agg(to_jsonb(x) ORDER BY snapshot_id)
              FROM (SELECT snapshot_id, changes_made, author, commit_message,
                           commit_extra_info
                    FROM ducklake.ducklake_snapshot_changes) AS x),
            'table_stats', (SELECT COALESCE(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
              FROM (SELECT record_count, next_row_id, file_size_bytes
                    FROM ducklake.ducklake_table_stats
                    WHERE table_id = {table_id}) AS x),
            'column_stats', (SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY column_id), '[]'::jsonb)
              FROM (SELECT column_id, contains_null, contains_nan, min_value,
                           max_value, extra_stats
                    FROM ducklake.ducklake_table_column_stats
                    WHERE table_id = {table_id}) AS x),
            'physical_data', (SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY row_id), '[]'::jsonb)
              FROM (SELECT row_id, begin_snapshot, end_snapshot, id, value
                    FROM ducklake.{inline_table}) AS x)
          )
        $state$;
        CREATE TABLE public.reference_claim_witness (
          baseline jsonb NOT NULL,
          observed jsonb,
          checked boolean NOT NULL DEFAULT false
        );
        INSERT INTO public.reference_claim_witness (baseline)
        SELECT public.reference_claim_protocol_state();
        CREATE FUNCTION public.assert_reference_claim_first() RETURNS trigger
        LANGUAGE plpgsql AS $trigger$
        DECLARE current_state jsonb;
        BEGIN
          IF current_setting('application_name') = '{application_name}' THEN
            current_state := public.reference_claim_protocol_state();
            UPDATE public.reference_claim_witness
            SET observed = current_state, checked = true;
            IF current_state IS DISTINCT FROM
               (SELECT baseline FROM public.reference_claim_witness) THEN
              RAISE EXCEPTION 'reference writer mutated protocol before snapshot claim';
            END IF;
          END IF;
          RETURN NEW;
        END
        $trigger$;
        CREATE TRIGGER assert_reference_claim_first
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.assert_reference_claim_first();
        """
    )


async def test_pinned_writer_snapshot_claim_is_first_protocol_mutation(local_lake, pg):
    table = "reference_claim_witness_t"
    application_name = "pinned_reference_claim_witness"
    await prepare_validation_table(pg, table)
    await install_reference_claim_witness(pg, table, application_name)

    reference = await local_lake.connect(
        server_settings={"application_name": application_name}
    )
    try:
        await run_pinned_writer(
            reference,
            "INSERT INTO pgducklake.public.reference_claim_witness_t "
            "VALUES (1, 10), (2, 20)",
        )
    finally:
        await reference.close()

    witness = await pg.fetchrow(
        "SELECT checked, baseline = observed AS unchanged "
        "FROM public.reference_claim_witness"
    )
    assert tuple(witness) == (True, True)
    await assert_fresh_rows(
        local_lake, table, [(0, 0), (1, 10), (2, 20)]
    )


async def install_claim_blocker(conn, application_name, advisory_key):
    await conn.execute(
        f"""
        CREATE FUNCTION public.block_validation_snapshot_claim() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('application_name') = '{application_name}' THEN
            PERFORM pg_advisory_xact_lock({advisory_key});
          END IF;
          RETURN NEW;
        END
        $$;
        CREATE TRIGGER block_validation_snapshot_claim
        BEFORE INSERT ON ducklake.ducklake_snapshot
        FOR EACH ROW EXECUTE FUNCTION public.block_validation_snapshot_claim();
        """
    )


async def prepare_validation_table(conn, table="writer_validation"):
    await conn.execute("CALL ducklake.set_option('data_inlining_row_limit', 100)")
    await conn.execute(f"CREATE TABLE {table} (id int, value int) USING ducklake")
    await conn.fetchval(
        f"SELECT count(*) FROM ducklake.ensure_inlined_data_table('{table}'::regclass)"
    )
    await run_pinned_writer(
        conn, f"INSERT INTO pgducklake.public.{table} VALUES (0, 0)"
    )


async def inline_table_name(conn, table):
    return await conn.fetchval(
        "SELECT it.table_name FROM ducklake.ducklake_inlined_data_tables it "
        "JOIN ducklake.ducklake_table t USING (table_id) "
        "WHERE t.table_name = $1 AND t.end_snapshot IS NULL "
        "ORDER BY it.schema_version DESC LIMIT 1",
        table,
    )


async def force_values_retag(local_lake, pg, table, application_name, advisory_key):
    await install_claim_blocker(pg, application_name, advisory_key)
    blocker = await local_lake.connect()
    winner = await local_lake.connect(
        server_settings={"ducklake.native_writer_reservation_queue": "off"}
    )
    loser = await local_lake.connect(
        server_settings={
            "application_name": application_name,
            "ducklake.native_writer_reservation_queue": "off",
        }
    )
    loser_task = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        loser_task = asyncio.create_task(
            loser.execute(f"INSERT INTO {table} VALUES (10, 10), (11, 11)")
        )
        await wait_for_backend_lock(pg, loser_pid, loser_task)
        await winner.execute(f"INSERT INTO {table} VALUES (20, 20)")
        assert await blocker.fetchval("SELECT pg_advisory_unlock($1)", advisory_key)
        await loser_task
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        if loser_task is not None and not loser_task.done():
            loser_task.cancel()
            await asyncio.gather(loser_task, return_exceptions=True)
        await blocker.close()
        await winner.close()
        await loser.close()


async def exact_protocol_state(conn, table="writer_validation"):
    table_row = await conn.fetchrow(
        "SELECT t.table_id, it.table_name AS inline_table "
        "FROM ducklake.ducklake_table t "
        "JOIN ducklake.ducklake_inlined_data_tables it USING (table_id) "
        "WHERE t.table_name = $1 AND t.end_snapshot IS NULL "
        "ORDER BY it.schema_version DESC LIMIT 1",
        table,
    )
    table_id = table_row["table_id"]
    inline_table = table_row["inline_table"]

    async def rows(query, *args):
        return [tuple(row) for row in await conn.fetch(query, *args)]

    return {
        "snapshots": await rows(
            "SELECT snapshot_id, snapshot_time, schema_version, next_catalog_id, "
            "next_file_id FROM ducklake.ducklake_snapshot ORDER BY snapshot_id"
        ),
        "changes": await rows(
            "SELECT snapshot_id, changes_made, author, commit_message, "
            "commit_extra_info FROM ducklake.ducklake_snapshot_changes "
            "ORDER BY snapshot_id"
        ),
        "table_stats": await rows(
            "SELECT record_count, next_row_id, file_size_bytes "
            "FROM ducklake.ducklake_table_stats WHERE table_id = $1",
            table_id,
        ),
        "column_stats": await rows(
            "SELECT column_id, contains_null, contains_nan, min_value, max_value, "
            "extra_stats FROM ducklake.ducklake_table_column_stats "
            "WHERE table_id = $1 ORDER BY column_id",
            table_id,
        ),
        "physical_data": await rows(
            f"SELECT row_id, begin_snapshot, end_snapshot, id, value "
            f"FROM ducklake.{inline_table} ORDER BY row_id"
        ),
        "visible_data": await rows(f"SELECT id, value FROM {table} ORDER BY id"),
    }


async def assert_fresh_rows(local_lake, table, expected):
    fresh = local_lake.duckdb(read_only=True)
    try:
        assert (
            fresh.execute(
                f"SELECT id, value FROM lake.public.{table} ORDER BY id"
            ).fetchall()
            == expected
        )
    finally:
        fresh.close()


@pytest.mark.parametrize(
    "fault",
    [
        "after_prewrite",
        "after_claim",
        "after_table_stats",
        "after_column_stats",
        "after_change_record",
        "after_publication",
    ],
)
async def test_native_writer_fault_rolls_back_exact_state(local_lake, pg, fault):
    await prepare_validation_table(pg)
    await pg.execute("SET ducklake.native_writer_reservation_queue = on")
    baseline = await exact_protocol_state(pg)

    await pg.execute(f"SET ducklake.test_native_writer_fault = '{fault}'")
    with pytest.raises(
        asyncpg.InternalServerError, match=f"test fault {fault.replace('_', ' ')}"
    ):
        await pg.execute("INSERT INTO writer_validation VALUES (1, -10), (2, 20)")

    # Boundary failures either unwind the publication subtransaction or, after
    # its release, force the statement transaction to remove the completed
    # publication and prewritten parent tuples.
    assert await exact_protocol_state(pg) == baseline

    await pg.execute("RESET ducklake.test_native_writer_fault")
    await pg.execute("INSERT INTO writer_validation VALUES (90, 90)")
    assert await pg.fetchval(
        "SELECT array_agg(id ORDER BY id) FROM writer_validation"
    ) == [0, 90]
    await assert_fresh_rows(local_lake, "writer_validation", [(0, 0), (90, 90)])


async def test_pinned_writer_claims_before_native_rebase(local_lake, pg):
    table = "reference_claim_first"
    await prepare_validation_table(pg, table)
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")
    initial = await pg.fetchrow(
        "SELECT s.snapshot_id, ts.next_row_id "
        "FROM ducklake.ducklake_snapshot s "
        "CROSS JOIN ducklake.ducklake_table t "
        "JOIN ducklake.ducklake_table_stats ts USING (table_id) "
        "WHERE t.table_name = $1 AND t.end_snapshot IS NULL "
        "ORDER BY s.snapshot_id DESC LIMIT 1",
        table,
    )

    advisory_key = 724_913_570
    application_name = "pinned_reference_claim_loser"
    await install_claim_blocker(pg, application_name, advisory_key)
    blocker = await local_lake.connect()
    winner = await local_lake.connect()
    loser = await local_lake.connect(
        server_settings={"application_name": application_name}
    )
    loser_task = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        loser_task = asyncio.create_task(
            loser.execute("INSERT INTO reference_claim_first VALUES (10, 10), (11, 11)")
        )
        await wait_for_backend_lock(pg, loser_pid, loser_task)

        await run_pinned_writer(
            winner,
            "INSERT INTO pgducklake.public.reference_claim_first VALUES (20, 20)",
        )
        assert (
            await pg.fetchval("SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot")
            == initial["snapshot_id"] + 1
        )
        reference_change = await pg.fetchval(
            "SELECT changes_made FROM ducklake.ducklake_snapshot_changes "
            "WHERE snapshot_id = $1",
            initial["snapshot_id"] + 1,
        )
        assert reference_change.startswith("inlined_insert:")

        assert await blocker.fetchval("SELECT pg_advisory_unlock($1)", advisory_key)
        await loser_task
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        if loser_task is not None and not loser_task.done():
            loser_task.cancel()
            await asyncio.gather(loser_task, return_exceptions=True)
        await blocker.close()
        await winner.close()
        await loser.close()

    assert (
        await pg.fetchval("SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot")
        == initial["snapshot_id"] + 2
    )
    assert await pg.fetchval(
        "SELECT array_agg(id ORDER BY id) FROM reference_claim_first"
    ) == [0, 10, 11, 20]
    stats = dict(
        await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
    )
    assert stats["snapshot_claim_conflicts"] == 1
    assert stats["rows_retagged"] == 2
    await assert_fresh_rows(local_lake, table, [(0, 0), (10, 10), (11, 11), (20, 20)])


async def test_custom_default_table_am_uses_ownership_update(local_lake, pg):
    await pg.execute(
        "CREATE ACCESS METHOD native_test_heap TYPE TABLE "
        "HANDLER heap_tableam_handler"
    )
    await pg.execute("SET default_table_access_method = native_test_heap")
    try:
        await prepare_validation_table(pg, "custom_am_retag")
    finally:
        await pg.execute("RESET default_table_access_method")

    inline_table = await inline_table_name(pg, "custom_am_retag")
    assert await pg.fetchval(
        "SELECT am.amname FROM pg_class c JOIN pg_am am ON am.oid = c.relam "
        "WHERE c.oid = $1::regclass",
        f"ducklake.{inline_table}",
    ) == "native_test_heap"

    await force_values_retag(
        local_lake, pg, "custom_am_retag", "custom_am_retag_loser", 724_913_573
    )
    assert await pg.fetchval(
        "SELECT array_agg(id ORDER BY id) FROM custom_am_retag"
    ) == [0, 10, 11, 20]


@pytest.mark.parametrize("unsafe_state", ["index", "trigger", "update_rule"])
async def test_unsafe_inline_state_uses_ownership_update(
    local_lake, pg, unsafe_state
):
    table = f"unsafe_retag_{unsafe_state}"
    await prepare_validation_table(pg, table)
    inline_table = await inline_table_name(pg, table)
    await pg.execute("CREATE TABLE retag_audit (id int)")

    if unsafe_state == "index":
        await pg.execute(
            f"CREATE INDEX retag_protocol_index ON ducklake.{inline_table} "
            "(row_id, begin_snapshot)"
        )
    elif unsafe_state == "trigger":
        await pg.execute(
            f"""
            CREATE FUNCTION audit_retag_trigger() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
              INSERT INTO retag_audit VALUES (NEW.id);
              RETURN NEW;
            END
            $$;
            CREATE TRIGGER audit_retag BEFORE UPDATE ON ducklake.{inline_table}
            FOR EACH ROW EXECUTE FUNCTION audit_retag_trigger();
            """
        )
    else:
        await pg.execute(
            f"CREATE RULE audit_retag AS ON UPDATE TO ducklake.{inline_table} "
            "DO ALSO INSERT INTO retag_audit VALUES (NEW.id)"
        )

    await force_values_retag(
        local_lake,
        pg,
        table,
        f"unsafe_retag_{unsafe_state}_loser",
        {"index": 724_913_574, "trigger": 724_913_575, "update_rule": 724_913_576}[
            unsafe_state
        ],
    )

    if unsafe_state == "index":
        latest_snapshot = await pg.fetchval(
            "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
        )
        await pg.execute("SET enable_seqscan = off")
        try:
            plan = "\n".join(
                row[0]
                for row in await pg.fetch(
                    f"EXPLAIN SELECT id FROM ducklake.{inline_table} "
                    "WHERE row_id IN (2, 3) AND begin_snapshot = $1",
                    latest_snapshot,
                )
            )
            assert "retag_protocol_index" in plan
            assert await pg.fetchval(
                f"SELECT array_agg(id ORDER BY id) FROM ducklake.{inline_table} "
                "WHERE row_id IN (2, 3) AND begin_snapshot = $1",
                latest_snapshot,
            ) == [10, 11]
        finally:
            await pg.execute("RESET enable_seqscan")
    else:
        assert await pg.fetchval(
            "SELECT array_agg(id ORDER BY id) FROM retag_audit"
        ) == [10, 11]


async def test_pointer_tracking_cap_falls_back_for_large_copy(local_lake, pg):
    table = "tracking_cap_copy"
    await pg.execute("CALL ducklake.set_option('data_inlining_row_limit', 100000)")
    await pg.execute(f"CREATE TABLE {table} (id int) USING ducklake")
    await pg.fetchval(
        f"SELECT count(*) FROM ducklake.ensure_inlined_data_table('{table}'::regclass)"
    )
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    application_name = "tracking_cap_copy_loser"
    advisory_key = 724_913_577
    await install_claim_blocker(pg, application_name, advisory_key)
    blocker = await local_lake.connect()
    winner = await local_lake.connect(
        server_settings={"ducklake.native_writer_reservation_queue": "off"}
    )
    loser = await local_lake.connect(
        server_settings={
            "application_name": application_name,
            "ducklake.native_writer_reservation_queue": "off",
        }
    )
    copy_task = None
    row_count = 66_000
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        copy_task = asyncio.create_task(
            loser.copy_records_to_table(
                table, records=((value,) for value in range(row_count))
            )
        )
        await wait_for_backend_lock(pg, loser_pid, copy_task, timeout=30)
        await winner.execute(f"INSERT INTO {table} VALUES ({row_count})")
        assert await blocker.fetchval("SELECT pg_advisory_unlock($1)", advisory_key)
        await asyncio.wait_for(copy_task, timeout=30)
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        if copy_task is not None and not copy_task.done():
            copy_task.cancel()
            await asyncio.gather(copy_task, return_exceptions=True)
        await blocker.close()
        await winner.close()
        await loser.close()

    assert await pg.fetchval(f"SELECT count(*) FROM {table}") == row_count + 1
    assert await pg.fetchval(
        "SELECT count FROM ducklake.native_writer_stats() "
        "WHERE event = 'rows_retagged'"
    ) == row_count


async def test_inline_relation_rewrite_waits_through_retag(local_lake, pg):
    table = "rewrite_locked_retag"
    application_name = "rewrite_locked_retag_loser"
    advisory_key = 724_913_578
    await prepare_validation_table(pg, table)
    inline_table = await inline_table_name(pg, table)
    await install_claim_blocker(pg, application_name, advisory_key)

    blocker = await local_lake.connect()
    winner = await local_lake.connect(
        server_settings={"ducklake.native_writer_reservation_queue": "off"}
    )
    loser = await local_lake.connect(
        server_settings={
            "application_name": application_name,
            "ducklake.native_writer_reservation_queue": "off",
        }
    )
    rewriter = await local_lake.connect()
    loser_task = rewrite_task = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        loser_task = asyncio.create_task(
            loser.execute(f"INSERT INTO {table} VALUES (10, 10), (11, 11)")
        )
        await wait_for_backend_lock(pg, loser_pid, loser_task)
        await winner.execute(f"INSERT INTO {table} VALUES (20, 20)")

        rewrite_pid = await rewriter.fetchval("SELECT pg_backend_pid()")
        rewrite_task = asyncio.create_task(
            rewriter.execute(f"VACUUM FULL ducklake.{inline_table}")
        )
        await wait_for_backend_lock(pg, rewrite_pid, rewrite_task)
        assert not loser_task.done()
        assert not rewrite_task.done()

        assert await blocker.fetchval("SELECT pg_advisory_unlock($1)", advisory_key)
        await loser_task
        await asyncio.wait_for(rewrite_task, timeout=10)
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        for task in (loser_task, rewrite_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await blocker.close()
        await winner.close()
        await loser.close()
        await rewriter.close()

    assert await pg.fetchval(f"SELECT array_agg(id ORDER BY id) FROM {table}") == [
        0,
        10,
        11,
        20,
    ]


async def test_completed_publication_outer_rollback_recovers_queue(local_lake, pg):
    await prepare_validation_table(pg)
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")
    baseline = await exact_protocol_state(pg)

    advisory_key = 724_913_572
    application_name = "fault_after_publication_first"
    await install_claim_blocker(pg, application_name, advisory_key)
    blocker = await local_lake.connect()
    first = await local_lake.connect(
        server_settings={
            "application_name": application_name,
            "ducklake.native_writer_reservation_queue": "on",
            "ducklake.test_native_writer_fault": "after_publication",
        }
    )
    successor = await local_lake.connect(
        server_settings={"ducklake.native_writer_reservation_queue": "on"}
    )
    first_task = None
    successor_task = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        first_pid = await first.fetchval("SELECT pg_backend_pid()")
        first_task = asyncio.create_task(
            first.execute("INSERT INTO writer_validation VALUES (10, 10), (11, 11)")
        )
        await wait_for_backend_lock(pg, first_pid, first_task)

        successor_pid = await successor.fetchval("SELECT pg_backend_pid()")
        successor_task = asyncio.create_task(
            successor.execute("INSERT INTO writer_validation VALUES (20, 20), (21, 21)")
        )
        await wait_for_backend_event(
            pg, successor_pid, successor_task, "Extension"
        )

        assert await blocker.fetchval("SELECT pg_advisory_unlock($1)", advisory_key)
        with pytest.raises(
            asyncpg.InternalServerError, match="test fault after publication"
        ):
            await first_task
        await successor_task
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        for task in (first_task, successor_task):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await blocker.close()
        await first.close()
        await successor.close()

    assert await pg.fetchval(
        "SELECT array_agg(id ORDER BY id) FROM writer_validation"
    ) == [0, 20, 21]
    state = await exact_protocol_state(pg)
    assert state["physical_data"] == [
        baseline["physical_data"][0],
        (1, baseline["snapshots"][-1][0] + 1, None, 20, 20),
        (2, baseline["snapshots"][-1][0] + 1, None, 21, 21),
    ]

    # A fresh reservation after the invalidated successor chain must also
    # publish, proving the queue did not retain a completed-but-aborted head.
    await pg.execute("SET ducklake.native_writer_reservation_queue = on")
    await pg.execute("INSERT INTO writer_validation VALUES (90, 90)")
    await assert_fresh_rows(
        local_lake, "writer_validation", [(0, 0), (20, 20), (21, 21), (90, 90)]
    )


async def test_fault_guc_grant_set_access(local_lake, pg):
    server_version = int(await pg.fetchval("SHOW server_version_num"))
    if server_version < 150000:
        pytest.skip("GRANT SET ON PARAMETER requires PostgreSQL 15")

    assert "ducklake.test_native_writer_fault" not in {
        row["name"] for row in await pg.fetch("SHOW ALL")
    }
    await pg.execute("CREATE ROLE native_fault_setter")
    delegated = await local_lake.connect()
    try:
        await delegated.execute("SET ROLE native_fault_setter")
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await delegated.execute(
                "SET ducklake.test_native_writer_fault = 'after_prewrite'"
            )
        await delegated.execute("RESET ROLE")
        await pg.execute(
            "GRANT SET ON PARAMETER ducklake.test_native_writer_fault "
            "TO native_fault_setter"
        )
        await delegated.execute("SET ROLE native_fault_setter")
        await delegated.execute(
            "SET ducklake.test_native_writer_fault = 'after_prewrite'"
        )
        assert await delegated.fetchval(
            "SELECT current_setting('ducklake.test_native_writer_fault')"
        ) == "after_prewrite"
    finally:
        await delegated.close()


async def test_native_writer_fault_after_retag_rolls_back_exact_state(local_lake, pg):
    await prepare_validation_table(pg)
    await pg.execute("SELECT ducklake.reset_native_writer_stats()")

    advisory_key = 724_913_571
    application_name = "fault_after_retag_loser"
    await install_claim_blocker(pg, application_name, advisory_key)
    blocker = await local_lake.connect()
    winner = await local_lake.connect()
    loser = await local_lake.connect(
        server_settings={
            "application_name": application_name,
            "ducklake.test_native_writer_fault": "after_retag",
        }
    )
    loser_task = None
    try:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", advisory_key)
        loser_pid = await loser.fetchval("SELECT pg_backend_pid()")
        loser_task = asyncio.create_task(
            loser.execute("INSERT INTO writer_validation VALUES (1, 1), (2, 2)")
        )
        await wait_for_backend_lock(pg, loser_pid, loser_task)
        await run_pinned_writer(
            winner,
            "INSERT INTO pgducklake.public.writer_validation VALUES (50, 50)",
        )
        winner_state = await exact_protocol_state(pg)

        assert await blocker.fetchval("SELECT pg_advisory_unlock($1)", advisory_key)
        with pytest.raises(asyncpg.InternalServerError, match="test fault after retag"):
            await loser_task

        # The successful reference publication remains byte-for-byte
        # represented in protocol/data state; the later native claim and retag
        # do not.
        assert await exact_protocol_state(pg) == winner_state
        stats = dict(
            await pg.fetch("SELECT event, count FROM ducklake.native_writer_stats()")
        )
        assert stats["publication_attempts"] == 2
        assert stats["snapshot_claim_conflicts"] == 1
        assert stats["rows_retagged"] == 2

        # The backend that raised the injected ERROR starts and publishes a
        # later statement normally after disabling the session fault.
        await loser.execute("RESET ducklake.test_native_writer_fault")
        await loser.execute("INSERT INTO writer_validation VALUES (90, 90)")
    finally:
        await blocker.execute("SELECT pg_advisory_unlock_all()")
        if loser_task is not None and not loser_task.done():
            loser_task.cancel()
            await asyncio.gather(loser_task, return_exceptions=True)
        await blocker.close()
        await winner.close()
        await loser.close()

    assert await pg.fetchval(
        "SELECT array_agg(id ORDER BY id) FROM writer_validation"
    ) == [0, 50, 90]
    await assert_fresh_rows(
        local_lake, "writer_validation", [(0, 0), (50, 50), (90, 90)]
    )
