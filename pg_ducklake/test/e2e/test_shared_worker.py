# Duckdb worker under real concurrency: N asyncpg clients dispatch reads
# (and autocommit writes) to the one per-database DuckDB worker at the same time.
# pg_regress and isolation drive statements serially, so this is the only place the
# thread-per-session worker actually serves overlapping sessions.

import asyncio

READERS = 6
WRITERS = 2
ROUNDS = 5


async def test_concurrent_worker_reads(lake):
    conn = await lake.connect()
    try:
        await conn.execute("CREATE TABLE cw_r (a int, b text) USING ducklake")
        await conn.execute(
            "INSERT INTO cw_r SELECT g, 'r' || (g % 7) FROM generate_series(1, 5000) g"
        )
    finally:
        await conn.close()

    async def reader(i):
        c = await lake.connect()
        try:
            await c.execute("SET ducklake.use_shared_worker = on")
            for _ in range(ROUNDS):
                # count/min/max only: their PG parser type matches the DuckDB result
                # type. sum(int) does not (PG describes numeric) and trips the known,
                # worker-independent extended-protocol type-mismatch seam.
                assert await c.fetchval("SELECT count(*) FROM cw_r") == 5000
                assert await c.fetchval("SELECT min(a) FROM cw_r") == 1
                assert await c.fetchval("SELECT max(a) FROM cw_r") == 5000
                rows = await c.fetch(
                    "SELECT b, count(*) AS n FROM cw_r GROUP BY b ORDER BY b"
                )
                assert len(rows) == 7
                assert sum(r["n"] for r in rows) == 5000
        finally:
            await c.close()

    await asyncio.gather(*(reader(i) for i in range(READERS)))


async def test_concurrent_worker_reads_and_writes(lake):
    conn = await lake.connect()
    try:
        await conn.execute("CREATE TABLE cw_w (a int, tag int) USING ducklake")
    finally:
        await conn.close()

    async def writer(tag):
        c = await lake.connect()
        try:
            await c.execute("SET ducklake.use_shared_worker = on")
            for r in range(ROUNDS):
                # Autocommit DML dispatches; its DuckLake metadata commit runs on
                # this backend, so concurrent writers exercise the conflict retry.
                await c.execute(f"INSERT INTO cw_w SELECT g, {tag} FROM generate_series(1, 100) g")
        finally:
            await c.close()

    async def reader(i):
        c = await lake.connect()
        try:
            await c.execute("SET ducklake.use_shared_worker = on")
            for _ in range(ROUNDS):
                n = await c.fetchval("SELECT count(*) FROM cw_w")
                assert n % 100 == 0  # only whole committed batches are visible
        finally:
            await c.close()

    await asyncio.gather(
        *(writer(t) for t in range(WRITERS)),
        *(reader(i) for i in range(READERS)),
    )

    conn = await lake.connect()
    try:
        # Verify through the worker too: an in-process read of an inlined-data table
        # goes DuckDB -> PostgresTableReader -> PG parallel workers, which hangs on
        # macOS (the known ExecParallelFinish issue documented in regression.conf).
        await conn.execute("SET ducklake.use_shared_worker = on")
        assert await conn.fetchval("SELECT count(*) FROM cw_w") == WRITERS * ROUNDS * 100
        for tag in range(WRITERS):
            assert (
                await conn.fetchval(f"SELECT count(*) FROM cw_w WHERE tag = {tag}")
                == ROUNDS * 100
            )
    finally:
        await conn.close()
