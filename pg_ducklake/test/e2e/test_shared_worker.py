# Many backends dispatching to one shared duckdb worker at the same time,
# alongside in-process writers. Local storage only: the worker's engine takes
# its S3 credentials from user mappings, not from a session's raw secret.

import asyncio

import asyncpg

READERS = 6
ROUNDS = 5
ROWS_PER_WRITE = 100


async def connect(cluster, db, shared_worker):
    conn = await asyncpg.connect(
        host=cluster.host, port=cluster.port, user="postgres", database=db
    )
    await conn.execute(
        f"SET ducklake.use_shared_worker = {'on' if shared_worker else 'off'}"
    )
    return conn


async def test_concurrent_readers(cluster, db):
    writer = await connect(cluster, db, shared_worker=False)
    await writer.execute("CREATE TABLE t (i int, s text) USING ducklake")
    await writer.execute(
        "INSERT INTO t SELECT g, 'row' || g FROM generate_series(1, 10000) g"
    )
    readers = [await connect(cluster, db, shared_worker=True) for _ in range(READERS)]
    try:
        before = await writer.fetchval(
            "SELECT coalesce(total_sessions, 0) FROM ducklake.worker_stats()"
        )

        async def read(conn, lower):
            rows = await conn.fetch(
                "SELECT count(*), min(i), min(s) FROM t WHERE i > $1", lower
            )
            return tuple(rows[0])

        for _ in range(ROUNDS):
            results = await asyncio.gather(
                *(read(conn, k * 1000) for k, conn in enumerate(readers))
            )
            for k, (count, lowest, smallest) in enumerate(results):
                expected = range(k * 1000 + 1, 10001)
                assert count == len(expected)
                assert lowest == expected[0]
                assert smallest == min(f"row{i}" for i in expected)

        after = await writer.fetchval(
            "SELECT total_sessions FROM ducklake.worker_stats()"
        )
        assert after - before == READERS * ROUNDS
    finally:
        for conn in readers:
            await conn.close()
        await writer.close()


async def test_readers_see_whole_commits(cluster, db):
    writer = await connect(cluster, db, shared_worker=False)
    await writer.execute("CREATE TABLE t (i int) USING ducklake")
    readers = [await connect(cluster, db, shared_worker=True) for _ in range(READERS)]
    stop = asyncio.Event()
    try:

        async def write():
            for _ in range(ROUNDS):
                await writer.execute(
                    f"INSERT INTO t SELECT g FROM generate_series(1, {ROWS_PER_WRITE}) g"
                )
            stop.set()

        async def read(conn):
            seen = 0
            while not stop.is_set():
                count = await conn.fetchval("SELECT count(*) FROM t")
                assert count % ROWS_PER_WRITE == 0, count
                assert count >= seen, (count, seen)
                seen = count
            return seen

        results = await asyncio.gather(write(), *(read(conn) for conn in readers))
        final = await readers[0].fetchval("SELECT count(*) FROM t")
        assert final == ROUNDS * ROWS_PER_WRITE
        assert all(seen <= final for seen in results[1:])
    finally:
        for conn in readers:
            await conn.close()
        await writer.close()
