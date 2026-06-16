# Data-inlining matrix: DuckLake can store an INSERT or a DELETE either inline
# (in the PostgreSQL metadata catalog) or as Parquet data / delete files,
# governed by data_inlining_row_limit. The four insert x delete combinations --
# including the cross cases (file rows removed by an inline delete, inline rows
# removed by a file delete) -- must all read back correctly and survive a fresh
# DuckDB instance (data lives in the PG catalog + storage, not in the instance).

import pytest

from conftest import active_file_counts, set_inlining

# A small change (<= this many rows) is inlined; 0 forces Parquet/delete files.
INLINE_LIMIT = 1000


def _limit(mode):
    return INLINE_LIMIT if mode == "inline" else 0


@pytest.mark.parametrize("delete_mode", ["inline", "file"])
@pytest.mark.parametrize("insert_mode", ["inline", "file"])
async def test_insert_delete_inline_file_matrix(inlining_lake, insert_mode, delete_mode):
    # Create the table while inlining is enabled so inline rows read back; then
    # pick the storage path for the INSERT and the DELETE independently.
    conn = await inlining_lake.connect(inlining=INLINE_LIMIT)
    try:
        await conn.execute("CREATE TABLE t (id int, v text) USING ducklake")

        await set_inlining(conn, _limit(insert_mode))
        await conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")

        await set_inlining(conn, _limit(delete_mode))
        await conn.execute("DELETE FROM t WHERE id IN (2, 4)")

        expected = [(1, "a"), (3, "c")]
        rows = await conn.fetch("SELECT id, v FROM t ORDER BY id")
        assert [tuple(r) for r in rows] == expected
        assert await conn.fetchval("SELECT count(*) FROM t") == 2

        # Confirm the storage path actually taken. A file insert writes one data
        # file; an inline insert writes none. A file delete writes a delete file
        # only when there is a data file to attach it to -- deleting inline rows
        # (inline insert) or recording the delete inline (inline delete) leaves
        # no delete file.
        data_files, delete_files = await active_file_counts(conn, "t")
        assert data_files == (1 if insert_mode == "file" else 0)
        assert delete_files == (1 if insert_mode == "file" and delete_mode == "file" else 0)

        # Durability: a brand-new DuckDB instance must reconstruct the same view
        # from the catalog + storage (inline rows, parquet, and any delete file).
        await set_inlining(conn, INLINE_LIMIT)
        await conn.execute("CALL ducklake.recycle_ddb()")
        if inlining_lake.s3:
            await inlining_lake.configure_s3(conn)  # recycle dropped the secret
        rows2 = await conn.fetch("SELECT id, v FROM t ORDER BY id")
        assert [tuple(r) for r in rows2] == expected
    finally:
        await conn.close()
