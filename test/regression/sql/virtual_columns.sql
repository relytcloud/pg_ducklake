-- Virtual columns: DuckLake virtual columns are accessible via
-- ducklake.rowid() etc. but excluded from SELECT * results.

CREATE TABLE vctest (a int, b text) USING ducklake;

INSERT INTO vctest VALUES (1, 'hello'), (2, 'world');

-- SELECT * excludes virtual columns
SELECT * FROM vctest ORDER BY a;

-- Explicit selection of virtual column via function
SELECT ducklake.rowid(), a FROM vctest ORDER BY a;

-- All virtual columns
SELECT ducklake.rowid(),
       ducklake.snapshot_id() IS NOT NULL AS has_snapshot,
       ducklake.filename() IS NOT NULL AS has_filename,
       ducklake.file_row_number(),
       ducklake.file_index() IS NOT NULL AS has_file_index,
       a
FROM vctest ORDER BY a;

-- Mixed: star + explicit virtual column
SELECT *, ducklake.rowid() FROM vctest ORDER BY a;

-- Virtual column in WHERE
SELECT * FROM vctest WHERE ducklake.rowid() >= 0 ORDER BY a;

-- Virtual column in ORDER BY
SELECT * FROM vctest ORDER BY ducklake.rowid();

-- ALTER TABLE ADD COLUMN: virtual columns still work
ALTER TABLE vctest ADD COLUMN c int DEFAULT 0;
SELECT ducklake.rowid(), a, c FROM vctest ORDER BY a;

-- ============================================================
-- Column name shadowing: table has a real column named "rowid"
-- ============================================================
CREATE TABLE vctest_shadow (a int, rowid int) USING ducklake;
INSERT INTO vctest_shadow VALUES (1, 42);

-- The real column appears in SELECT *
SELECT * FROM vctest_shadow;

-- Known limitation: ducklake.rowid() resolves to the user-defined
-- column when a real column named "rowid" exists (DuckDB shadowing).
SELECT ducklake.rowid(), rowid FROM vctest_shadow;

DROP TABLE vctest_shadow;

-- ============================================================
-- Non-ducklake table: virtual column functions should error
-- ============================================================
CREATE TABLE vctest_pg (a int);
INSERT INTO vctest_pg VALUES (1);

-- Should fail: not a DuckLake table
SELECT ducklake.rowid() FROM vctest_pg;

DROP TABLE vctest_pg;
DROP TABLE vctest;
