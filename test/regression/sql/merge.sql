-- MERGE INTO support (PG 15+, routed to DuckDB/DuckLake).
-- PG 14 does not have MERGE syntax; skip the entire file.

SELECT current_setting('server_version_num')::int >= 150000 AS has_merge \gset
\if :has_merge

-- Enable row-count display so completion tags are visible.
\set QUIET off

-- Setup: target and source tables
CREATE TABLE merge_target (id int, val text, n int) USING ducklake;
CREATE TABLE merge_source (id int, val text, n int) USING ducklake;

INSERT INTO merge_target VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30);
INSERT INTO merge_source VALUES (2, 'TWO', 200), (3, 'THREE', 300), (4, 'four', 40);

-- Basic upsert: update matched, insert unmatched
MERGE INTO merge_target t
USING merge_source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET val = s.val, n = s.n
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val, s.n);

SELECT * FROM merge_target ORDER BY id;

-- Reset (TRUNCATE is a no-op on ducklake tables, use DELETE)
DELETE FROM merge_target;
INSERT INTO merge_target VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30);

-- MERGE with DELETE action
MERGE INTO merge_target t
USING merge_source s ON t.id = s.id
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val, s.n);

SELECT * FROM merge_target ORDER BY id;

-- Reset
DELETE FROM merge_target;
INSERT INTO merge_target VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30);

-- Conditional match: only update when s.n > 250
MERGE INTO merge_target t
USING merge_source s ON t.id = s.id
WHEN MATCHED AND s.n > 250 THEN UPDATE SET val = s.val, n = s.n
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val, s.n);

SELECT * FROM merge_target ORDER BY id;

-- Reset
DELETE FROM merge_target;
INSERT INTO merge_target VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30);

-- USING subquery
MERGE INTO merge_target t
USING (SELECT id, val, n FROM merge_source WHERE n > 100) s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET val = s.val, n = s.n
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val, s.n);

SELECT * FROM merge_target ORDER BY id;

-- Reset
DELETE FROM merge_target;
INSERT INTO merge_target VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30);

-- USING VALUES
MERGE INTO merge_target t
USING (VALUES (2, 'replaced', 99), (5, 'new', 50)) AS s(id, val, n) ON t.id = s.id
WHEN MATCHED THEN UPDATE SET val = s.val, n = s.n
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val, s.n);

SELECT * FROM merge_target ORDER BY id;

-- Reset
DELETE FROM merge_target;
INSERT INTO merge_target VALUES (1, 'one', 10);

-- DO NOTHING action
MERGE INTO merge_target t
USING merge_source s ON t.id = s.id
WHEN MATCHED THEN DO NOTHING
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val, s.n);

SELECT * FROM merge_target ORDER BY id;

\set QUIET on

-- Cleanup
DROP TABLE merge_source;
DROP TABLE merge_target;

\else
-- PG 14: MERGE syntax not available, emit placeholder output.
\echo 'MERGE requires PostgreSQL 15+, skipping'
\endif
