-- Test data change feed functions: table_insertions, table_deletions, table_changes

-- Setup
CREATE TABLE dcf (id int, val text) USING ducklake;
SELECT max(snapshot_id) AS v0 FROM ducklake.ducklake_snapshot \gset

INSERT INTO dcf VALUES (1, 'one');
SELECT max(snapshot_id) AS v1 FROM ducklake.ducklake_snapshot \gset

INSERT INTO dcf VALUES (2, 'two');
SELECT max(snapshot_id) AS v2 FROM ducklake.ducklake_snapshot \gset

DELETE FROM dcf WHERE id = 1;
SELECT max(snapshot_id) AS v3 FROM ducklake.ducklake_snapshot \gset

-- 1. table_insertions by version: rows inserted between v0 and v2
SELECT count(*) FROM ducklake.table_insertions('public', 'dcf', :v0, :v2);

-- 2. table_deletions by version: rows deleted between v2 and v3
SELECT count(*) FROM ducklake.table_deletions('public', 'dcf', :v2, :v3);

-- 3. table_changes by version: all changes between v0 and v3
SELECT count(*) > 0 AS has_changes FROM ducklake.table_changes('public', 'dcf', :v0, :v3);

-- 4. Timestamp overloads
SELECT count(*) >= 0 AS ok FROM ducklake.table_insertions('public', 'dcf', now() - interval '1 hour', now());
SELECT count(*) >= 0 AS ok FROM ducklake.table_deletions('public', 'dcf', now() - interval '1 hour', now());
SELECT count(*) >= 0 AS ok FROM ducklake.table_changes('public', 'dcf', now() - interval '1 hour', now());

-- Cleanup
DROP TABLE dcf;
