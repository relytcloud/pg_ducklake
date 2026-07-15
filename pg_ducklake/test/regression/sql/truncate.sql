-- TRUNCATE on ducklake tables (issue #224).
\set QUIET off

CREATE TABLE trunc_t (id int, val text) USING ducklake;
INSERT INTO trunc_t VALUES (1, 'one'), (2, 'two'), (3, 'three');

-- Plain TRUNCATE: rows gone, a delete snapshot is recorded
TRUNCATE trunc_t;
SELECT count(*) FROM trunc_t;
SELECT changes_made ~ '^(deleted_from_table|inlined_delete):\d+$' AS truncate_recorded
FROM ducklake.ducklake_snapshot_changes ORDER BY snapshot_id DESC LIMIT 1;

-- TRUNCATE then INSERT
INSERT INTO trunc_t VALUES (4, 'four');
SELECT * FROM trunc_t ORDER BY id;

-- TRUNCATE inside a rolled-back transaction: rows restored
BEGIN;
TRUNCATE trunc_t;
SELECT count(*) FROM trunc_t;
ROLLBACK;
SELECT * FROM trunc_t ORDER BY id;

-- Mixed-list TRUNCATE of a heap table and a ducklake table
CREATE TABLE trunc_heap (id int);
INSERT INTO trunc_heap VALUES (1), (2);
TRUNCATE trunc_heap, trunc_t;
SELECT count(*) FROM trunc_heap;
SELECT count(*) FROM trunc_t;

-- TRUNCATE of a table created in the same transaction (nontransactional path)
BEGIN;
CREATE TABLE trunc_same_txn (id int) USING ducklake;
INSERT INTO trunc_same_txn VALUES (1), (2);
TRUNCATE trunc_same_txn;
SELECT count(*) FROM trunc_same_txn;
COMMIT;
SELECT count(*) FROM trunc_same_txn;

\set QUIET on

-- Cleanup
DROP TABLE trunc_t;
DROP TABLE trunc_heap;
DROP TABLE trunc_same_txn;
