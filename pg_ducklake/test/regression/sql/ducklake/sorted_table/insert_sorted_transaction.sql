-- Upstream: test/sql/sorted_table/insert_sorted_transaction.test
-- Skip: Physical inserted-file order is not exposed by the PostgreSQL API.
-- Transaction commit and rollback control both sort metadata and inserted rows.
CREATE TABLE upstream_insert_sort_tx (i integer) USING ducklake;
BEGIN;
CALL ducklake.set_sort('upstream_insert_sort_tx'::regclass, 'i DESC');
INSERT INTO upstream_insert_sort_tx VALUES (3), (1), (2);
COMMIT;
SELECT * FROM ducklake.get_sort('upstream_insert_sort_tx'::regclass);
SELECT i FROM upstream_insert_sort_tx ORDER BY i;
CREATE TABLE upstream_insert_sort_rollback (i integer) USING ducklake;
BEGIN;
CALL ducklake.set_sort('upstream_insert_sort_rollback'::regclass, 'i ASC');
INSERT INTO upstream_insert_sort_rollback VALUES (3), (1), (2);
ROLLBACK;
SELECT * FROM ducklake.get_sort('upstream_insert_sort_rollback'::regclass);
SELECT count(*) FROM upstream_insert_sort_rollback;
DROP TABLE upstream_insert_sort_tx, upstream_insert_sort_rollback;
