-- Upstream: test/sql/deletion_inlining/test_deletion_inlining_transaction.test
-- Skip: Internal inlined-delete row contents are not exposed by the PostgreSQL API.
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_delete_tx USING ducklake AS SELECT g AS a FROM generate_series(0,49) g;
BEGIN;
DELETE FROM upstream_delete_tx WHERE a < 5;
SELECT count(*) FROM upstream_delete_tx;
ROLLBACK;
SELECT count(*) FROM upstream_delete_tx;
BEGIN;
INSERT INTO upstream_delete_tx VALUES (56),(57),(58),(59),(60),(61),(62),(63),(64),(65),(66),(67),(68),(69),(70);
SELECT count(*) FROM upstream_delete_tx;
DELETE FROM upstream_delete_tx WHERE a < 5 OR a = 65;
SELECT count(*) FROM upstream_delete_tx;
ROLLBACK;
SELECT count(*) FROM upstream_delete_tx;
BEGIN;
DELETE FROM upstream_delete_tx WHERE a < 5;
DELETE FROM upstream_delete_tx WHERE a < 10;
DELETE FROM upstream_delete_tx WHERE a < 15;
SELECT count(*) FROM upstream_delete_tx;
ROLLBACK;
SELECT count(*) FROM upstream_delete_tx;
BEGIN;
DELETE FROM upstream_delete_tx WHERE a < 5;
DELETE FROM upstream_delete_tx WHERE a < 10;
DELETE FROM upstream_delete_tx WHERE a < 15;
COMMIT;
SELECT count(*) FROM upstream_delete_tx;
BEGIN;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_tx'::regclass);
SELECT count(*) FROM upstream_delete_tx;
ROLLBACK;
SELECT count(*) AS active_delete_files
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_tx' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
BEGIN;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_tx'::regclass);
COMMIT;
SELECT count(*) FROM upstream_delete_tx;
SELECT sum(f.delete_count)
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_tx' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DROP TABLE upstream_delete_tx;
CALL ducklake.set_option('data_inlining_row_limit', 0);
