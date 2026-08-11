-- Upstream: test/sql/deletion_inlining/test_deletion_inlining.test
-- Skip: Deletion-vector contents and scheduled physical-file cleanup are not exposed by the PostgreSQL API.
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_delete_basic USING ducklake AS SELECT g AS a FROM generate_series(0, 49) g;
DELETE FROM upstream_delete_basic WHERE a < 5;
DELETE FROM upstream_delete_basic WHERE a >= 5 AND a < 9;
DELETE FROM upstream_delete_basic WHERE a = 15;
SELECT count(*), sum(a) FROM upstream_delete_basic;
SELECT count(*) AS active_delete_files
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_basic' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_basic'::regclass);
SELECT sum(f.delete_count), count(*)
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_basic' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DELETE FROM upstream_delete_basic WHERE a >= 9 AND a < 15;
DELETE FROM upstream_delete_basic WHERE a > 45;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_basic'::regclass);
INSERT INTO upstream_delete_basic VALUES (51),(52),(53),(54),(55);
INSERT INTO upstream_delete_basic SELECT g FROM generate_series(56, 70) g;
DELETE FROM upstream_delete_basic WHERE a = 40 OR a = 53 OR a > 65;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_basic'::regclass);
SELECT * FROM upstream_delete_basic ORDER BY a;
SELECT sum(f.delete_count)
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_basic' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DROP TABLE upstream_delete_basic;
CALL ducklake.set_option('data_inlining_row_limit', 0);
