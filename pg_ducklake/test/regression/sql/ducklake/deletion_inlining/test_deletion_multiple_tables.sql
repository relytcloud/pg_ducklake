-- Upstream: test/sql/deletion_inlining/test_deletion_multiple_tables.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_delete_multi_1 (a integer) USING ducklake;
CREATE TABLE upstream_delete_multi_2 (a integer) USING ducklake;
INSERT INTO upstream_delete_multi_1 VALUES (1),(2),(3),(1),(2),(3),(5),(6);
INSERT INTO upstream_delete_multi_2 SELECT * FROM upstream_delete_multi_1;
DELETE FROM upstream_delete_multi_1 WHERE a = 2;
DELETE FROM upstream_delete_multi_1 WHERE a = 1;
DELETE FROM upstream_delete_multi_1 WHERE a = 5;
DELETE FROM upstream_delete_multi_2 WHERE a = 2;
DELETE FROM upstream_delete_multi_2 WHERE a = 1;
DELETE FROM upstream_delete_multi_2 WHERE a = 5;
SELECT * FROM ducklake.flush_inlined_data();
SELECT * FROM upstream_delete_multi_1 ORDER BY a;
SELECT * FROM upstream_delete_multi_2 ORDER BY a;
SELECT t.table_name, sum(f.delete_count)
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name IN ('upstream_delete_multi_1','upstream_delete_multi_2')
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL
GROUP BY t.table_name ORDER BY t.table_name;
DROP TABLE upstream_delete_multi_1, upstream_delete_multi_2;
CALL ducklake.set_option('data_inlining_row_limit', 0);
