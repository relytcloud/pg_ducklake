-- Upstream: test/sql/deletion_inlining/test_deletion_inlining_stats.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_delete_stats USING ducklake AS SELECT g AS a FROM generate_series(0,49) g;
DELETE FROM upstream_delete_stats WHERE a < 5;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_stats'::regclass);
SELECT f.delete_count
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_stats' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT s.record_count, s.next_row_id
FROM ducklake.ducklake_table_stats s JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_stats' AND t.end_snapshot IS NULL;
DELETE FROM upstream_delete_stats WHERE a >= 45;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_stats'::regclass);
SELECT f.delete_count
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_stats' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT count(*) FROM upstream_delete_stats;
DROP TABLE upstream_delete_stats;
CALL ducklake.set_option('data_inlining_row_limit', 0);
