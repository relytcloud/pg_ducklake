-- Upstream: test/sql/deletion_inlining/test_deletion_from_file_and_inserted_inlining_non_sequential.test
CALL ducklake.set_option('data_inlining_row_limit', 5);
CREATE TABLE upstream_delete_mixed_ns USING ducklake AS
SELECT * FROM (VALUES (42),(17),(89),(3),(156),(71),(28),(200),(55),(102)) v(a);
INSERT INTO upstream_delete_mixed_ns VALUES (999), (500), (777);
DELETE FROM upstream_delete_mixed_ns WHERE a IN (71, 500);
SELECT count(*), sum(a) FROM upstream_delete_mixed_ns;
SELECT count(*) AS active_delete_files
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_mixed_ns' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_mixed_ns'::regclass);
SELECT count(*), sum(a) FROM upstream_delete_mixed_ns;
SELECT * FROM upstream_delete_mixed_ns ORDER BY a;
DROP TABLE upstream_delete_mixed_ns;
CALL ducklake.set_option('data_inlining_row_limit', 0);
