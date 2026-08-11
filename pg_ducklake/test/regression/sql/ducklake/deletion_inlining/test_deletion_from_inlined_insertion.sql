-- Upstream: test/sql/deletion_inlining/test_deletion_from_inlined_insertion.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_delete_inline (a integer) USING ducklake;
INSERT INTO upstream_delete_inline VALUES (1), (2), (3);
SELECT max(snapshot_id) AS vins FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_delete_inline WHERE a = 2;
SELECT r['a']::integer AS a
FROM ducklake.time_travel('upstream_delete_inline'::regclass, :vins) AS r
ORDER BY a;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_inline'::regclass);
SELECT r['a']::integer AS a
FROM ducklake.time_travel('upstream_delete_inline'::regclass, :vins) AS r
ORDER BY a;
SELECT * FROM upstream_delete_inline ORDER BY a;
SELECT f.delete_count
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_inline' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DROP TABLE upstream_delete_inline;
CALL ducklake.set_option('data_inlining_row_limit', 0);
