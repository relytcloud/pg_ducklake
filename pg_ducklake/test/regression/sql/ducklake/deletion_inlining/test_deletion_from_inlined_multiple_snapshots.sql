-- Upstream: test/sql/deletion_inlining/test_deletion_from_inlined_multiple_snapshots.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_delete_history (a integer) USING ducklake;
INSERT INTO upstream_delete_history VALUES (1),(2),(3),(1),(2),(3),(5),(6);
SELECT max(snapshot_id) AS vins FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_delete_history WHERE a = 2;
SELECT max(snapshot_id) AS vd2 FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_delete_history WHERE a = 1;
SELECT max(snapshot_id) AS vd1 FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_delete_history WHERE a = 5;
SELECT max(snapshot_id) AS vd5 FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_delete_history WHERE a = 6;
SELECT max(snapshot_id) AS vd6 FROM ducklake.ducklake_snapshot \gset
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_history'::regclass);
SELECT * FROM upstream_delete_history ORDER BY a;
SELECT r['a']::integer AS a FROM ducklake.time_travel('upstream_delete_history'::regclass, :vins) AS r ORDER BY a;
SELECT r['a']::integer AS a FROM ducklake.time_travel('upstream_delete_history'::regclass, :vd2) AS r ORDER BY a;
SELECT r['a']::integer AS a FROM ducklake.time_travel('upstream_delete_history'::regclass, :vd1) AS r ORDER BY a;
SELECT r['a']::integer AS a FROM ducklake.time_travel('upstream_delete_history'::regclass, :vd5) AS r ORDER BY a;
SELECT r['a']::integer AS a FROM ducklake.time_travel('upstream_delete_history'::regclass, :vd6) AS r ORDER BY a;
SELECT sum(f.delete_count)
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_history' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DROP TABLE upstream_delete_history;
CALL ducklake.set_option('data_inlining_row_limit', 0);
