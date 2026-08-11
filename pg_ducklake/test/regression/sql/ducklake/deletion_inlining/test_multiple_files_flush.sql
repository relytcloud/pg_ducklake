-- Upstream: test/sql/deletion_inlining/test_multiple_files_flush.test_slow
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_delete_files (a integer, payload text) USING ducklake;
INSERT INTO upstream_delete_files SELECT g, repeat('x', 50) FROM generate_series(0,199) g;
INSERT INTO upstream_delete_files SELECT g, repeat('y', 50) FROM generate_series(200,399) g;
SELECT max(snapshot_id) AS vinsert FROM ducklake.ducklake_snapshot \gset
CALL ducklake.set_option('data_inlining_row_limit', 1000);
DELETE FROM upstream_delete_files WHERE a BETWEEN 10 AND 19;
DELETE FROM upstream_delete_files WHERE a BETWEEN 210 AND 219;
DELETE FROM upstream_delete_files WHERE a % 2 = 0;
SELECT max(snapshot_id) AS vdelete FROM ducklake.ducklake_snapshot \gset
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_files'::regclass);
SELECT count(*) FROM upstream_delete_files;
SELECT count(*) FROM ducklake.time_travel('upstream_delete_files'::regclass, :vinsert);
SELECT count(*) FROM ducklake.time_travel('upstream_delete_files'::regclass, :vdelete);
SELECT count(*), sum(f.delete_count)
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_files' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DROP TABLE upstream_delete_files;
CALL ducklake.set_option('data_inlining_row_limit', 0);
