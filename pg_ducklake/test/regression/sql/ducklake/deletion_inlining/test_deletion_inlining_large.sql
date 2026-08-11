-- Upstream: test/sql/deletion_inlining/test_deletion_inlining_large.test
CALL ducklake.set_option('data_inlining_row_limit', 300);
CREATE TABLE upstream_delete_large USING ducklake AS SELECT g AS a, g * 2 AS b FROM generate_series(0, 999) g;
SELECT max(snapshot_id) AS vorig FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_delete_large WHERE a IN (100, 500, 999);
DELETE FROM upstream_delete_large WHERE a IN (50, 250, 750);
DELETE FROM upstream_delete_large WHERE a >= 300 AND a < 590;
SELECT max(snapshot_id) AS vdeleted FROM ducklake.ducklake_snapshot \gset
SELECT count(*), sum(a) FROM upstream_delete_large;
SELECT count(*) AS active_delete_files
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_large' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_large'::regclass);
SELECT count(*), sum(a) FROM upstream_delete_large;
SELECT count(*), sum(r['a']::integer)
FROM ducklake.time_travel('upstream_delete_large'::regclass, :vorig) AS r;
SELECT count(*), sum(r['a']::integer)
FROM ducklake.time_travel('upstream_delete_large'::regclass, :vdeleted) AS r;
DELETE FROM upstream_delete_large WHERE a >= 900;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_large'::regclass);
SELECT count(*), sum(a) FROM upstream_delete_large;
DROP TABLE upstream_delete_large;
CALL ducklake.set_option('data_inlining_row_limit', 0);
