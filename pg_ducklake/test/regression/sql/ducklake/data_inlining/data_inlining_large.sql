-- Upstream: test/sql/data_inlining/data_inlining_large.test
CALL ducklake.set_option('data_inlining_row_limit', 9999);
CREATE TABLE upstream_inline_large (i integer) USING ducklake;
INSERT INTO upstream_inline_large SELECT g FROM generate_series(0, 9999) g;
SELECT count(*), sum(i), min(i), max(i) FROM upstream_inline_large;
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_large' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DROP TABLE upstream_inline_large;
CALL ducklake.set_option('data_inlining_row_limit', 0);
