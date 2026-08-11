-- Upstream: test/sql/rewrite_data_files/test_rewrite_large_file_with_deletes.test
-- Delete rewrite must not skip an oversized data file.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CALL ducklake.set_option('target_file_size', '1KB');
CALL ducklake.set_option('rewrite_delete_threshold', 0);
CREATE TABLE upstream_rewrite_large (key integer, value text) USING ducklake;
INSERT INTO upstream_rewrite_large SELECT i, 'thisisastring_' || i FROM generate_series(0, 999) AS g(i);
DELETE FROM upstream_rewrite_large WHERE key < 500;
SELECT count(*) = 1 AS one_live_data_file_before
FROM ducklake.ducklake_data_file df
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_rewrite_large' AND t.end_snapshot IS NULL
  AND df.end_snapshot IS NULL;
SELECT count(*) = 1 AS one_live_delete_file_before
FROM ducklake.ducklake_delete_file df
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_rewrite_large' AND t.end_snapshot IS NULL
  AND df.end_snapshot IS NULL;
SELECT * FROM ducklake.rewrite_data_files('upstream_rewrite_large'::regclass);
SELECT count(*), min(key), max(key) FROM upstream_rewrite_large;
SELECT count(*) = 1 AS one_live_data_file_after
FROM ducklake.ducklake_data_file df
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_rewrite_large' AND t.end_snapshot IS NULL
  AND df.end_snapshot IS NULL;
SELECT count(*) = 0 AS no_live_delete_files_after
FROM ducklake.ducklake_delete_file df
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_rewrite_large' AND t.end_snapshot IS NULL
  AND df.end_snapshot IS NULL;
DROP TABLE upstream_rewrite_large;
