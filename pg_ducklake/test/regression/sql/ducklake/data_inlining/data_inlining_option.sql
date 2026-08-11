-- Upstream: test/sql/data_inlining/data_inlining_option.test
-- Skip: Requires implementation: ducklake.options() does not expose the persisted inlining option.
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_option (i integer, j integer) USING ducklake;
INSERT INTO upstream_inline_option VALUES (1, 2), (NULL, 3), (5, 5);
SELECT * FROM upstream_inline_option ORDER BY j;
SELECT option_name, value, scope
FROM ducklake.options()
WHERE option_name = 'data_inlining_row_limit' AND scope = 'global';
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_option' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DROP TABLE upstream_inline_option;
CALL ducklake.set_option('data_inlining_row_limit', 0);
