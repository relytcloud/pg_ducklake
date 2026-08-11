-- Upstream: test/sql/data_inlining/data_inlining_option_transaction_local.test
CALL ducklake.set_option('data_inlining_row_limit', 0);
BEGIN;
CREATE TABLE upstream_inline_option_tx (i integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 10);
INSERT INTO upstream_inline_option_tx VALUES (42);
COMMIT;
CALL ducklake.set_option('data_inlining_row_limit', 0);
BEGIN;
ALTER TABLE upstream_inline_option_tx ADD COLUMN j integer;
CALL ducklake.set_option('data_inlining_row_limit', 10);
INSERT INTO upstream_inline_option_tx VALUES (1, 2), (NULL, 3);
COMMIT;
SELECT * FROM upstream_inline_option_tx ORDER BY i NULLS LAST;
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_option_tx'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DROP TABLE upstream_inline_option_tx;
CALL ducklake.set_option('data_inlining_row_limit', 0);
