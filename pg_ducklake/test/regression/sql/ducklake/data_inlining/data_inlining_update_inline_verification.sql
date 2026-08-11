-- Upstream: test/sql/data_inlining/data_inlining_update_inline_verification.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_update_file USING ducklake AS
SELECT g AS i, 'val_' || g AS j FROM generate_series(0, 19) g;
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_update_file' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
UPDATE upstream_inline_update_file SET j = 'updated' WHERE i = 5;
UPDATE upstream_inline_update_file SET j = 'changed' WHERE i = 10;
SELECT ducklake.rowid(), i, j
FROM upstream_inline_update_file WHERE i IN (5, 10) ORDER BY i;
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_update_file' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT * FROM upstream_inline_update_file ORDER BY i;
DROP TABLE upstream_inline_update_file;
CALL ducklake.set_option('data_inlining_row_limit', 0);
