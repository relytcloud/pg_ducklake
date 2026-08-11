-- Upstream: test/sql/data_inlining/data_inlining_delete.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_delete USING ducklake AS
SELECT * FROM (VALUES (1,2), (NULL::integer,3), (10,20)) v(i,j);
BEGIN;
DELETE FROM upstream_inline_delete WHERE i = 1;
SELECT * FROM upstream_inline_delete ORDER BY j;
COMMIT;
BEGIN;
DELETE FROM upstream_inline_delete WHERE i = 10;
SELECT * FROM upstream_inline_delete;
COMMIT;
DELETE FROM upstream_inline_delete;
SELECT count(*) FROM upstream_inline_delete;
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_delete' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DROP TABLE upstream_inline_delete;
CALL ducklake.set_option('data_inlining_row_limit', 0);
