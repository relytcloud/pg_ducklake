-- Upstream: test/sql/data_inlining/data_inlining_update.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_update USING ducklake AS
SELECT * FROM (VALUES (1,2), (NULL::integer,3), (10,20)) v(i,j);
BEGIN;
UPDATE upstream_inline_update SET i = i + 100 WHERE i = 1;
SELECT ducklake.rowid(), i, j FROM upstream_inline_update ORDER BY ducklake.rowid();
COMMIT;
BEGIN;
UPDATE upstream_inline_update SET i = i + 1000 WHERE i = 10;
COMMIT;
SELECT ducklake.rowid(), i, j FROM upstream_inline_update ORDER BY ducklake.rowid();
CREATE TEMP TABLE upstream_inline_dup AS SELECT 101 AS update_id FROM generate_series(1, 10000);
UPDATE upstream_inline_update u SET i = i + 1000
FROM upstream_inline_dup d WHERE u.i = d.update_id;
SELECT i, j FROM upstream_inline_update ORDER BY j;
DROP TABLE upstream_inline_dup;
DROP TABLE upstream_inline_update;
CALL ducklake.set_option('data_inlining_row_limit', 0);
