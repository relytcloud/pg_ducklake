-- Upstream: test/sql/data_inlining/data_inlining_txn_delete_visibility.test
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_inline_delete_vis (id integer, val integer) USING ducklake;
INSERT INTO upstream_inline_delete_vis VALUES (1,10), (2,20), (3,30);
DELETE FROM upstream_inline_delete_vis WHERE id = 1;
SELECT * FROM upstream_inline_delete_vis ORDER BY id;
BEGIN;
DELETE FROM upstream_inline_delete_vis WHERE id = 2;
SELECT * FROM upstream_inline_delete_vis ORDER BY id;
COMMIT;
SELECT * FROM upstream_inline_delete_vis ORDER BY id;
DROP TABLE upstream_inline_delete_vis;
