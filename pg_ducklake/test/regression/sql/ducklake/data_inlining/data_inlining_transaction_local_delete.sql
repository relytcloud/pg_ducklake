-- Upstream: test/sql/data_inlining/data_inlining_transaction_local_delete.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_tx_delete (i integer, j integer) USING ducklake;
BEGIN;
INSERT INTO upstream_inline_tx_delete VALUES
 (42,84), (100,200), (200,300), (300,400), (400,500);
DELETE FROM upstream_inline_tx_delete WHERE i = 100;
SELECT * FROM upstream_inline_tx_delete ORDER BY i;
COMMIT;
BEGIN;
DELETE FROM upstream_inline_tx_delete WHERE i = 300;
DELETE FROM upstream_inline_tx_delete WHERE i = 200;
SELECT * FROM upstream_inline_tx_delete ORDER BY i;
COMMIT;
SELECT * FROM upstream_inline_tx_delete ORDER BY i;
DROP TABLE upstream_inline_tx_delete;
CALL ducklake.set_option('data_inlining_row_limit', 0);
