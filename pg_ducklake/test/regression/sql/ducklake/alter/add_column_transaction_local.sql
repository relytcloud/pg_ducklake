-- Upstream: test/sql/alter/add_column_transaction_local.test
CREATE TABLE upstream_add_tx_local (col1 integer) USING ducklake;
BEGIN;
INSERT INTO upstream_add_tx_local VALUES (42);
ALTER TABLE upstream_add_tx_local ADD COLUMN new_col2 integer;
SELECT * FROM upstream_add_tx_local ORDER BY col1;
COMMIT;
SELECT * FROM upstream_add_tx_local ORDER BY col1;
DROP TABLE upstream_add_tx_local;
