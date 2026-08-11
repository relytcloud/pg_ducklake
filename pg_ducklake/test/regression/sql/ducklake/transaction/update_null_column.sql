-- Upstream: test/sql/transaction/update_null_column.test
-- Stats pruning must remain correct after updating a previously null column.

CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_tx_boolean (active boolean) USING ducklake;
INSERT INTO upstream_tx_boolean VALUES (false);
SELECT count(*) FROM upstream_tx_boolean WHERE active = false;

CREATE TABLE upstream_tx_null_update (id bigint, tag text) USING ducklake;
INSERT INTO upstream_tx_null_update (id) VALUES (1);
UPDATE upstream_tx_null_update SET tag = 'new';
SELECT * FROM upstream_tx_null_update;
SELECT * FROM upstream_tx_null_update WHERE tag = 'new';

DROP TABLE upstream_tx_null_update;
DROP TABLE upstream_tx_boolean;
