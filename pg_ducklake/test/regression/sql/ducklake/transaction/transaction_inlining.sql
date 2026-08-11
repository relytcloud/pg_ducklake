-- Upstream: test/sql/transaction/transaction_inlining.test
-- Inlined inserts into a newly partitioned table must survive commit and flush.

CALL ducklake.set_option('data_inlining_row_limit', 1000);
BEGIN;
CREATE TABLE upstream_tx_inlining (yr integer) USING ducklake;
CALL ducklake.set_partition('upstream_tx_inlining'::regclass, 'yr');
INSERT INTO upstream_tx_inlining VALUES (2025), (2026), (2027);
COMMIT;

SELECT * FROM upstream_tx_inlining ORDER BY yr;
SELECT * FROM ducklake.flush_inlined_data('upstream_tx_inlining'::regclass);
SELECT * FROM upstream_tx_inlining ORDER BY yr;

DROP TABLE upstream_tx_inlining;
CALL ducklake.set_option('data_inlining_row_limit', 0);
