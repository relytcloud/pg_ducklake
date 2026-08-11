-- Upstream: test/sql/delete/delete_same_transaction.test
-- Skip: exact physical delete-file consolidation is not observable through PostgreSQL metadata.
-- Multiple deletes of transaction-local rows must compose without resurrecting data.

BEGIN;
CREATE TABLE upstream_delete_same_tx USING ducklake AS
SELECT i AS id FROM generate_series(0, 999) AS g(i);
DELETE FROM upstream_delete_same_tx WHERE id % 2 = 0;
SELECT count(*) FROM upstream_delete_same_tx WHERE id <= 250;
DELETE FROM upstream_delete_same_tx WHERE id <= 250;
SELECT count(*) FROM upstream_delete_same_tx WHERE id <= 250;
SELECT count(*), count(*) FILTER (WHERE id % 2 = 0) FROM upstream_delete_same_tx;
COMMIT;

SELECT count(*), count(*) FILTER (WHERE id % 2 = 0) FROM upstream_delete_same_tx;

DROP TABLE upstream_delete_same_tx;
