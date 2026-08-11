-- Upstream: test/sql/delete/empty_delete.test
-- A DELETE matching no rows must leave the transaction and table unchanged.

CREATE TABLE upstream_empty_delete USING ducklake AS
SELECT i AS id FROM generate_series(0, 999) AS g(i);

BEGIN;
DELETE FROM upstream_empty_delete WHERE id > 10000;
SELECT count(*), count(*) FILTER (WHERE id % 2 = 0) FROM upstream_empty_delete;
COMMIT;

SELECT count(*), count(*) FILTER (WHERE id % 2 = 0) FROM upstream_empty_delete;

DROP TABLE upstream_empty_delete;
