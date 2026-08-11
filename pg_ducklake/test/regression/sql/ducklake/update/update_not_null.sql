-- Upstream: test/sql/update/update_not_null.test
-- A NOT NULL violation must abort the transaction and leave the row unchanged.

CREATE TABLE upstream_update_not_null (i integer NOT NULL, j integer) USING ducklake;
INSERT INTO upstream_update_not_null VALUES (42, NULL);

BEGIN;
UPDATE upstream_update_not_null SET i = NULL;
UPDATE upstream_update_not_null SET i = 100;
ROLLBACK;

SELECT * FROM upstream_update_not_null;

DROP TABLE upstream_update_not_null;
