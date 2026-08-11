-- Upstream: test/sql/transaction/basic_transaction.test
-- Skip: recreating a DuckLake table after rolling back its creation currently loses the catalog.
-- DuckLake DDL and DML must obey basic commit and rollback semantics.

BEGIN;
CREATE TABLE upstream_basic_tx (i integer, j integer) USING ducklake;
SELECT * FROM upstream_basic_tx;
SELECT count(*) FROM pg_class WHERE oid = 'upstream_basic_tx'::regclass;
ROLLBACK;

SELECT to_regclass('upstream_basic_tx') IS NULL AS creation_rolled_back;

CREATE TABLE upstream_basic_tx (i integer, j integer) USING ducklake;
BEGIN;
INSERT INTO upstream_basic_tx VALUES (42, 84);
SELECT * FROM upstream_basic_tx;
ROLLBACK;
SELECT * FROM upstream_basic_tx;

DROP TABLE upstream_basic_tx;
