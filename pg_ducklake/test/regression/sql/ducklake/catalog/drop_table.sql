-- Upstream: test/sql/catalog/drop_table.test

CREATE TABLE upstream_catalog_drop (i integer) USING ducklake;
BEGIN;
DROP TABLE upstream_catalog_drop;
SELECT to_regclass('upstream_catalog_drop') IS NULL AS hidden_in_transaction;
ROLLBACK;
SELECT * FROM upstream_catalog_drop;
DROP TABLE upstream_catalog_drop;
SELECT to_regclass('upstream_catalog_drop') IS NULL AS dropped;
BEGIN;
SAVEPOINT expected_missing_drop;
DROP TABLE upstream_catalog_drop;
ROLLBACK TO SAVEPOINT expected_missing_drop;
COMMIT;
DROP TABLE IF EXISTS upstream_catalog_drop;

BEGIN;
CREATE TABLE upstream_catalog_drop_local (i integer) USING ducklake;
SELECT * FROM upstream_catalog_drop_local;
DROP TABLE upstream_catalog_drop_local;
SELECT to_regclass('upstream_catalog_drop_local') IS NULL AS local_dropped;
COMMIT;

CREATE TABLE upstream_catalog_drop (i integer) USING ducklake;
BEGIN;
DROP TABLE upstream_catalog_drop;
CREATE TABLE upstream_catalog_drop (i text, j text) USING ducklake;
INSERT INTO upstream_catalog_drop VALUES ('hello', 'world');
SELECT * FROM upstream_catalog_drop;
COMMIT;
SELECT * FROM upstream_catalog_drop;
DROP TABLE upstream_catalog_drop;
