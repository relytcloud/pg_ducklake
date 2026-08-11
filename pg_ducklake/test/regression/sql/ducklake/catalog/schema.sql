-- Upstream: test/sql/catalog/schema.test
-- Map DuckLake table behavior across PostgreSQL schemas.

CREATE SCHEMA upstream_catalog_s1;
CREATE SCHEMA upstream_catalog_s2;
CREATE TABLE upstream_catalog_s1.tbl (i integer) USING ducklake;
CREATE TABLE upstream_catalog_s2.tbl (a text, b text) USING ducklake;
INSERT INTO upstream_catalog_s1.tbl VALUES (42);
INSERT INTO upstream_catalog_s2.tbl VALUES ('hello', 'world');
SELECT * FROM upstream_catalog_s1.tbl;
SELECT * FROM upstream_catalog_s2.tbl;
DROP SCHEMA upstream_catalog_s1;
DROP TABLE upstream_catalog_s1.tbl;
DROP SCHEMA upstream_catalog_s1;
DROP SCHEMA upstream_catalog_s2 CASCADE;
SELECT to_regnamespace('upstream_catalog_s1') IS NULL AS s1_dropped,
       to_regnamespace('upstream_catalog_s2') IS NULL AS s2_dropped;

BEGIN;
CREATE SCHEMA upstream_catalog_s1;
CREATE TABLE upstream_catalog_s1.tbl (i integer) USING ducklake;
INSERT INTO upstream_catalog_s1.tbl VALUES (84);
COMMIT;
SELECT * FROM upstream_catalog_s1.tbl;
DROP SCHEMA upstream_catalog_s1 CASCADE;
