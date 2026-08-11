-- Upstream: test/sql/catalog/drop_entry_same_schema.test
-- Skip: PostgreSQL views are not represented as DuckLake catalog view entries.
-- The supported table portion: dropping one transaction-local table leaves its sibling intact.

BEGIN;
CREATE TABLE upstream_drop_same_a (x integer) USING ducklake;
CREATE TABLE upstream_drop_same_b (y integer) USING ducklake;
INSERT INTO upstream_drop_same_a VALUES (1);
INSERT INTO upstream_drop_same_b VALUES (2);
DROP TABLE upstream_drop_same_a;
SELECT to_regclass('upstream_drop_same_a') IS NULL AS first_dropped;
SELECT * FROM upstream_drop_same_b;
COMMIT;
SELECT * FROM upstream_drop_same_b;
DROP TABLE upstream_drop_same_b;
