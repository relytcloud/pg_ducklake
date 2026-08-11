-- Upstream: test/sql/alter/promote_type.test
CREATE TABLE upstream_promote_type (col1 smallint) USING ducklake;
INSERT INTO upstream_promote_type VALUES (25);
INSERT INTO upstream_promote_type VALUES (100000);
ALTER TABLE upstream_promote_type ALTER COLUMN col1 TYPE integer;
INSERT INTO upstream_promote_type VALUES (100000);
SELECT col1 FROM upstream_promote_type ORDER BY col1;
ALTER TABLE upstream_promote_type ALTER COLUMN col1 TYPE smallint;
ALTER TABLE upstream_promote_type ALTER COLUMN nonexistent_column TYPE bigint;
DROP TABLE upstream_promote_type;
