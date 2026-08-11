-- Upstream: test/sql/alter/add_column_nested.test
-- Skip: PostgreSQL has no anonymous STRUCT column type equivalent to the upstream cases.
-- PostgreSQL exposes DuckLake LIST values as arrays; STRUCT fields are omitted.
CREATE TABLE upstream_add_nested (col1 integer[]) USING ducklake;
INSERT INTO upstream_add_nested VALUES (ARRAY[1, 2]);
ALTER TABLE upstream_add_nested ADD COLUMN new_col2 integer[];
INSERT INTO upstream_add_nested VALUES (ARRAY[100, 200], ARRAY[]::integer[]);
ALTER TABLE upstream_add_nested ADD COLUMN new_col3 text[];
INSERT INTO upstream_add_nested VALUES
  (ARRAY[42, NULL], ARRAY[1, 2, 3], ARRAY['k=1', 'v=2']);
SELECT * FROM upstream_add_nested ORDER BY col1[1];
CALL ducklake.recycle_ddb();
SELECT * FROM upstream_add_nested ORDER BY col1[1];
DROP TABLE upstream_add_nested;
