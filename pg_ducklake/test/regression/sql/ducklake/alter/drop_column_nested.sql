-- Upstream: test/sql/alter/drop_column_nested.test
-- Skip: PostgreSQL has no anonymous STRUCT column type equivalent to the upstream cases.
-- PostgreSQL exposes DuckLake LIST values as arrays; STRUCT fields are omitted.
CREATE TABLE upstream_drop_nested (
  col1 integer[], col2 text[], col3 integer[]
) USING ducklake;
ALTER TABLE upstream_drop_nested DROP COLUMN col2;
ALTER TABLE upstream_drop_nested ADD COLUMN new_col2 integer[];
ALTER TABLE upstream_drop_nested DROP COLUMN col3;
ALTER TABLE upstream_drop_nested ADD COLUMN new_col3 text[];
INSERT INTO upstream_drop_nested VALUES
  (ARRAY[42, NULL], ARRAY[1, 2, 3], ARRAY['k=1', 'v=2']);
SELECT * FROM upstream_drop_nested ORDER BY col1[1];
CALL ducklake.recycle_ddb();
SELECT * FROM upstream_drop_nested ORDER BY col1[1];
DROP TABLE upstream_drop_nested;
