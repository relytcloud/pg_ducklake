-- Upstream: test/sql/constraints/unsupported.test
-- Skip: DuckLake currently accepts PRIMARY KEY, while upstream requires it to be rejected.
CREATE TABLE upstream_constraint_primary (
  i integer PRIMARY KEY,
  j integer
) USING ducklake;
CREATE TABLE upstream_constraint_check (
  i integer,
  j integer,
  CHECK (i > j)
) USING ducklake;
DROP TABLE IF EXISTS upstream_constraint_primary, upstream_constraint_check;
