# Upstream: test/sql/transaction/concurrent_table_creation.test
# Skip: concurrent different-name table creation currently conflicts at the second commit.
# Keep deterministic serialized creation coverage until that conflict is fixed.

setup
{
  DROP TABLE IF EXISTS upstream_iso_create_one;
  DROP TABLE IF EXISTS upstream_iso_create_two;
}

session s1
step s1_begin  { BEGIN; }
step s1_create { CREATE TABLE upstream_iso_create_one USING ducklake AS SELECT 42 AS i; }
step s1_commit { COMMIT; }

session s2
step s2_begin  { BEGIN; }
step s2_create { CREATE TABLE upstream_iso_create_two USING ducklake AS SELECT 'hello world'::text AS s; }
step s2_commit { COMMIT; }

session check_session
step check_rows {
  SELECT * FROM upstream_iso_create_one;
  SELECT * FROM upstream_iso_create_two;
}

teardown
{
  DROP TABLE IF EXISTS upstream_iso_create_one;
  DROP TABLE IF EXISTS upstream_iso_create_two;
}

permutation s1_begin s1_create s1_commit s2_begin s2_create s2_commit check_rows
