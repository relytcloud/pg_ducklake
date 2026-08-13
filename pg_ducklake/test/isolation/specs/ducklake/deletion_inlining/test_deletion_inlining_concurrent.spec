# Upstream: test/sql/deletion_inlining/test_deletion_inlining_concurrent.test
# Skip: Requires implementation: disjoint same-table deletes currently conflict instead of retrying; do not bless the conflict.
# Keep deterministic serialized coverage until the overlapping transaction path is supported.
setup
{
  DROP TABLE IF EXISTS upstream_iso_delete_same;
  CALL ducklake.set_option('data_inlining_row_limit', 10);
  CREATE TABLE upstream_iso_delete_same USING ducklake AS
    SELECT g AS a FROM generate_series(0, 49) g;
}

session d1
step d1_begin  { BEGIN; }
step d1_pin    { SELECT count(*) FROM upstream_iso_delete_same; }
step d1_delete { DELETE FROM upstream_iso_delete_same WHERE a < 5; }
step d1_commit { COMMIT; }

session d2
step d2_begin  { BEGIN; }
step d2_pin    { SELECT count(*) FROM upstream_iso_delete_same; }
step d2_delete { DELETE FROM upstream_iso_delete_same WHERE a >= 5 AND a < 10; }
step d2_commit { COMMIT; }

session checker
step check_rows {
  SELECT count(*) > 0 AS rows_remain,
         count(*) FILTER (WHERE a < 10) AS low_rows
  FROM upstream_iso_delete_same;
}

teardown
{
  DROP TABLE IF EXISTS upstream_iso_delete_same;
  CALL ducklake.set_option('data_inlining_row_limit', 0);
}

permutation d1_begin d1_pin d1_delete d1_commit d2_begin d2_pin d2_delete d2_commit check_rows
permutation d2_begin d2_pin d2_delete d2_commit d1_begin d1_pin d1_delete d1_commit check_rows
