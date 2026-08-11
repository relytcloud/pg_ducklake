# Upstream: test/sql/deletion_inlining/test_deletion_inlining_concurrent_retry.test
# Concurrent inlined deletes on separate tables must both survive snapshot-claim retry.
setup
{
  CALL ducklake.set_option('data_inlining_row_limit', 10);
  CREATE TABLE upstream_iso_delete_retry_1 USING ducklake AS
    SELECT g AS a FROM generate_series(0, 49) g;
  CREATE TABLE upstream_iso_delete_retry_2 USING ducklake AS
    SELECT g AS a FROM generate_series(0, 49) g;
}

session r1
step r1_begin  { BEGIN; }
step r1_pin    { SELECT count(*) FROM upstream_iso_delete_retry_1; }
step r1_delete { DELETE FROM upstream_iso_delete_retry_1 WHERE a < 5; }
step r1_commit { COMMIT; }

session r2
step r2_begin  { BEGIN; }
step r2_pin    { SELECT count(*) FROM upstream_iso_delete_retry_2; }
step r2_delete { DELETE FROM upstream_iso_delete_retry_2 WHERE a < 5; }
step r2_commit { COMMIT; }
step r2_check  {
  SELECT (SELECT count(*) FROM upstream_iso_delete_retry_1) AS t1_rows,
         (SELECT count(*) FROM upstream_iso_delete_retry_2) AS t2_rows,
         (SELECT count(*) FROM upstream_iso_delete_retry_1 WHERE a < 5) AS t1_low,
         (SELECT count(*) FROM upstream_iso_delete_retry_2 WHERE a < 5) AS t2_low,
         (SELECT sum(a) FROM upstream_iso_delete_retry_1) AS t1_sum,
         (SELECT sum(a) FROM upstream_iso_delete_retry_2) AS t2_sum;
}

teardown
{
  DROP TABLE upstream_iso_delete_retry_1, upstream_iso_delete_retry_2;
  CALL ducklake.set_option('data_inlining_row_limit', 0);
}

permutation r1_begin r1_pin r2_begin r2_pin r1_delete r2_delete r1_commit r2_commit r2_check
permutation r1_begin r1_pin r2_begin r2_pin r1_delete r2_delete r2_commit r1_commit r2_check
