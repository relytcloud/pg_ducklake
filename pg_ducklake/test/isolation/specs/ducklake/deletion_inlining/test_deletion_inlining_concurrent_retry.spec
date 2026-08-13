# Upstream: test/sql/deletion_inlining/test_deletion_inlining_concurrent_retry.test
# Concurrent small deletes on separate file-backed tables must all survive the
# shared snapshot-claim retry path.
setup {
  DROP TABLE IF EXISTS upstream_iso_delete_retry_0;
  DROP TABLE IF EXISTS upstream_iso_delete_retry_1;
  DROP TABLE IF EXISTS upstream_iso_delete_retry_2;
}
setup { CREATE TABLE upstream_iso_delete_retry_0 (a bigint) USING ducklake; }
setup { CREATE TABLE upstream_iso_delete_retry_1 (a bigint) USING ducklake; }
setup { CREATE TABLE upstream_iso_delete_retry_2 (a bigint) USING ducklake; }
setup {
  CALL ducklake.set_option(
    'data_inlining_row_limit', 10, 'upstream_iso_delete_retry_0'::regclass);
}
setup {
  CALL ducklake.set_option(
    'data_inlining_row_limit', 10, 'upstream_iso_delete_retry_1'::regclass);
}
setup {
  CALL ducklake.set_option(
    'data_inlining_row_limit', 10, 'upstream_iso_delete_retry_2'::regclass);
}
setup {
  INSERT INTO upstream_iso_delete_retry_0 SELECT g FROM generate_series(0, 49) g;
}
setup {
  INSERT INTO upstream_iso_delete_retry_1 SELECT g FROM generate_series(0, 49) g;
}
setup {
  INSERT INTO upstream_iso_delete_retry_2 SELECT g FROM generate_series(0, 49) g;
}

session r0
step r0_begin  { BEGIN; }
step r0_pin    { SELECT count(*) AS pinned_rows FROM upstream_iso_delete_retry_0; }
step r0_delete { DELETE FROM upstream_iso_delete_retry_0 WHERE a < 5; }
step r0_commit { COMMIT; }

session r1
step r1_begin  { BEGIN; }
step r1_pin    { SELECT count(*) AS pinned_rows FROM upstream_iso_delete_retry_1; }
step r1_delete { DELETE FROM upstream_iso_delete_retry_1 WHERE a < 5; }
step r1_commit { COMMIT; }

session r2
step r2_begin  { BEGIN; }
step r2_pin    { SELECT count(*) AS pinned_rows FROM upstream_iso_delete_retry_2; }
step r2_delete { DELETE FROM upstream_iso_delete_retry_2 WHERE a < 5; }
step r2_commit { COMMIT; }

session checker
step check_rows {
  SELECT table_id, rows, low_rows, value_sum
    FROM (VALUES
      (0,
       (SELECT count(*) FROM upstream_iso_delete_retry_0),
       (SELECT count(*) FROM upstream_iso_delete_retry_0 WHERE a < 5),
       (SELECT sum(a) FROM upstream_iso_delete_retry_0)),
      (1,
       (SELECT count(*) FROM upstream_iso_delete_retry_1),
       (SELECT count(*) FROM upstream_iso_delete_retry_1 WHERE a < 5),
       (SELECT sum(a) FROM upstream_iso_delete_retry_1)),
      (2,
       (SELECT count(*) FROM upstream_iso_delete_retry_2),
       (SELECT count(*) FROM upstream_iso_delete_retry_2 WHERE a < 5),
       (SELECT sum(a) FROM upstream_iso_delete_retry_2))
    ) AS results(table_id, rows, low_rows, value_sum)
   ORDER BY table_id;
}

teardown {
  DROP TABLE IF EXISTS upstream_iso_delete_retry_0,
                       upstream_iso_delete_retry_1,
                       upstream_iso_delete_retry_2;
}

permutation r0_begin r0_pin r1_begin r1_pin r2_begin r2_pin r0_delete r1_delete r2_delete r0_commit r1_commit r2_commit check_rows
permutation r0_begin r0_pin r1_begin r1_pin r2_begin r2_pin r0_delete r1_delete r2_delete r2_commit r1_commit r0_commit check_rows
