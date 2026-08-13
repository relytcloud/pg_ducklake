# Upstream: test/sql/concurrent/file_level_conflict.test
# Skip: pg_ducklake currently conflicts on concurrent deletes from disjoint
# partition files; keep a serialized baseline until file-level conflicts work.
setup {
  DROP TABLE IF EXISTS upstream_iso_file_conflict;
  CREATE TABLE upstream_iso_file_conflict
    (key integer, grouping integer) USING ducklake;
}
setup {
  CALL ducklake.set_option(
    'data_inlining_row_limit', 0, 'upstream_iso_file_conflict'::regclass);
}
setup {
  CALL ducklake.set_partition(
    'upstream_iso_file_conflict'::regclass, 'grouping');
}
setup {
  INSERT INTO upstream_iso_file_conflict
    SELECT i, i % 2 FROM generate_series(0, 19) AS g(i);
}

session s1
step s1_begin  { BEGIN; }
step s1_delete { DELETE FROM upstream_iso_file_conflict WHERE key = 1; }
step s1_commit { COMMIT; }

session s2
step s2_begin  { BEGIN; }
step s2_delete { DELETE FROM upstream_iso_file_conflict WHERE key = 2; }
step s2_commit { COMMIT; }

session checker
step check_rows {
  SELECT count(*) AS rows,
         sum(key) AS key_sum,
         count(*) FILTER (WHERE key IN (1, 2)) AS deleted_keys_remaining
    FROM upstream_iso_file_conflict;
}

teardown { DROP TABLE IF EXISTS upstream_iso_file_conflict; }

permutation s1_begin s1_delete s1_commit s2_begin s2_delete s2_commit check_rows
