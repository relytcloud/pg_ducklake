# Upstream: test/sql/concurrent/concurrent_insert_data_inlining.test
# Concurrent inserts into inlined data merge rows and global stats.
setup {
  DROP TABLE IF EXISTS upstream_iso_inline_insert;
  CREATE TABLE upstream_iso_inline_insert (key integer) USING ducklake;
}
setup {
  CALL ducklake.set_option(
    'data_inlining_row_limit', 10, 'upstream_iso_inline_insert'::regclass);
}

session s1
step s1_begin  { BEGIN; }
step s1_insert { INSERT INTO upstream_iso_inline_insert VALUES (0); }
step s1_commit { COMMIT; }

session s2
step s2_begin  { BEGIN; }
step s2_insert { INSERT INTO upstream_iso_inline_insert VALUES (1); }
step s2_commit { COMMIT; }

session checker
step check_rows {
  SELECT count(*) AS rows, sum(key) AS key_sum,
         min(key) AS min_key, max(key) AS max_key,
         count(*) FILTER (WHERE key IS NULL) AS null_keys
    FROM upstream_iso_inline_insert;
}
# PostgreSQL has no stats(column) display function; assert its backing
# DuckLake global-stat row while keeping the result cardinality deterministic.
step check_stats {
  SELECT count(*) AS stat_rows,
         min(s.min_value) AS min_value,
         max(s.max_value) AS max_value,
         bool_or(s.contains_null) AS contains_null
    FROM ducklake.ducklake_table_column_stats s
    JOIN ducklake.ducklake_table t USING (table_id)
    JOIN ducklake.ducklake_schema sch USING (schema_id)
    JOIN ducklake.ducklake_column c USING (table_id, column_id)
   WHERE sch.schema_name = 'public'
     AND t.table_name = 'upstream_iso_inline_insert'
     AND t.end_snapshot IS NULL
     AND c.column_name = 'key'
     AND c.end_snapshot IS NULL;
}

teardown { DROP TABLE IF EXISTS upstream_iso_inline_insert; }

permutation s1_begin s2_begin s1_insert s2_insert s1_commit s2_commit check_rows check_stats
permutation s1_begin s2_begin s1_insert s2_insert s2_commit s1_commit check_rows check_stats
