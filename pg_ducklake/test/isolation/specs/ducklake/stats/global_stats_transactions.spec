# Upstream: test/sql/stats/global_stats_transactions.test
# Global min/max/null stats merge three concurrent inserts regardless of commit order.
setup {
  DROP TABLE IF EXISTS upstream_iso_global_stats;
  CREATE TABLE upstream_iso_global_stats (i integer) USING ducklake;
}
setup {
  CALL ducklake.set_option(
    'data_inlining_row_limit', 0, 'upstream_iso_global_stats'::regclass);
}

session s1
step s1_begin  { BEGIN; }
step s1_insert { INSERT INTO upstream_iso_global_stats VALUES (42); }
step s1_commit { COMMIT; }

session s2
step s2_begin  { BEGIN; }
step s2_insert { INSERT INTO upstream_iso_global_stats VALUES (84); }
step s2_commit { COMMIT; }

session s3
step s3_begin  { BEGIN; }
step s3_insert { INSERT INTO upstream_iso_global_stats VALUES (NULL); }
step s3_commit { COMMIT; }

session checker
step check_rows {
  SELECT count(*) AS rows,
         min(i) AS min_value,
         max(i) AS max_value,
         count(*) FILTER (WHERE i IS NULL) AS null_rows
    FROM upstream_iso_global_stats;
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
     AND t.table_name = 'upstream_iso_global_stats'
     AND t.end_snapshot IS NULL
     AND c.column_name = 'i'
     AND c.end_snapshot IS NULL;
}

teardown { DROP TABLE IF EXISTS upstream_iso_global_stats; }

permutation s1_begin s2_begin s3_begin s1_insert s2_insert s3_insert s1_commit s2_commit s3_commit check_rows check_stats
permutation s1_begin s2_begin s3_begin s1_insert s2_insert s3_insert s3_commit s2_commit s1_commit check_rows check_stats
