# Upstream: test/sql/stats/global_stats_transactions.test
# Global min/max/null stats merge concurrent inserts.
setup { CREATE TABLE upstream_iso_global_stats (i integer) USING ducklake; }
session s1
step s1_begin { BEGIN; }
step s1_insert { INSERT INTO upstream_iso_global_stats VALUES (42); }
step s1_commit { COMMIT; }
session s2
step s2_begin { BEGIN; }
step s2_insert { INSERT INTO upstream_iso_global_stats VALUES (84), (NULL); }
step s2_commit { COMMIT; }
session check_session
step check_rows { SELECT count(*), min(i), max(i), count(*) FILTER (WHERE i IS NULL) FROM upstream_iso_global_stats; }
step check_stats {
 SELECT s.min_value, s.max_value, s.contains_null
 FROM ducklake.ducklake_table_column_stats s
 JOIN ducklake.ducklake_table t USING (table_id)
 JOIN ducklake.ducklake_column c USING (table_id, column_id)
 WHERE t.table_name = 'upstream_iso_global_stats' AND t.end_snapshot IS NULL
   AND c.column_name = 'i' AND c.end_snapshot IS NULL;
}
teardown { DROP TABLE upstream_iso_global_stats; }
permutation s1_begin s2_begin s1_insert s2_insert s1_commit s2_commit check_rows check_stats
permutation s1_begin s2_begin s1_insert s2_insert s2_commit s1_commit check_rows check_stats
