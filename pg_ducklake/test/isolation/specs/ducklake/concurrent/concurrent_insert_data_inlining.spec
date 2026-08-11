# Upstream: test/sql/concurrent/concurrent_insert_data_inlining.test
# Concurrent inserts into inlined data merge rows and global stats.
setup {
 CALL ducklake.set_option('data_inlining_row_limit', 10);
 CREATE TABLE upstream_iso_inline_insert (key integer) USING ducklake;
}
session s1
step s1_begin { BEGIN; }
step s1_insert { INSERT INTO upstream_iso_inline_insert VALUES (1); }
step s1_commit { COMMIT; }
session s2
step s2_begin { BEGIN; }
step s2_insert { INSERT INTO upstream_iso_inline_insert VALUES (2); }
step s2_commit { COMMIT; }
session check_session
step check_rows {
 SELECT count(*) AS rows, sum(key) AS key_sum,
        min(key) AS min_key, max(key) AS max_key,
        count(*) FILTER (WHERE key IS NULL) AS null_keys,
        (SELECT count(*)
         FROM ducklake.ducklake_data_file f
         JOIN ducklake.ducklake_table t USING (table_id)
         WHERE t.table_name = 'upstream_iso_inline_insert'
           AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL) AS active_files
 FROM upstream_iso_inline_insert;
}
step check_stats {
 SELECT s.min_value, s.max_value, s.contains_null
 FROM ducklake.ducklake_table_column_stats s
 JOIN ducklake.ducklake_table t USING (table_id)
 JOIN ducklake.ducklake_column c USING (table_id, column_id)
 WHERE t.table_name = 'upstream_iso_inline_insert' AND t.end_snapshot IS NULL
   AND c.column_name = 'key' AND c.end_snapshot IS NULL;
}
teardown {
 DROP TABLE upstream_iso_inline_insert;
 CALL ducklake.set_option('data_inlining_row_limit', 0);
}
permutation s1_begin s2_begin s1_insert s2_insert s1_commit s2_commit check_rows check_stats
permutation s1_begin s2_begin s1_insert s2_insert s2_commit s1_commit check_rows check_stats
