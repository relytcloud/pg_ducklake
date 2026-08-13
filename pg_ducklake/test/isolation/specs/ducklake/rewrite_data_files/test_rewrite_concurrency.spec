# Upstream: test/sql/rewrite_data_files/test_rewrite_concurrency.test
# Skip: commit conflict text exposes unstable internal table indexes; retain a
# serialized rewrite baseline until conflicts have stable diagnostics.
setup {
 DROP TABLE IF EXISTS upstream_iso_rewrite;
 CALL ducklake.set_option('data_inlining_row_limit', 0);
 CALL ducklake.set_option('rewrite_delete_threshold', 0);
 CREATE TABLE upstream_iso_rewrite (key integer, value text) USING ducklake;
 INSERT INTO upstream_iso_rewrite SELECT i, 'v' || i FROM generate_series(0, 99) AS g(i);
 DELETE FROM upstream_iso_rewrite WHERE key < 39 OR key > 41;
}
session s1
step s1_begin { BEGIN; }
step s1_rewrite { SELECT * FROM ducklake.rewrite_data_files('upstream_iso_rewrite'::regclass); }
step s1_commit { COMMIT; }
session s2
step s2_begin { BEGIN; }
step s2_rewrite { SELECT * FROM ducklake.rewrite_data_files('upstream_iso_rewrite'::regclass); }
step s2_commit { COMMIT; }
teardown { ROLLBACK; }
session check_session
step check_rows { SELECT * FROM upstream_iso_rewrite ORDER BY key; }
teardown {
 DROP TABLE IF EXISTS upstream_iso_rewrite;
 CALL ducklake.set_option('data_inlining_row_limit', 0);
 CALL ducklake.set_option('rewrite_delete_threshold', 0.95);
}
permutation s1_begin s1_rewrite s1_commit s2_begin s2_rewrite s2_commit check_rows
