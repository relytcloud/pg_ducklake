# Upstream: test/sql/rewrite_data_files/test_rewrite_transaction_conflict.test
# Delete and rewrite operations on the same files detect a commit conflict.
setup {
 CALL ducklake.set_option('data_inlining_row_limit', 0);
 CALL ducklake.set_option('rewrite_delete_threshold', 0);
 CREATE TABLE upstream_iso_rewrite_conflict (i integer) USING ducklake;
 INSERT INTO upstream_iso_rewrite_conflict SELECT i FROM generate_series(0, 99) AS g(i);
 DELETE FROM upstream_iso_rewrite_conflict WHERE i < 10;
}
session s1
step s1_begin { BEGIN; }
step s1_rewrite { SELECT * FROM ducklake.rewrite_data_files('upstream_iso_rewrite_conflict'::regclass); }
step s1_commit { COMMIT; }
session s2
step s2_begin { BEGIN; }
step s2_delete { DELETE FROM upstream_iso_rewrite_conflict WHERE i >= 30 AND i < 50; }
step s2_commit { COMMIT; }
session check_session
step check_rows { SELECT count(*), min(i), max(i) FROM upstream_iso_rewrite_conflict; }
teardown { DROP TABLE upstream_iso_rewrite_conflict; }
permutation s1_begin s2_begin s1_rewrite s2_delete s1_commit s2_commit check_rows
