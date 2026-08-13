# Upstream: test/sql/concurrent/concurrent_insert_delete_conflict.test
# Skip: commit conflict text exposes unstable internal table indexes; retain a
# serialized insert/delete baseline until conflicts have stable diagnostics.
setup {
 DROP TABLE IF EXISTS upstream_iso_insert_delete;
 CALL ducklake.set_option('data_inlining_row_limit', 0);
 CREATE TABLE upstream_iso_insert_delete (key integer) USING ducklake;
 INSERT INTO upstream_iso_insert_delete SELECT i FROM generate_series(0, 9) AS g(i);
}
session s1
step s1_begin { BEGIN; }
step s1_delete { DELETE FROM upstream_iso_insert_delete WHERE key = 1; }
step s1_commit { COMMIT; }
session s2
step s2_begin { BEGIN; }
step s2_insert { INSERT INTO upstream_iso_insert_delete VALUES (101); }
step s2_commit { COMMIT; }
teardown { ROLLBACK; }
session check_session
step check_rows {
 SELECT count(*), count(*) FILTER (WHERE key = 1), count(*) FILTER (WHERE key = 101),
        count(*) - count(DISTINCT key) AS duplicates
 FROM upstream_iso_insert_delete;
}
teardown {
 DROP TABLE IF EXISTS upstream_iso_insert_delete;
 CALL ducklake.set_option('data_inlining_row_limit', 0);
}
permutation s1_begin s1_delete s1_commit s2_begin s2_insert s2_commit check_rows
