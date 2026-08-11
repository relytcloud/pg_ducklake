# Upstream: test/sql/concurrent/concurrent_insert_delete_conflict.test
# Concurrent insert and delete metadata changes must not create duplicate rows.
setup {
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
session check_session
step check_rows {
 SELECT count(*), count(*) FILTER (WHERE key = 1), count(*) FILTER (WHERE key = 101),
        count(*) - count(DISTINCT key) AS duplicates
 FROM upstream_iso_insert_delete;
}
teardown { DROP TABLE upstream_iso_insert_delete; }
permutation s1_begin s2_begin s1_delete s2_insert s1_commit s2_commit check_rows
