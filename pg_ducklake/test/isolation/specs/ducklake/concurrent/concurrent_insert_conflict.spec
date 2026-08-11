# Upstream: test/sql/concurrent/concurrent_insert_conflict.test
# Concurrent inserts retry metadata commits without losing either row.
setup { CREATE TABLE upstream_iso_insert (key integer) USING ducklake; }
session s1
step s1_begin { BEGIN; }
step s1_insert { INSERT INTO upstream_iso_insert VALUES (1); }
step s1_commit { COMMIT; }
session s2
step s2_begin { BEGIN; }
step s2_insert { INSERT INTO upstream_iso_insert VALUES (2); }
step s2_commit { COMMIT; }
session check_session
step check_rows { SELECT count(*), sum(key) FROM upstream_iso_insert; }
teardown { DROP TABLE upstream_iso_insert; }
permutation s1_begin s2_begin s1_insert s2_insert s1_commit s2_commit check_rows
permutation s1_begin s2_begin s1_insert s2_insert s2_commit s1_commit check_rows
