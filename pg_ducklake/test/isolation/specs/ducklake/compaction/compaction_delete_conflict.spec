# Upstream: test/sql/compaction/compaction_delete_conflict.test
# Adjacent-file compaction conflicts safely with a concurrent delete of the compacted files.
setup {
 DROP TABLE IF EXISTS upstream_iso_compact_delete;
 CALL ducklake.set_option('data_inlining_row_limit', 0);
 CREATE TABLE upstream_iso_compact_delete (i integer) USING ducklake;
 INSERT INTO upstream_iso_compact_delete SELECT i FROM generate_series(0, 99) AS g(i);
 INSERT INTO upstream_iso_compact_delete SELECT i FROM generate_series(100, 199) AS g(i);
}
session s1
step s1_begin { BEGIN; }
step s1_compact { SELECT * FROM ducklake.merge_adjacent_files('upstream_iso_compact_delete'::regclass); }
step s1_commit { COMMIT; }
session s2
step s2_begin { BEGIN; }
step s2_delete { DELETE FROM upstream_iso_compact_delete WHERE i < 50; }
step s2_commit { COMMIT; }
teardown { ROLLBACK; }
session check_session
step check_rows { SELECT count(*), min(i), max(i) FROM upstream_iso_compact_delete; }
teardown {
 DROP TABLE IF EXISTS upstream_iso_compact_delete;
 CALL ducklake.set_option('data_inlining_row_limit', 0);
}
permutation s1_begin s2_begin s1_compact s2_delete s1_commit s2_commit check_rows
