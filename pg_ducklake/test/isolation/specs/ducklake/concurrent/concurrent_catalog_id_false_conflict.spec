# Upstream: test/sql/concurrent/concurrent_catalog_id_false_conflict.test
# Independent catalog creates must not conflict because an earlier catalog id was reused.
setup { CREATE TABLE upstream_iso_catalog_seed (x integer) USING ducklake; }
session s1
step s1_begin { BEGIN; }
step s1_read { SELECT * FROM upstream_iso_catalog_seed; }
step s1_create { CREATE TABLE upstream_iso_catalog_a (x integer) USING ducklake; }
step s1_commit { COMMIT; }
step s1_drop { DROP TABLE upstream_iso_catalog_a; }
session s2
step s2_begin { BEGIN; }
step s2_read { SELECT * FROM upstream_iso_catalog_seed; }
step s2_create { CREATE TABLE upstream_iso_catalog_b (x integer) USING ducklake; }
step s2_commit { COMMIT; }
teardown {
 DROP TABLE IF EXISTS upstream_iso_catalog_a;
 DROP TABLE IF EXISTS upstream_iso_catalog_b;
 DROP TABLE upstream_iso_catalog_seed;
}
permutation s1_begin s2_begin s1_read s2_read s1_create s2_create s1_commit s1_drop s2_commit
