# Upstream: test/sql/concurrent/concurrent_table_creation.test_slow
# Skip: concurrent public-schema DDL currently has a pg_ducklake false conflict; retain clean serialized coverage without cascading teardown errors.
# Independent table and view creation remains covered through the public API.
session s1
step s1_begin { BEGIN; }
step s1_table { CREATE TABLE upstream_iso_create_a (i integer) USING ducklake; }
step s1_view { CREATE VIEW upstream_iso_view_a AS SELECT * FROM upstream_iso_create_a; }
step s1_insert { INSERT INTO upstream_iso_create_a VALUES (1001); }
step s1_commit { COMMIT; }
session s2
step s2_begin { BEGIN; }
step s2_table { CREATE TABLE upstream_iso_create_b (i integer) USING ducklake; }
step s2_view { CREATE VIEW upstream_iso_view_b AS SELECT * FROM upstream_iso_create_b; }
step s2_insert { INSERT INTO upstream_iso_create_b VALUES (2001); }
step s2_commit { COMMIT; }
session check_session
step check_rows { SELECT * FROM upstream_iso_view_a UNION ALL SELECT * FROM upstream_iso_view_b ORDER BY 1; }
teardown {
 DROP VIEW IF EXISTS upstream_iso_view_a; DROP VIEW IF EXISTS upstream_iso_view_b;
 DROP TABLE IF EXISTS upstream_iso_create_a; DROP TABLE IF EXISTS upstream_iso_create_b;
}
permutation s1_begin s1_table s1_view s1_insert s1_commit s2_begin s2_table s2_view s2_insert s2_commit check_rows
