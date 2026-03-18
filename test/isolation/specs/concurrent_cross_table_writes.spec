# Concurrent writes to different DuckLake tables must not interfere.
# Reproduces a bug where concurrent DELETE+INSERT on separate tables
# caused duplicate rows due to snapshot/metadata corruption.

# Each DuckLake DDL/DML must be a separate autocommit step because
# pg_duckdb disallows mixing DuckDB and Postgres writes in one transaction.

session s1
step s1_create_a { CREATE TABLE iso_cross_tbl_a (id int, val text) USING ducklake; }
step s1_seed_a   { INSERT INTO iso_cross_tbl_a SELECT g, 'a_' || g FROM generate_series(1, 100) g; }
step s1_begin    { BEGIN; }
step s1_delete_a { DELETE FROM iso_cross_tbl_a WHERE id <= 50; }
step s1_insert_a { INSERT INTO iso_cross_tbl_a SELECT g, 'updated_' || g FROM generate_series(1, 50) g; }
step s1_commit   { COMMIT; }
step s1_verify_a { SELECT count(*) AS a_total FROM iso_cross_tbl_a; }
step s1_drop_a   { DROP TABLE IF EXISTS iso_cross_tbl_a CASCADE; }

session s2
step s2_create_b { CREATE TABLE iso_cross_tbl_b (id int, num int) USING ducklake; }
step s2_seed_b   { INSERT INTO iso_cross_tbl_b SELECT g, g * 10 FROM generate_series(1, 100) g; }
step s2_begin    { BEGIN; }
step s2_delete_b { DELETE FROM iso_cross_tbl_b WHERE id = 1; }
step s2_insert_b { INSERT INTO iso_cross_tbl_b VALUES (1, -1); }
step s2_commit   { COMMIT; }
step s2_verify_b { SELECT count(*) AS b_id1_count FROM iso_cross_tbl_b WHERE id = 1; }
step s2_verify_total { SELECT count(*) AS b_total FROM iso_cross_tbl_b; }
step s2_drop_b   { DROP TABLE IF EXISTS iso_cross_tbl_b CASCADE; }

# Serial: s1 completes before s2 begins -- no concurrency, both succeed
permutation s1_create_a s2_create_b s1_seed_a s2_seed_b s1_begin s1_delete_a s1_insert_a s1_commit s2_begin s2_delete_b s2_insert_b s2_commit s1_verify_a s2_verify_b s2_verify_total s1_drop_a s2_drop_b

# Concurrent writes to different tables: both open, interleaved ops, s1 commits first
# Table B must not get duplicate rows for id=1
permutation s1_create_a s2_create_b s1_seed_a s2_seed_b s1_begin s2_begin s1_delete_a s2_delete_b s1_insert_a s2_insert_b s1_commit s2_commit s1_verify_a s2_verify_b s2_verify_total s1_drop_a s2_drop_b

# Concurrent writes to different tables: both open, interleaved ops, s2 commits first
# Table B must not get duplicate rows for id=1
permutation s1_create_a s2_create_b s1_seed_a s2_seed_b s1_begin s2_begin s1_delete_a s2_delete_b s1_insert_a s2_insert_b s2_commit s1_commit s1_verify_a s2_verify_b s2_verify_total s1_drop_a s2_drop_b
