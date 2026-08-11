# Upstream: test/sql/transaction/transaction_conflicts.test
# Skip: required DuckLake commit-time DDL/write conflicts currently surface early in PostgreSQL.
# Preserve representative same-name, concurrent-insert, and drop/write conflicts.

session s1
step s1_begin       { BEGIN; }
step s1_create_same { CREATE TABLE upstream_iso_conflict_same (i integer) USING ducklake; }
step s1_insert      { INSERT INTO upstream_iso_conflict_base VALUES (1); }
step s1_drop        { DROP TABLE upstream_iso_conflict_base; }
step s1_commit      { COMMIT; }
step s1_rollback    { ROLLBACK; }

session s2
step s2_begin       { BEGIN; }
step s2_create_same { CREATE TABLE upstream_iso_conflict_same (s text) USING ducklake; }
step s2_insert      { INSERT INTO upstream_iso_conflict_base VALUES (100); }
step s2_commit      { COMMIT; }
step s2_rollback    { ROLLBACK; }

session check_session
step create_base { CREATE TABLE upstream_iso_conflict_base (i integer) USING ducklake; }
step check_same {
  SELECT count(*) FROM pg_attribute
  WHERE attrelid = 'upstream_iso_conflict_same'::regclass
    AND attnum > 0 AND NOT attisdropped;
}
step check_inserts { SELECT * FROM upstream_iso_conflict_base ORDER BY i; }
step check_dropped { SELECT to_regclass('upstream_iso_conflict_base') IS NULL; }
step drop_base { DROP TABLE upstream_iso_conflict_base; }

teardown
{
  DROP TABLE IF EXISTS upstream_iso_conflict_same;
  DROP TABLE IF EXISTS upstream_iso_conflict_base;
}

# Creating one relation name concurrently leaves exactly one definition.
permutation s1_begin s2_begin s1_create_same s2_create_same s1_commit s2_rollback check_same

# Concurrent inserts into the same table both survive.
permutation create_base s1_begin s2_begin s1_insert s2_insert s1_commit s2_commit check_inserts drop_base

# An insert cannot commit against a table concurrently dropped by another transaction.
permutation create_base s1_begin s2_begin s1_drop s2_insert s1_commit s2_rollback check_dropped
