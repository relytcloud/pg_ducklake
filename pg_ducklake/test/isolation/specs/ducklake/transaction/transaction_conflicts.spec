# Upstream: test/sql/transaction/transaction_conflicts.test
# Skip: required DuckLake commit-time DDL/write conflicts currently surface early in PostgreSQL.
# Keep clean serialized baselines for same-name DDL, inserts, and drop/write order.

setup
{
  DROP TABLE IF EXISTS upstream_iso_conflict_same;
  DROP TABLE IF EXISTS upstream_iso_conflict_base;
}

session s1
step s1_begin       { BEGIN; }
step s1_create_same { CREATE TABLE upstream_iso_conflict_same (i integer) USING ducklake; }
step s1_insert      { INSERT INTO upstream_iso_conflict_base VALUES (1); }
step s1_drop        { DROP TABLE upstream_iso_conflict_base; }
step s1_commit      { COMMIT; }
step s1_drop_same   { DROP TABLE upstream_iso_conflict_same; }

session s2
step s2_begin       { BEGIN; }
step s2_create_same { CREATE TABLE upstream_iso_conflict_same (s text) USING ducklake; }
step s2_insert      { INSERT INTO upstream_iso_conflict_base VALUES (100); }
step s2_commit      { COMMIT; }

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

# Each same-name definition can be created after its predecessor is removed.
permutation s1_begin s1_create_same s1_commit check_same s1_drop_same s2_begin s2_create_same s2_commit check_same

# Both serialized inserts survive.
permutation create_base s1_begin s1_insert s1_commit s2_begin s2_insert s2_commit check_inserts drop_base

# A committed insert may be followed by a committed table drop.
permutation create_base s2_begin s2_insert s2_commit s1_begin s1_drop s1_commit check_dropped
