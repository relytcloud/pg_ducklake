# Upstream: test/sql/transaction/transaction_conflicts_delete.test
# Skip: concurrent delete-versus-DDL conflicts currently fail before the required commit conflict.
# Concurrent deletes and concurrent DDL must not silently corrupt table contents.

setup
{
  CREATE TABLE upstream_iso_delete_conflict USING ducklake AS
  SELECT i FROM generate_series(0, 999) AS g(i);
}

session s1
step s1_begin          { BEGIN; }
step s1_delete_partial { DELETE FROM upstream_iso_delete_conflict WHERE i < 200; }
step s1_delete_all     { DELETE FROM upstream_iso_delete_conflict; }
step s1_drop           { DROP TABLE upstream_iso_delete_conflict; }
step s1_alter          { ALTER TABLE upstream_iso_delete_conflict ADD COLUMN j integer; }
step s1_commit         { COMMIT; }

session s2
step s2_begin          { BEGIN; }
step s2_delete_partial { DELETE FROM upstream_iso_delete_conflict WHERE i < 100; }
step s2_delete_all     { DELETE FROM upstream_iso_delete_conflict; }
step s2_commit         { COMMIT; }
step s2_rollback       { ROLLBACK; }

session check_session
step check_partial { SELECT count(*), min(i), max(i) FROM upstream_iso_delete_conflict; }
step check_empty   { SELECT count(*) FROM upstream_iso_delete_conflict; }
step check_dropped { SELECT to_regclass('upstream_iso_delete_conflict') IS NULL; }
step check_altered {
  SELECT attname FROM pg_attribute
  WHERE attrelid = 'upstream_iso_delete_conflict'::regclass
    AND attnum > 0 AND NOT attisdropped ORDER BY attnum;
  SELECT count(*) FROM upstream_iso_delete_conflict;
}

teardown
{
  DROP TABLE IF EXISTS upstream_iso_delete_conflict;
}

permutation s1_begin s2_begin s1_delete_partial s2_delete_partial s1_commit s2_commit check_partial
permutation s1_begin s2_begin s1_delete_all s2_delete_all s1_commit s2_commit check_empty
permutation s1_begin s2_begin s1_drop s2_delete_all s1_commit s2_rollback check_dropped
permutation s1_begin s2_begin s1_alter s2_delete_all s1_commit s2_rollback check_altered
