# XFAIL TRACKER: cross-path concurrency between direct insert and the
# DuckDB-path commit.
#
# Phase 1+2 made direct-insert-vs-direct-insert race-free.  The
# remaining hazard, which this spec documents, is that the upstream
# DuckLake commit (`DuckLakeTransaction::ExecuteCommit`) does not know
# how to apply a `ducklake_snapshot_changes.changes_made =
# 'inlined_data_insert'` row that some other path produced.  When a
# DuckDB-path commit is open and a direct insert lands in the gap, the
# DuckDB-path COMMIT throws:
#
#   ERROR: pg_ducklake commit hook failed to commit DuckDB:
#          ... TransactionContext Error: Failed to commit:
#          ... Unsupported change type inlined_data_insert
#
# The direct insert itself succeeds (autocommit); the DuckDB-path
# transaction is the loser.  Net data state is correct for the direct
# insert and rolled back for the DuckDB-path side -- nothing is
# corrupted, but the DuckDB-path's intent is silently dropped.
#
# Resolution requires either an upstream change to DuckLake's commit
# (recognise `inlined_data_insert` change rows) or pg_ducklake-side
# advisory-lock injection that serialises the two paths.  Both are
# out of scope for the initial pre-reserve PR.
#
# The expected output captures *current* (broken) behaviour so that:
#   1. CI runs are deterministic against the failure mode.
#   2. The day either fix lands, the expected output flips and these
#      permutations move to the must-pass specs.

setup
{
  CALL ducklake.set_option('data_inlining_row_limit', 1000);
  CREATE TABLE iso_di_xp (id int, val text) USING ducklake;
}

session s1
step s1_seed   { INSERT INTO iso_di_xp VALUES (1, 'seed1'), (2, 'seed2'), (3, 'seed3'); }
step s1_begin  { BEGIN; }
step s1_delete { DELETE FROM iso_di_xp WHERE id IN (1, 2); }
step s1_commit { COMMIT; }
# When s1_commit fails (the documented xfail), the session's transaction
# state is left aborted; an explicit ROLLBACK clears it.
step s1_rollback { ROLLBACK; }

session s2
step s2_di { INSERT INTO iso_di_xp VALUES (10, 'new1'), (20, 'new2'); }

session checker
step check_count { SELECT count(*) AS total, count(DISTINCT id) AS unique_ids FROM iso_di_xp; }
step check_ids   { SELECT id FROM iso_di_xp ORDER BY id; }

teardown
{
  DROP TABLE iso_di_xp;
}

# Direct insert lands inside an open DELETE transaction.  DELETE rolls
# back at COMMIT (Unsupported change type); direct insert's rows are
# committed.  Final state: the seeds (1, 2, 3) plus direct insert (10,
# 20) -- the DELETE intent is silently lost.  When the upstream issue
# is fixed, this expected output flips to "DELETE applied, then
# direct-insert rows visible -> 3, 10, 20".
permutation s1_seed s1_begin s1_delete s2_di s1_commit s1_rollback check_count check_ids
