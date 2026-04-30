# Direct insert (autocommit) racing with ALTER TABLE ADD COLUMN.  ALTER
# bumps the table's schema_version, which the direct insert captures at
# planning time.  Phase 2's snapshot reservation reads schema_version at
# the same instant it picks the snapshot_id, so the recorded snapshot is
# always self-consistent, regardless of which side wins.
#
# Key invariant: after both operations land, no row should be hidden by
# a stale schema_version on the snapshot row.

setup
{
  CALL ducklake.set_option('data_inlining_row_limit', 1000);
  CREATE TABLE iso_di_alt (id int, val text) USING ducklake;
}

session s1
# Pre-seed via direct insert (autocommit step); see direct_insert_vs_delete
# for the rationale (setup can't mix Postgres+DuckDB writes).
step s1_seed   { INSERT INTO iso_di_alt VALUES (1, 'pre'); }
step s1_begin  { BEGIN; }
step s1_alter  { ALTER TABLE iso_di_alt ADD COLUMN extra int DEFAULT 0; }
step s1_commit { COMMIT; }

session s2
step s2_di_old { INSERT INTO iso_di_alt VALUES (10, 'b1'), (20, 'b2'); }
step s2_di_new { INSERT INTO iso_di_alt(id, val, extra) VALUES (30, 'c1', 100); }

session checker
step check_count   { SELECT count(*) AS total FROM iso_di_alt; }
step check_ids     { SELECT id FROM iso_di_alt ORDER BY id; }

teardown
{
  DROP TABLE iso_di_alt;
}

# Serial: alter commits first, then direct insert with the new column.
permutation s1_seed s1_begin s1_alter s1_commit s2_di_new check_count check_ids

# Serial reverse: direct insert without the new column, then alter.
permutation s1_seed s2_di_old s1_begin s1_alter s1_commit check_count check_ids

# Interleaved: alter txn open, direct insert fires (sees the pre-alter
# schema), then alter commits.  Both writes must end up visible.
permutation s1_seed s1_begin s1_alter s2_di_old s1_commit check_count check_ids
