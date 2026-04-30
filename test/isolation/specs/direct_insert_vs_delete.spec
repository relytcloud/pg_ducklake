# Direct insert (autocommit) racing with DELETE (DuckDB-path) on the same
# ducklake table.  DELETE goes through the DuckDB commit path and writes
# to the inlined deletion table; direct insert writes to the inlined data
# table with a fresh snapshot_id.  No row_id collision should occur
# within the direct insert's own range, and previously-deleted rows must
# stay deleted regardless of insert ordering.

setup
{
  CALL ducklake.set_option('data_inlining_row_limit', 1000);
  CREATE TABLE iso_di_del (id int, val text) USING ducklake;
}

session s1
# Seed has to live in a step (not setup) -- setup runs as a single
# transaction, and CALL ducklake.set_option already wrote to a Postgres
# metadata table there, so a DuckDB-path INSERT in the same block
# trips the "no mixed PG+DuckDB writes" guard.  Direct insert in an
# autocommit step is fine.
step s1_seed    { INSERT INTO iso_di_del VALUES (1, 'seed1'), (2, 'seed2'), (3, 'seed3'); }
step s1_begin   { BEGIN; }
step s1_delete  { DELETE FROM iso_di_del WHERE id IN (1, 2); }
step s1_commit  { COMMIT; }

session s2
step s2_di { INSERT INTO iso_di_del VALUES (10, 'new1'), (20, 'new2'); }

session checker
step check_count  { SELECT count(*) AS total FROM iso_di_del; }
step check_ids    { SELECT id FROM iso_di_del ORDER BY id; }

teardown
{
  DROP TABLE iso_di_del;
}

# Serial: delete commits first, then direct insert.  Final: id 3, 10, 20.
permutation s1_seed s1_begin s1_delete s1_commit s2_di check_count check_ids

# Direct insert before the delete: id 1 and 2 still get deleted; final: 3, 10, 20.
permutation s1_seed s2_di s1_begin s1_delete s1_commit check_count check_ids

# The interleaved case (delete txn open + direct insert fires before commit)
# trips a known cross-path limitation: DuckLake's commit cannot accept a
# snapshot_changes row with `inlined_data_insert` from another path.  That
# permutation lives in direct_insert_cross_path_xfail.spec.
