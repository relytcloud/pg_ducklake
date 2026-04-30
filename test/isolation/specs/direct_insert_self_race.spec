# Concurrent direct inserts into the same ducklake table must produce
# distinct snapshots and non-overlapping row_id ranges.  The advisory
# lock taken inside DirectInsertReservation serializes concurrent
# reservers without needing a UNIQUE constraint on ducklake_table_stats.
#
# Verification has two layers:
#   - check_user_count   -- user-visible row count and id uniqueness
#   - check_storage_rows -- storage-level row_id uniqueness in the
#                           inlined data table.  This is the property
#                           that the row_id race fix actually enforces;
#                           a regression that re-introduces overlapping
#                           ranges would surface here even though
#                           user-visible queries might still look fine.

setup
{
  CALL ducklake.set_option('data_inlining_row_limit', 1000);
  CREATE TABLE iso_di_self (id int, val text) USING ducklake;

  -- Helper that resolves the dynamic inlined-data table name and
  -- reports row_id uniqueness; used by check_storage_rows below.
  CREATE FUNCTION iso_di_self_storage_check(table_name text)
  RETURNS TABLE (row_count bigint, distinct_row_ids bigint)
  LANGUAGE plpgsql AS $fn$
  DECLARE
    tbl_id bigint;
    sv bigint;
    q text;
  BEGIN
    SELECT t.table_id, idt.schema_version
      INTO tbl_id, sv
      FROM ducklake.ducklake_table t
      JOIN ducklake.ducklake_inlined_data_tables idt ON idt.table_id = t.table_id
     WHERE t.table_name = $1 AND t.end_snapshot IS NULL
     LIMIT 1;

    q := format('SELECT count(*)::bigint, count(DISTINCT row_id)::bigint '
                'FROM ducklake.ducklake_inlined_data_%s_%s',
                tbl_id, sv);
    RETURN QUERY EXECUTE q;
  END;
  $fn$;
}

session s1
# Hold the same advisory lock that direct insert acquires inside
# DirectInsertReservation.  Source the namespace key from
# ducklake.direct_insert_lock_ns() rather than hardcoding the literal,
# so any future change to the C constant is picked up automatically.
step s1_hold_lock {
  BEGIN;
  SELECT pg_advisory_xact_lock(
           ducklake.direct_insert_lock_ns(),
           (SELECT table_id FROM ducklake.ducklake_table
             WHERE table_name = 'iso_di_self' AND end_snapshot IS NULL)::int4
         );
}
step s1_release { COMMIT; }
step s1_di { INSERT INTO iso_di_self VALUES (1, 'a1'), (2, 'a2'), (3, 'a3'); }

session s2
step s2_di { INSERT INTO iso_di_self VALUES (10, 'b1'), (20, 'b2'); }

session checker
step check_user_count {
  SELECT count(*) AS total, count(DISTINCT id) AS unique_ids FROM iso_di_self;
}
step check_select { SELECT id FROM iso_di_self ORDER BY id; }
# Asserts row_count == distinct_row_ids in the inlined data table.
step check_storage_rows { SELECT * FROM iso_di_self_storage_check('iso_di_self'); }

teardown
{
  DROP TABLE iso_di_self;
  DROP FUNCTION iso_di_self_storage_check(text);
}

# Serial direct inserts: both succeed immediately.  No contention.
permutation s1_di s2_di check_user_count check_select check_storage_rows

# Lock-induced interleaving: s2's direct insert blocks on the advisory
# lock held by s1.  s1 commits (releases the lock), s2 then completes,
# and s1 fires its own direct insert.  Total still 5 rows; advisory lock
# ensured the reservation phases were strictly serial so no row_id
# collision could happen.
permutation s1_hold_lock s2_di s1_release s1_di check_user_count check_select check_storage_rows
