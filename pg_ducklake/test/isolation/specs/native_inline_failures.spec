# A transaction holding the next snapshot claim forces a native writer to
# block after its one-time payload prewrite. This covers retry exhaustion and
# cancellation while blocked on the snapshot claim.

setup
{
  CALL ducklake.set_option('data_inlining_row_limit', 100);
  CREATE TABLE iso_native_failure_t (id int) USING ducklake;
}

setup
{
  SELECT count(*) FROM ducklake.ensure_inlined_data_table('iso_native_failure_t'::regclass);
  SELECT ducklake.reset_native_writer_stats();
}

setup
{
  DO $$
  DECLARE tname text;
  BEGIN
    SELECT it.table_name INTO STRICT tname
    FROM ducklake.ducklake_inlined_data_tables it
    JOIN ducklake.ducklake_table t USING (table_id)
    WHERE t.table_name = 'iso_native_failure_t' AND t.end_snapshot IS NULL
    ORDER BY it.schema_version DESC LIMIT 1;
    EXECUTE format('CREATE VIEW iso_native_failure_inlined AS '
                   'SELECT row_id, begin_snapshot, id FROM ducklake.%I', tname);
  END $$;
}

session holder
step holder_begin { BEGIN; }
step holder_claim {
  DO $$
  DECLARE claimed_snapshot bigint;
  BEGIN
    INSERT INTO ducklake.ducklake_snapshot
      (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
    SELECT snapshot_id + 1, now(), schema_version, next_catalog_id, next_file_id + 1
    FROM ducklake.ducklake_snapshot
    ORDER BY snapshot_id DESC LIMIT 1
    RETURNING snapshot_id INTO claimed_snapshot;

    INSERT INTO ducklake.ducklake_snapshot_changes
      (snapshot_id, changes_made, author, commit_message, commit_extra_info)
    VALUES (claimed_snapshot, '', NULL, NULL, NULL);
  END $$;
}
step holder_commit { COMMIT; }
step holder_rollback { ROLLBACK; }

session writer
step writer_name { SET application_name = 'iso_native_failure_writer'; }
step writer_exhaust { SET ducklake.native_writer_max_retry_count = 0; }
step writer_insert { INSERT INTO iso_native_failure_t VALUES (1); }

session control
step cancel_writer {
  SELECT pg_cancel_backend(pid)
  FROM pg_stat_activity
  WHERE application_name = 'iso_native_failure_writer';
}
step check_rollback {
  SELECT count(*) AS raw_rows FROM iso_native_failure_inlined;
  SELECT count(*) AS visible_rows FROM iso_native_failure_t;
}
step check_exhaustion {
  SELECT event, count
  FROM ducklake.native_writer_stats()
  ORDER BY event;
}
step check_blocked_cancel {
  SELECT event, count
  FROM ducklake.native_writer_stats()
  ORDER BY event;
}

teardown
{
  DROP VIEW iso_native_failure_inlined;
  DROP TABLE iso_native_failure_t;
  CALL ducklake.set_option('data_inlining_row_limit', 0);
}

# The failed statement rolls back the parent payload, but its nontransactional
# work counters retain the child claim conflict and exhaustion.
permutation holder_begin holder_claim writer_name writer_exhaust writer_insert holder_commit check_rollback check_exhaustion

# Cancellation while blocked on the uncommitted unique-index claim unwinds the
# publication subtransaction and the top-level payload.
permutation holder_begin holder_claim writer_name writer_insert cancel_writer holder_rollback check_rollback check_blocked_cancel
