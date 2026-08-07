# One native writer prewrites a candidate, then loses two consecutive snapshot
# claims to writers that start after it. Advisory locks in a snapshot trigger
# hold that writer at each exact claim until the scheduled winner commits.

setup
{
  CALL ducklake.set_option('data_inlining_row_limit', 100);
  SELECT ducklake.reset_direct_insert_stats();
  SELECT ducklake.reset_native_writer_stats();
  CREATE TABLE iso_native_retry_t (id int, writer text) USING ducklake;
}

setup
{
  SELECT count(*) FROM ducklake.ensure_inlined_data_table('iso_native_retry_t'::regclass);
  CREATE TABLE iso_native_retry_claim (first_snapshot bigint NOT NULL);
  INSERT INTO iso_native_retry_claim
    SELECT max(snapshot_id) + 1 FROM ducklake.ducklake_snapshot;

  CREATE FUNCTION iso_block_native_retry_claim() RETURNS trigger
  LANGUAGE plpgsql AS $$
  DECLARE first_claim bigint;
  BEGIN
    IF current_setting('application_name') = 'iso_native_retry_loser' THEN
      SELECT first_snapshot INTO STRICT first_claim FROM iso_native_retry_claim;
      IF NEW.snapshot_id = first_claim THEN
        PERFORM pg_advisory_xact_lock(9021001);
      ELSIF NEW.snapshot_id = first_claim + 1 THEN
        PERFORM pg_advisory_xact_lock(9021002);
      END IF;
    END IF;
    RETURN NEW;
  END $$;

  CREATE TRIGGER iso_block_native_retry_claim
    BEFORE INSERT ON ducklake.ducklake_snapshot
    FOR EACH ROW EXECUTE FUNCTION iso_block_native_retry_claim();
}

setup
{
  DO $$
  DECLARE tname text;
  BEGIN
    SELECT it.table_name INTO STRICT tname
    FROM ducklake.ducklake_inlined_data_tables it
    JOIN ducklake.ducklake_table t USING (table_id)
    WHERE t.table_name = 'iso_native_retry_t' AND t.end_snapshot IS NULL
    ORDER BY it.schema_version DESC LIMIT 1;
    EXECUTE format('CREATE VIEW iso_native_retry_inlined AS '
                   'SELECT row_id, begin_snapshot, id, '
                   'convert_from(writer, ''UTF8'') AS writer FROM ducklake.%I', tname);
  END $$;
}

session blocker
step blocker_lock {
  SELECT pg_advisory_lock(9021001), pg_advisory_lock(9021002);
}
step blocker_unlock_first { SELECT pg_advisory_unlock(9021001); }
step blocker_unlock_second { SELECT pg_advisory_unlock(9021002); }

session loser
step loser_name { SET application_name = 'iso_native_retry_loser'; }
step loser_no_wait { SET ducklake.native_writer_retry_wait_ms = '0ms'; }
step loser_insert { INSERT INTO iso_native_retry_t VALUES (1, 'loser'), (2, 'loser'); }

session winner1
step winner1_insert { INSERT INTO iso_native_retry_t VALUES (3, 'winner1'), (4, 'winner1'); }

session winner2
step winner2_insert { INSERT INTO iso_native_retry_t VALUES (5, 'winner2'), (6, 'winner2'); }

session check_session
step check_rows {
  SELECT id, writer
  FROM iso_native_retry_inlined
  ORDER BY row_id;
}
step check_metadata {
  SELECT count(*) AS rows, count(DISTINCT row_id) AS row_ids,
         count(DISTINCT begin_snapshot) AS snapshots
  FROM iso_native_retry_inlined;

  SELECT writer, count(*) AS rows,
         max(row_id) - min(row_id) + 1 = count(*) AS contiguous,
         count(DISTINCT begin_snapshot) AS snapshots
  FROM iso_native_retry_inlined
  GROUP BY writer
  ORDER BY writer;
}
step check_path {
  SELECT pattern, reason, count
  FROM ducklake.direct_insert_stats()
  WHERE pattern = 'matched_values';
}
step check_writer_stats {
  SELECT event, count
  FROM ducklake.native_writer_stats()
  ORDER BY event;
}
step check_stats {
  SELECT s.record_count, s.next_row_id - min(i.row_id) AS allocated_rows
  FROM ducklake.ducklake_table_stats s
  JOIN ducklake.ducklake_table t USING (table_id)
  CROSS JOIN iso_native_retry_inlined i
  WHERE t.table_name = 'iso_native_retry_t' AND t.end_snapshot IS NULL
  GROUP BY s.record_count, s.next_row_id;
}

teardown
{
  SELECT pg_advisory_unlock_all();
  DROP VIEW iso_native_retry_inlined;
  DROP TABLE iso_native_retry_claim;
  DROP TRIGGER iso_block_native_retry_claim ON ducklake.ducklake_snapshot;
  DROP FUNCTION iso_block_native_retry_claim();
  DROP TABLE iso_native_retry_t;
  CALL ducklake.set_option('data_inlining_row_limit', 0);
}

permutation blocker_lock loser_name loser_no_wait loser_insert winner1_insert blocker_unlock_first winner2_insert blocker_unlock_second check_rows check_metadata check_path check_writer_stats check_stats
