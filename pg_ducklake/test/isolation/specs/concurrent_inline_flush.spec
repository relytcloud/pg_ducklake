# Inlined-data flush racing a concurrent fast-path (direct) insert.
# The insert commits a snapshot between the flush's snapshot pin and its
# commit, so the flush commit collides on the snapshot id and must retry;
# the retry parses the concurrent snapshot's changes_made.  No rows may be
# lost (issue #215).

setup
{
  CALL ducklake.set_option('data_inlining_row_limit', 100);
  CREATE TABLE iso_inline_flush_t (id int, val text) USING ducklake;
  INSERT INTO iso_inline_flush_t VALUES (1, 'a'), (2, 'b');
  INSERT INTO iso_inline_flush_t VALUES (3, 'c');
}

# separate block: the previous block is one implicit transaction, so the
# ducklake metadata written at its commit is only visible from here on
setup
{
  DO $$
  DECLARE tname text;
  BEGIN
    SELECT it.table_name INTO STRICT tname
    FROM ducklake.ducklake_inlined_data_tables it
    JOIN ducklake.ducklake_table t ON t.table_id = it.table_id
    WHERE t.table_name = 'iso_inline_flush_t' AND t.end_snapshot IS NULL
    ORDER BY it.schema_version DESC LIMIT 1;
    EXECUTE format('CREATE VIEW iso_inline_flush_inlined AS '
                   'SELECT row_id, end_snapshot IS NULL AS live, id, '
                   'convert_from(val, ''UTF8'') AS val FROM ducklake.%I', tname);
  END $$;
}

session s1
step s1_begin  { BEGIN; }
step s1_pin    { SELECT count(*) FROM iso_inline_flush_t; }
step s1_flush  { SELECT * FROM ducklake.flush_inlined_data('iso_inline_flush_t'::regclass); }
step s1_commit { COMMIT; }
step s1_count  { SELECT count(*) FROM iso_inline_flush_t; }
# metadata state: raw inlined-data rows and data files (snapshot ids omitted,
# they depend on the position in the schedule)
step s1_inlined {
  SELECT row_id, live, id, val FROM iso_inline_flush_inlined ORDER BY row_id;
}
step s1_files {
  SELECT ddf.record_count, ddf.end_snapshot IS NULL AS live
  FROM ducklake.ducklake_data_file ddf
  JOIN ducklake.ducklake_table dt ON ddf.table_id = dt.table_id
  WHERE dt.table_name = 'iso_inline_flush_t' AND dt.end_snapshot IS NULL
  ORDER BY ddf.data_file_id;
}

session s2
# autocommit: must not be in a transaction block so the direct-insert fast
# path engages and commits its own snapshot
step s2_insert { INSERT INTO iso_inline_flush_t VALUES (4, 'd'); }
step s2_count  { SELECT count(*) FROM iso_inline_flush_t; }

teardown
{
  DROP VIEW iso_inline_flush_inlined;
  DROP TABLE iso_inline_flush_t;
  CALL ducklake.set_option('data_inlining_row_limit', 0);
}

# Flush with no concurrency: sanity baseline -- all inlined rows move to one
# data file, the inlined table drains
permutation s1_begin s1_pin s1_flush s1_commit s1_count s1_inlined s1_files

# A direct insert commits after the flush transaction pinned its snapshot:
# the flush commit retries past the snapshot-id collision and both the
# flushed rows and the concurrently inserted row survive -- the setup rows
# land in a data file, the concurrent row stays inlined
permutation s1_begin s1_pin s2_insert s1_flush s1_commit s1_count s2_count s1_inlined s1_files
