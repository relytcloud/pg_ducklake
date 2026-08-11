# Upstream: test/sql/data_inlining/flush_concurrent_insert.test
# A flush must not lose or duplicate a row committed after its snapshot was pinned.
setup
{
  CALL ducklake.set_option('data_inlining_row_limit', 100);
  CREATE TABLE upstream_iso_inline_flush (id integer, val text) USING ducklake;
  INSERT INTO upstream_iso_inline_flush VALUES (1,'pre'), (2,'pre'), (3,'pre');
}

session flusher
step f_begin  { BEGIN; }
step f_pin    { SELECT count(*) FROM upstream_iso_inline_flush; }
step f_flush  { SELECT count(*) FROM ducklake.flush_inlined_data('upstream_iso_inline_flush'::regclass); }
step f_commit { COMMIT; }
step f_check  {
  SELECT count(*) AS rows, count(DISTINCT id) AS distinct_ids,
         count(*) = count(DISTINCT id) AS no_duplicates
  FROM upstream_iso_inline_flush;
}

session inserter
step i_insert { INSERT INTO upstream_iso_inline_flush VALUES (4, 'concurrent'); }
step i_check  { SELECT id, val FROM upstream_iso_inline_flush ORDER BY id; }
step i_flush  { SELECT count(*) AS flushed FROM ducklake.flush_inlined_data('upstream_iso_inline_flush'::regclass); }
step i_final  {
  SELECT count(*) AS rows, count(DISTINCT id) AS distinct_ids,
         (SELECT count(*)
          FROM ducklake.ducklake_data_file f
          JOIN ducklake.ducklake_table t USING (table_id)
          WHERE t.table_name = 'upstream_iso_inline_flush'
            AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL) AS active_files
  FROM upstream_iso_inline_flush;
}

teardown
{
  DROP TABLE upstream_iso_inline_flush;
  CALL ducklake.set_option('data_inlining_row_limit', 0);
}

permutation f_begin f_pin i_insert f_flush f_commit f_check i_check i_flush i_final
permutation f_begin f_pin f_flush i_insert f_commit f_check i_check i_flush i_final
