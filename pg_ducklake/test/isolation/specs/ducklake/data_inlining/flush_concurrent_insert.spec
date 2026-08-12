# Upstream: test/sql/data_inlining/flush_concurrent_insert.test
# A flush must not lose or duplicate a row committed after its snapshot was pinned.
setup {
  CREATE TABLE upstream_iso_inline_flush
    (id integer, val text) USING ducklake;
}
setup {
  CALL ducklake.set_option(
    'data_inlining_row_limit', 100, 'upstream_iso_inline_flush'::regclass);
}
setup {
  INSERT INTO upstream_iso_inline_flush
    VALUES (1, 'pre'), (2, 'pre'), (3, 'pre');
}

session flusher
step f_begin  { BEGIN; }
step f_pin    { SELECT count(*) AS pinned_rows FROM upstream_iso_inline_flush; }
step f_flush {
  SELECT count(*) > 0 AS flushed
    FROM ducklake.flush_inlined_data(
      'upstream_iso_inline_flush'::regclass);
}
step f_commit { COMMIT; }
step f_check {
  SELECT count(*) AS rows,
         count(DISTINCT id) AS distinct_ids,
         count(*) = count(DISTINCT id) AS no_duplicates
    FROM upstream_iso_inline_flush;
}

session inserter
step i_insert {
  INSERT INTO upstream_iso_inline_flush VALUES (4, 'concurrent');
}
step i_check {
  SELECT id, val FROM upstream_iso_inline_flush ORDER BY id;
}
step i_flush {
  SELECT count(*) > 0 AS flushed
    FROM ducklake.flush_inlined_data(
      'upstream_iso_inline_flush'::regclass);
}
step i_final {
  SELECT count(*) AS rows,
         count(DISTINCT id) AS distinct_ids,
         count(*) = count(DISTINCT id) AS no_duplicates,
         (SELECT count(*) > 0
            FROM ducklake.list_files(
              'upstream_iso_inline_flush'::regclass)) AS has_files
    FROM upstream_iso_inline_flush;
}

teardown { DROP TABLE upstream_iso_inline_flush; }

# The insert commits after the flush transaction pins its snapshot.
permutation f_begin f_pin i_insert f_flush f_commit f_check i_check i_flush i_final
# The flush writes first, but its transaction commits after the insert.
permutation f_begin f_pin f_flush i_insert f_commit f_check i_check i_flush i_final
