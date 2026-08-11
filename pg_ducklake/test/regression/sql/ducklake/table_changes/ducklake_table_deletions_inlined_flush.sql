-- Upstream: test/sql/table_changes/ducklake_table_deletions_inlined_flush.test
-- Skip: per-delete rowid/snapshot_id fields and a mixed inlined/file deletion range are not exposed reliably by the PostgreSQL API.

CREATE TABLE upstream_delete_inlined_flush (i integer, j text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_delete_inlined_flush'::regclass);
INSERT INTO upstream_delete_inlined_flush SELECT g, 'row' || g FROM generate_series(0, 9) AS g;
DELETE FROM upstream_delete_inlined_flush WHERE i IN (2, 5, 8);
SELECT max(r['snapshot_id']::bigint) AS spreflush_delete FROM ducklake.snapshots() AS r \gset
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_inlined_flush'::regclass);
DELETE FROM upstream_delete_inlined_flush WHERE i = 1;
SELECT max(r['snapshot_id']::bigint) AS sd1 FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_delete_inlined_flush WHERE i = 4;
SELECT max(r['snapshot_id']::bigint) AS sd2 FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_delete_inlined_flush WHERE i = 7;
SELECT max(r['snapshot_id']::bigint) AS sd3 FROM ducklake.snapshots() AS r \gset

SELECT * FROM upstream_delete_inlined_flush ORDER BY i;
SELECT r['i']::integer
FROM ducklake.table_deletions('upstream_delete_inlined_flush'::regclass, :sd1, :sd3) AS r
ORDER BY 1;
SELECT
  (SELECT count(*) FROM ducklake.table_deletions('upstream_delete_inlined_flush'::regclass, :spreflush_delete, :spreflush_delete)) = 3 AS preflush_deletes_ok,
  (SELECT count(*) FROM ducklake.table_deletions('upstream_delete_inlined_flush'::regclass, :sd1, :sd1)) = 1 AS first_file_delete_ok,
  (SELECT count(*) FROM ducklake.table_deletions('upstream_delete_inlined_flush'::regclass, :sd2, :sd2)) = 1 AS second_file_delete_ok,
  (SELECT count(*) FROM ducklake.table_deletions('upstream_delete_inlined_flush'::regclass, :sd3, :sd3)) = 1 AS third_file_delete_ok;
DROP TABLE upstream_delete_inlined_flush;
