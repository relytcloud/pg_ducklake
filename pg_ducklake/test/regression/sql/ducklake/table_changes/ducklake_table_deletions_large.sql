-- Upstream: test/sql/table_changes/ducklake_table_deletions_large.test
-- Preserve every large deletion snapshot and its cumulative feed cardinality.

CREATE TABLE upstream_delete_large (i integer) USING ducklake;
INSERT INTO upstream_delete_large SELECT g FROM generate_series(0, 9999) AS g;
DELETE FROM upstream_delete_large WHERE i >= 0 AND i < 1200;
SELECT max(r['snapshot_id']::bigint) AS sd1 FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_delete_large WHERE i >= 1200 AND i < 2400;
SELECT max(r['snapshot_id']::bigint) AS sd2 FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_delete_large WHERE i >= 2400 AND i < 3600;
SELECT max(r['snapshot_id']::bigint) AS sd3 FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_delete_large WHERE i >= 3600 AND i < 4800;
SELECT max(r['snapshot_id']::bigint) AS sd4 FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_delete_large WHERE i >= 4800 AND i < 6000;
SELECT max(r['snapshot_id']::bigint) AS sd5 FROM ducklake.snapshots() AS r \gset
SELECT count(*), min(i), max(i) FROM upstream_delete_large;
SELECT
  (SELECT count(*) FROM ducklake.table_deletions('upstream_delete_large'::regclass, :sd1, :sd1)) = 1200 AS d1_ok,
  (SELECT count(*) FROM ducklake.table_deletions('upstream_delete_large'::regclass, :sd2, :sd2)) = 1200 AS d2_ok,
  (SELECT count(*) FROM ducklake.table_deletions('upstream_delete_large'::regclass, :sd3, :sd3)) = 1200 AS d3_ok,
  (SELECT count(*) FROM ducklake.table_deletions('upstream_delete_large'::regclass, :sd4, :sd4)) = 1200 AS d4_ok,
  (SELECT count(*) FROM ducklake.table_deletions('upstream_delete_large'::regclass, :sd5, :sd5)) = 1200 AS d5_ok,
  (SELECT count(*) FROM ducklake.table_deletions('upstream_delete_large'::regclass, :sd1, :sd5)) = 6000 AS cumulative_ok;
DELETE FROM upstream_delete_large WHERE i >= 6010;
SELECT max(r['snapshot_id']::bigint) AS sd6 FROM ducklake.snapshots() AS r \gset
SELECT count(*) = 3990 AS final_delete_ok
FROM ducklake.table_deletions('upstream_delete_large'::regclass, :sd6, :sd6);
SELECT count(*) = 9990 AS complete_feed_ok
FROM ducklake.table_deletions('upstream_delete_large'::regclass, :sd1, :sd6);
SELECT count(*), min(i), max(i) FROM upstream_delete_large;
DROP TABLE upstream_delete_large;
