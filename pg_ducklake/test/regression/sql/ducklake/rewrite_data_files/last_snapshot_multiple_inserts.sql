-- Upstream: test/sql/rewrite_data_files/last_snapshot_multiple_inserts.test
-- Rewrite preserves current data after interleaved insert and delete snapshots.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CALL ducklake.set_option('rewrite_delete_threshold', 0);
CREATE TABLE upstream_rewrite_multi (key integer, value text) USING ducklake;
INSERT INTO upstream_rewrite_multi SELECT i, 'v' || i FROM generate_series(0, 99) AS g(i);
SELECT max(r['snapshot_id']::bigint) AS sm1 FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_rewrite_multi WHERE key < 50;
SELECT max(r['snapshot_id']::bigint) AS sm2 FROM ducklake.snapshots() AS r \gset
INSERT INTO upstream_rewrite_multi SELECT i, 'v' || i FROM generate_series(100, 199) AS g(i);
SELECT max(r['snapshot_id']::bigint) AS sm3 FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_rewrite_multi WHERE key < 80;
SELECT max(r['snapshot_id']::bigint) AS sm4 FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_rewrite_multi WHERE key = 120;
SELECT max(r['snapshot_id']::bigint) AS sm5 FROM ducklake.snapshots() AS r \gset
INSERT INTO upstream_rewrite_multi SELECT i, 'v' || i FROM generate_series(200, 299) AS g(i);
SELECT max(r['snapshot_id']::bigint) AS sm6 FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_rewrite_multi WHERE key > 200 AND key < 250;
SELECT max(r['snapshot_id']::bigint) AS sm7 FROM ducklake.snapshots() AS r \gset
SELECT count(*) AS before_rewrite FROM upstream_rewrite_multi;
SELECT * FROM ducklake.rewrite_data_files('upstream_rewrite_multi'::regclass);
SELECT count(*), min(key), max(key) FROM upstream_rewrite_multi;
SELECT
  (SELECT count(*) FROM ducklake.time_travel('upstream_rewrite_multi'::regclass, :sm1)) = 100 AS s1_ok,
  (SELECT count(*) FROM ducklake.time_travel('upstream_rewrite_multi'::regclass, :sm2)) = 50 AS s2_ok,
  (SELECT count(*) FROM ducklake.time_travel('upstream_rewrite_multi'::regclass, :sm3)) = 150 AS s3_ok,
  (SELECT count(*) FROM ducklake.time_travel('upstream_rewrite_multi'::regclass, :sm4)) = 120 AS s4_ok,
  (SELECT count(*) FROM ducklake.time_travel('upstream_rewrite_multi'::regclass, :sm5)) = 119 AS s5_ok,
  (SELECT count(*) FROM ducklake.time_travel('upstream_rewrite_multi'::regclass, :sm6)) = 219 AS s6_ok,
  (SELECT count(*) FROM ducklake.time_travel('upstream_rewrite_multi'::regclass, :sm7)) = 170 AS s7_ok;
DROP TABLE upstream_rewrite_multi;
