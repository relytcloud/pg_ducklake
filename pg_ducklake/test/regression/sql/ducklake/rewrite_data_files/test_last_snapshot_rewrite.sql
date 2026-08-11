-- Upstream: test/sql/rewrite_data_files/test_last_snapshot_rewrite.test
-- Multiple delete rewrites obey the catalog threshold while preserving snapshots.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_last_rewrite (key integer, value text) USING ducklake;
INSERT INTO upstream_last_rewrite SELECT i, 'v' || i FROM generate_series(0, 999) AS g(i);
SELECT max(snapshot_id) AS original_snapshot FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_last_rewrite WHERE key < 100;
DELETE FROM upstream_last_rewrite WHERE key > 900;
SELECT max(r['snapshot_id']::bigint) AS first_deleted_snapshot
FROM ducklake.snapshots() AS r \gset
-- The default-equivalent threshold must leave this partially deleted file untouched.
CALL ducklake.set_option('rewrite_delete_threshold', 0.95);
SELECT * FROM ducklake.rewrite_data_files('upstream_last_rewrite'::regclass);
CALL ducklake.set_option('rewrite_delete_threshold', 0);
SELECT * FROM ducklake.rewrite_data_files('upstream_last_rewrite'::regclass);
DELETE FROM upstream_last_rewrite WHERE key BETWEEN 400 AND 499;
SELECT max(r['snapshot_id']::bigint) AS second_deleted_snapshot
FROM ducklake.snapshots() AS r \gset
SELECT * FROM ducklake.rewrite_data_files('upstream_last_rewrite'::regclass);
SELECT count(*), min(key), max(key) FROM upstream_last_rewrite;
SELECT count(*) FROM ducklake.time_travel('upstream_last_rewrite', :original_snapshot);
SELECT count(*) = 801 AS first_history_ok
FROM ducklake.time_travel('upstream_last_rewrite'::regclass, :first_deleted_snapshot);
SELECT count(*) = 701 AS second_history_ok
FROM ducklake.time_travel('upstream_last_rewrite'::regclass, :second_deleted_snapshot);
DROP TABLE upstream_last_rewrite;
