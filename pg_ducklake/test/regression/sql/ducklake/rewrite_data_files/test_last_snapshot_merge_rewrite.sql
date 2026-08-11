-- Upstream: test/sql/rewrite_data_files/test_last_snapshot_merge_rewrite.test
-- Merge followed by delete rewrite preserves current and historical row sets.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CALL ducklake.set_option('rewrite_delete_threshold', 0);
CREATE TABLE upstream_merge_rewrite (key integer, value text) USING ducklake;
INSERT INTO upstream_merge_rewrite SELECT i, 'v' || i FROM generate_series(0, 99) AS g(i);
SELECT max(snapshot_id) AS initial_snapshot FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_merge_rewrite WHERE key < 50;
INSERT INTO upstream_merge_rewrite SELECT i, 'v' || i FROM generate_series(100, 199) AS g(i);
DELETE FROM upstream_merge_rewrite WHERE key = 120;
SELECT * FROM ducklake.merge_adjacent_files('upstream_merge_rewrite'::regclass);
SELECT * FROM ducklake.rewrite_data_files('upstream_merge_rewrite'::regclass);
SELECT count(*) FROM upstream_merge_rewrite;
SELECT count(*) FROM ducklake.time_travel('upstream_merge_rewrite', :initial_snapshot);
DROP TABLE upstream_merge_rewrite;
