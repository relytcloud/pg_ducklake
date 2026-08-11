-- Upstream: test/sql/stats/count_star_optimization_time_travel.test
-- Historical COUNT(*) uses the selected snapshot after later compaction.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_count_travel (i integer) USING ducklake;
INSERT INTO upstream_count_travel SELECT i FROM generate_series(0, 99) AS g(i);
SELECT max(snapshot_id) AS count_snapshot_1 FROM ducklake.ducklake_snapshot \gset
INSERT INTO upstream_count_travel SELECT i FROM generate_series(100, 199) AS g(i);
SELECT max(snapshot_id) AS count_snapshot_2 FROM ducklake.ducklake_snapshot \gset
INSERT INTO upstream_count_travel SELECT i FROM generate_series(200, 299) AS g(i);
SELECT * FROM ducklake.merge_adjacent_files('upstream_count_travel'::regclass);
SELECT count(*) FROM upstream_count_travel;
SELECT count(*) FROM ducklake.time_travel('upstream_count_travel', :count_snapshot_1);
SELECT count(*) FROM ducklake.time_travel('upstream_count_travel', :count_snapshot_2);
DROP TABLE upstream_count_travel;
