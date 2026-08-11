-- Upstream: test/sql/rewrite_data_files/test_rewrite_merge_adjacent.test
-- Rewrite and adjacent-file merge can be chained without changing results.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CALL ducklake.set_option('rewrite_delete_threshold', 0);
CREATE TABLE upstream_rewrite_merge (a text, b integer) USING ducklake;
INSERT INTO upstream_rewrite_merge VALUES ('text', 1);
INSERT INTO upstream_rewrite_merge VALUES ('text2', 2);
INSERT INTO upstream_rewrite_merge SELECT 'text', i FROM generate_series(3, 1000) AS g(i);
DELETE FROM upstream_rewrite_merge WHERE b > 100;
SELECT * FROM ducklake.rewrite_data_files('upstream_rewrite_merge'::regclass);
SELECT * FROM ducklake.merge_adjacent_files('upstream_rewrite_merge'::regclass);
SELECT count(*), min(b), max(b) FROM upstream_rewrite_merge;
DROP TABLE upstream_rewrite_merge;
