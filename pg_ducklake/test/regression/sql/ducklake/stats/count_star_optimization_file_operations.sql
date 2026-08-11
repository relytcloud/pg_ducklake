-- Upstream: test/sql/stats/count_star_optimization_file_operations.test
-- File maintenance operations retain the metadata count used by COUNT(*).
CALL ducklake.set_option('data_inlining_row_limit', 0);
CALL ducklake.set_option('rewrite_delete_threshold', 0);
CREATE TABLE upstream_count_files (i integer) USING ducklake;
INSERT INTO upstream_count_files SELECT i FROM generate_series(0, 99) AS g(i);
INSERT INTO upstream_count_files SELECT i FROM generate_series(100, 199) AS g(i);
INSERT INTO upstream_count_files SELECT i FROM generate_series(200, 299) AS g(i);
SELECT count(*) FROM upstream_count_files;
SELECT * FROM ducklake.merge_adjacent_files('upstream_count_files'::regclass);
SELECT count(*) FROM upstream_count_files;
DELETE FROM upstream_count_files WHERE i % 10 = 0;
SELECT count(*) FROM upstream_count_files;
BEGIN;
SELECT * FROM ducklake.rewrite_data_files('upstream_count_files'::regclass);
SELECT count(*) FROM upstream_count_files;
ROLLBACK;
SELECT count(*) FROM upstream_count_files;
SELECT * FROM ducklake.rewrite_data_files('upstream_count_files'::regclass);
SELECT count(*) FROM upstream_count_files;
INSERT INTO upstream_count_files SELECT i FROM generate_series(300, 399) AS g(i);
INSERT INTO upstream_count_files SELECT i FROM generate_series(400, 499) AS g(i);
SELECT count(*) FROM upstream_count_files;
BEGIN;
SELECT * FROM ducklake.merge_adjacent_files('upstream_count_files'::regclass);
SELECT count(*) FROM upstream_count_files;
ROLLBACK;
SELECT count(*) FROM upstream_count_files;
DROP TABLE upstream_count_files;
