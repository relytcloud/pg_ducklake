-- Upstream: test/sql/rewrite_data_files/test_rewrite_inlined_file_deletes.test
-- Skip: the per-table ducklake_inlined_delete_<table_id> relation has no stable public PostgreSQL API for direct inspection.
-- A file with only an inlined deletion remains rewriteable through the public maintenance API.
CALL ducklake.set_option('data_inlining_row_limit', 10);
CALL ducklake.set_option('rewrite_delete_threshold', 0);
CREATE TABLE upstream_rewrite_inline_delete USING ducklake AS
SELECT i AS a FROM generate_series(0, 49) AS g(i);
DELETE FROM upstream_rewrite_inline_delete WHERE a = 25;
SELECT count(*) FROM upstream_rewrite_inline_delete;
SELECT * FROM ducklake.rewrite_data_files('upstream_rewrite_inline_delete'::regclass);
SELECT count(*), count(*) FILTER (WHERE a = 25) FROM upstream_rewrite_inline_delete;
DROP TABLE upstream_rewrite_inline_delete;
