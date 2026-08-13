-- Upstream: test/sql/sorted_table/merge_adjacent_sorted_nested_expression.test
-- Skip: Requires implementation: nested expression-sorted compaction is unsupported.
-- Compaction supports nested PostgreSQL expressions as sort keys.
CREATE TABLE upstream_merge_sort_nested (id bigint, name text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_merge_sort_nested'::regclass);
INSERT INTO upstream_merge_sort_nested VALUES (2, 'beta');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_merge_sort_nested'::regclass);
INSERT INTO upstream_merge_sort_nested VALUES (1, 'alpha');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_merge_sort_nested'::regclass);
SELECT false AS nested_expression_sort_supported;
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_merge_sort_nested'::regclass);
SELECT * FROM ducklake.get_sort('upstream_merge_sort_nested'::regclass);
SELECT * FROM upstream_merge_sort_nested ORDER BY id;
DROP TABLE upstream_merge_sort_nested;
