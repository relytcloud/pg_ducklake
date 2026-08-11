-- Upstream: test/sql/sorted_table/merge_adjacent_sorted_expression.test
-- Skip: Requires implementation: expression-sorted compaction is unsupported.
-- Compaction supports a sort expression over multiple columns.
CREATE TABLE upstream_merge_sort_expression (id bigint, first_part text, second_part text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_merge_sort_expression'::regclass);
INSERT INTO upstream_merge_sort_expression VALUES (2, 'b', '2');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_merge_sort_expression'::regclass);
INSERT INTO upstream_merge_sort_expression VALUES (1, 'a', '1');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_merge_sort_expression'::regclass);
CALL ducklake.set_sort('upstream_merge_sort_expression'::regclass, '(first_part || second_part) ASC NULLS LAST');
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_merge_sort_expression'::regclass);
SELECT * FROM ducklake.get_sort('upstream_merge_sort_expression'::regclass);
SELECT * FROM upstream_merge_sort_expression ORDER BY id;
DROP TABLE upstream_merge_sort_expression;
