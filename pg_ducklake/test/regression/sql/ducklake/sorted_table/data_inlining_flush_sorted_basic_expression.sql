-- Upstream: test/sql/sorted_table/data_inlining_flush_sorted_basic_expression.test
-- Skip: Requires implementation: pg_ducklake does not yet support expression sort keys.
-- Expression sort keys survive an inline-data flush.
CREATE TABLE upstream_flush_sort_expression (i integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_flush_sort_expression'::regclass);
INSERT INTO upstream_flush_sort_expression VALUES (-3), (1), (-2);
CALL ducklake.set_sort('upstream_flush_sort_expression'::regclass, '(i * i) DESC NULLS LAST');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_flush_sort_expression'::regclass);
SELECT * FROM ducklake.get_sort('upstream_flush_sort_expression'::regclass);
SELECT i FROM upstream_flush_sort_expression ORDER BY i;
DROP TABLE upstream_flush_sort_expression;
