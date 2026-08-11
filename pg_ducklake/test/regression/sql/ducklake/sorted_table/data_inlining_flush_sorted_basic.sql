-- Upstream: test/sql/sorted_table/data_inlining_flush_sorted_basic.test
-- Skip: Physical Parquet row order is not exposed by the PostgreSQL API; keep this sorted-layout test unscheduled.
-- Sorted metadata is honored while flushing inlined rows.
CREATE TABLE upstream_flush_sort_basic (i integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_flush_sort_basic'::regclass);
INSERT INTO upstream_flush_sort_basic VALUES (3), (1), (2);
CALL ducklake.set_sort('upstream_flush_sort_basic'::regclass, 'i DESC NULLS LAST');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_flush_sort_basic'::regclass);
SELECT * FROM ducklake.get_sort('upstream_flush_sort_basic'::regclass);
SELECT i FROM upstream_flush_sort_basic ORDER BY i;
DROP TABLE upstream_flush_sort_basic;
