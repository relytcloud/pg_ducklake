-- Upstream: test/sql/sorted_table/data_inlining_flush_sorted_reset.test
-- Skip: Physical pre/post-flush row order is not exposed by the PostgreSQL API.
-- Resetting sort metadata before a flush leaves data intact.
CREATE TABLE upstream_flush_sort_reset (i integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_flush_sort_reset'::regclass);
INSERT INTO upstream_flush_sort_reset VALUES (3), (1), (2);
BEGIN;
CALL ducklake.set_sort('upstream_flush_sort_reset'::regclass, 'i DESC NULLS LAST');
CALL ducklake.reset_sort('upstream_flush_sort_reset'::regclass);
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_flush_sort_reset'::regclass);
COMMIT;
SELECT * FROM ducklake.get_sort('upstream_flush_sort_reset'::regclass);
SELECT i FROM upstream_flush_sort_reset ORDER BY i;
DROP TABLE upstream_flush_sort_reset;
