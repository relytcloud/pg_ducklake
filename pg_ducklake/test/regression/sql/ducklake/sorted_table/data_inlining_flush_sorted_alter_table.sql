-- Upstream: test/sql/sorted_table/data_inlining_flush_sorted_alter_table.test
-- Skip: Physical Parquet row order is not exposed by the PostgreSQL API; keep this sorted-layout test unscheduled.
-- Ordinary ALTER TABLE changes do not interfere with sorted inline flushes.
CREATE TABLE upstream_flush_sort_alter (i integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_flush_sort_alter'::regclass);
INSERT INTO upstream_flush_sort_alter VALUES (3), (1), (2);
BEGIN;
CALL ducklake.set_sort('upstream_flush_sort_alter'::regclass, 'i DESC NULLS LAST');
ALTER TABLE upstream_flush_sort_alter ADD COLUMN note text;
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_flush_sort_alter'::regclass);
COMMIT;
SELECT * FROM ducklake.get_sort('upstream_flush_sort_alter'::regclass);
SELECT i, note FROM upstream_flush_sort_alter ORDER BY i;
DROP TABLE upstream_flush_sort_alter;
