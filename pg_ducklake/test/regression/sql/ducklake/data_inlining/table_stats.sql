-- Upstream: test/sql/data_inlining/table_stats.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_stats (a integer) USING ducklake;
INSERT INTO upstream_inline_stats VALUES (0);
INSERT INTO upstream_inline_stats VALUES (1);
INSERT INTO upstream_inline_stats VALUES (2);
SELECT * FROM ducklake.flush_inlined_data('upstream_inline_stats'::regclass);
SELECT s.record_count, s.next_row_id
FROM ducklake.ducklake_table_stats s JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_stats' AND t.end_snapshot IS NULL;
DROP TABLE upstream_inline_stats;
CALL ducklake.set_option('data_inlining_row_limit', 0);
