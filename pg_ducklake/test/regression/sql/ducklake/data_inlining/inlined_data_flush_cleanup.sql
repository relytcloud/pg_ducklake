-- Upstream: test/sql/data_inlining/inlined_data_flush_cleanup.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_cleanup (i integer) USING ducklake;
INSERT INTO upstream_inline_cleanup VALUES (1), (2), (3);
SELECT max(snapshot_id) AS vbefore_alter FROM ducklake.ducklake_snapshot \gset
ALTER TABLE upstream_inline_cleanup ADD COLUMN j integer;
INSERT INTO upstream_inline_cleanup VALUES (4, 40);
SELECT count(*) AS inline_versions
FROM ducklake.ducklake_inlined_data_tables d JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_cleanup' AND t.end_snapshot IS NULL;
SELECT * FROM ducklake.flush_inlined_data('upstream_inline_cleanup'::regclass);
SELECT count(*) AS inline_versions
FROM ducklake.ducklake_inlined_data_tables d JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_cleanup' AND t.end_snapshot IS NULL;
SELECT count(*), sum(i) FROM upstream_inline_cleanup;
SELECT count(*) FROM ducklake.time_travel('upstream_inline_cleanup'::regclass, :vbefore_alter);
DROP TABLE upstream_inline_cleanup;
CALL ducklake.set_option('data_inlining_row_limit', 0);
