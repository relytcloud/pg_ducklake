-- Upstream: test/sql/data_inlining/superseded_inlined_table_flush_drop.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_superseded (i integer) USING ducklake;
INSERT INTO upstream_inline_superseded VALUES (1), (2), (3);
ALTER TABLE upstream_inline_superseded ADD COLUMN j integer;
INSERT INTO upstream_inline_superseded VALUES (4, 40);
SELECT count(*) AS inline_versions,
       min(d.schema_version) <> max(d.schema_version) AS distinct_versions
FROM ducklake.ducklake_inlined_data_tables d JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_superseded' AND t.end_snapshot IS NULL;
SELECT * FROM ducklake.flush_inlined_data('upstream_inline_superseded'::regclass);
SELECT count(*) AS inline_versions
FROM ducklake.ducklake_inlined_data_tables d JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_superseded' AND t.end_snapshot IS NULL;
SELECT * FROM upstream_inline_superseded ORDER BY i;
DROP TABLE upstream_inline_superseded;
CALL ducklake.set_option('data_inlining_row_limit', 0);
