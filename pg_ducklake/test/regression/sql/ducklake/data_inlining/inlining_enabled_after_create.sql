-- Upstream: test/sql/data_inlining/inlining_enabled_after_create.test
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_inline_late (i integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 10);
INSERT INTO upstream_inline_late VALUES (42);
SELECT count(*) AS rows, sum(i) AS value_sum FROM upstream_inline_late;
SELECT count(*) AS inline_tables
FROM ducklake.ducklake_inlined_data_tables d JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_late' AND t.end_snapshot IS NULL;
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_late' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
INSERT INTO upstream_inline_late VALUES (1), (2), (3);
SELECT count(*), sum(i) FROM upstream_inline_late;
SELECT count(*) AS inline_tables
FROM ducklake.ducklake_inlined_data_tables d JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_late' AND t.end_snapshot IS NULL;
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_late' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
INSERT INTO upstream_inline_late SELECT g FROM generate_series(0, 99) g;
SELECT count(*), sum(i) FROM upstream_inline_late;
DROP TABLE upstream_inline_late;
CALL ducklake.set_option('data_inlining_row_limit', 0);
