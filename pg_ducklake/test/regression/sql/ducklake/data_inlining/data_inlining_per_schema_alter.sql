-- Upstream: test/sql/data_inlining/data_inlining_per_schema_alter.test
-- Skip: Requires implementation: per-schema set_option currently rejects a valid DuckLake schema.
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE SCHEMA upstream_inline_noinline;
CALL ducklake.set_option('data_inlining_row_limit', 0, 'upstream_inline_noinline'::regnamespace);
CREATE TABLE upstream_inline_noinline.t (i integer, j text) USING ducklake;
INSERT INTO upstream_inline_noinline.t VALUES (1, 'hello'), (2, 'world');
ALTER TABLE upstream_inline_noinline.t ADD COLUMN k integer DEFAULT 42;
SELECT * FROM upstream_inline_noinline.t ORDER BY i;
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
JOIN ducklake.ducklake_schema s USING (schema_id)
WHERE s.schema_name = 'upstream_inline_noinline' AND t.table_name = 't'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DROP TABLE upstream_inline_noinline.t;
DROP SCHEMA upstream_inline_noinline;
CALL ducklake.set_option('data_inlining_row_limit', 0);
