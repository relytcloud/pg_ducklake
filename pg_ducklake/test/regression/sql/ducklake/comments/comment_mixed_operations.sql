-- Upstream: test/sql/comments/comment_mixed_operations.test
-- Skip: PostgreSQL views do not have DuckLake view-comment metadata.
CREATE TABLE upstream_comment_mixed (a integer, b text) USING ducklake;
SELECT count(*) AS schema_versions
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_comment_mixed' AND t.end_snapshot IS NULL;
COMMENT ON TABLE upstream_comment_mixed IS 'first comment';
SELECT count(*) AS schema_versions
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_comment_mixed' AND t.end_snapshot IS NULL;
ALTER TABLE upstream_comment_mixed ADD COLUMN c bigint;
COMMENT ON COLUMN upstream_comment_mixed.c IS 'c column comment';
COMMENT ON TABLE upstream_comment_mixed IS 'updated comment';
SELECT count(*) AS schema_versions
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_comment_mixed' AND t.end_snapshot IS NULL;
ALTER TABLE upstream_comment_mixed RENAME COLUMN b TO b_renamed;
SELECT count(*) AS schema_versions
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_comment_mixed' AND t.end_snapshot IS NULL;
SELECT obj_description('upstream_comment_mixed'::regclass, 'pg_class');
SELECT col_description('upstream_comment_mixed'::regclass, 3);
COMMENT ON TABLE upstream_comment_mixed IS NULL;
SELECT obj_description('upstream_comment_mixed'::regclass, 'pg_class') IS NULL AS comment_cleared;
DROP TABLE upstream_comment_mixed;
