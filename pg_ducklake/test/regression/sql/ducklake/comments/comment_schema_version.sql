-- Upstream: test/sql/comments/comment_schema_version.test
-- Skip: PostgreSQL views do not have DuckLake view-comment metadata or schema versions.
CREATE TABLE upstream_comment_schema_version (a integer, b text) USING ducklake;
SELECT count(*) AS schema_versions
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_comment_schema_version' AND t.end_snapshot IS NULL;
COMMENT ON TABLE upstream_comment_schema_version IS 'table comment';
COMMENT ON COLUMN upstream_comment_schema_version.a IS 'column a comment';
SELECT count(*) AS schema_versions
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_comment_schema_version' AND t.end_snapshot IS NULL;
SELECT obj_description('upstream_comment_schema_version'::regclass, 'pg_class'),
       col_description('upstream_comment_schema_version'::regclass, 1);
COMMENT ON TABLE upstream_comment_schema_version IS NULL;
COMMENT ON COLUMN upstream_comment_schema_version.a IS NULL;
SELECT count(*) AS schema_versions
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_comment_schema_version' AND t.end_snapshot IS NULL;
SELECT obj_description('upstream_comment_schema_version'::regclass, 'pg_class') IS NULL,
       col_description('upstream_comment_schema_version'::regclass, 1) IS NULL;
ALTER TABLE upstream_comment_schema_version ADD COLUMN c bigint;
SELECT count(*) AS schema_versions
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_comment_schema_version' AND t.end_snapshot IS NULL;
DROP TABLE upstream_comment_schema_version;
