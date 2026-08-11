-- Upstream: test/sql/comments/comment_same_transaction.test
-- Skip: PostgreSQL views do not have DuckLake view-comment metadata.
CREATE TABLE upstream_comment_same_tx (a integer, b text) USING ducklake;
SELECT count(*) AS schema_versions
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_comment_same_tx' AND t.end_snapshot IS NULL;
BEGIN;
COMMENT ON TABLE upstream_comment_same_tx IS 'my table comment';
ALTER TABLE upstream_comment_same_tx ADD COLUMN c bigint;
COMMENT ON COLUMN upstream_comment_same_tx.c IS 'new column comment';
COMMIT;
SELECT count(*) AS schema_versions
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_comment_same_tx' AND t.end_snapshot IS NULL;
SELECT obj_description('upstream_comment_same_tx'::regclass, 'pg_class');
SELECT a.attname, col_description(a.attrelid, a.attnum)
FROM pg_attribute a
WHERE a.attrelid = 'upstream_comment_same_tx'::regclass
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum;
ALTER TABLE upstream_comment_same_tx ADD COLUMN d bigint;
SELECT count(*) AS schema_versions
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_comment_same_tx' AND t.end_snapshot IS NULL;
DROP TABLE upstream_comment_same_tx;
