-- Upstream: test/sql/comments/comment_duplicate_same_transaction.test
-- Skip: PostgreSQL views do not have DuckLake view-comment metadata.
CREATE TABLE upstream_comment_duplicate (a integer) USING ducklake;
BEGIN;
COMMENT ON TABLE upstream_comment_duplicate IS 'first';
COMMENT ON TABLE upstream_comment_duplicate IS 'second';
COMMIT;
SELECT obj_description('upstream_comment_duplicate'::regclass, 'pg_class');
SELECT tag.key, tag.value
FROM ducklake.ducklake_tag tag
JOIN ducklake.ducklake_table t ON t.table_id = tag.object_id
WHERE t.table_name = 'upstream_comment_duplicate' AND t.end_snapshot IS NULL
  AND tag.end_snapshot IS NULL
ORDER BY tag.key;
BEGIN;
COMMENT ON COLUMN upstream_comment_duplicate.a IS 'col first';
COMMENT ON COLUMN upstream_comment_duplicate.a IS 'col second';
COMMIT;
SELECT col_description('upstream_comment_duplicate'::regclass, 1);
SELECT tag.key, tag.value
FROM ducklake.ducklake_column_tag tag
JOIN ducklake.ducklake_table t USING (table_id)
JOIN ducklake.ducklake_column c USING (table_id, column_id)
WHERE t.table_name = 'upstream_comment_duplicate' AND t.end_snapshot IS NULL
  AND c.column_name = 'a' AND c.end_snapshot IS NULL
  AND tag.end_snapshot IS NULL
ORDER BY tag.key;
DROP TABLE upstream_comment_duplicate;
