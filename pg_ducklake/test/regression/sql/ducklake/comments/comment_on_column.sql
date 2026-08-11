-- Upstream: test/sql/comments/comment_on_column.test
CREATE TABLE upstream_comment_column USING ducklake AS
SELECT 1 AS test_table_column;
SELECT col_description('upstream_comment_column'::regclass, 1);
COMMENT ON COLUMN upstream_comment_column.test_table_column IS 'very gezellige column';
SELECT col_description('upstream_comment_column'::regclass, 1);
BEGIN;
COMMENT ON COLUMN upstream_comment_column.test_table_column IS 'toch niet zo gezellig';
SELECT col_description('upstream_comment_column'::regclass, 1);
ROLLBACK;
SELECT col_description('upstream_comment_column'::regclass, 1);
DROP TABLE upstream_comment_column;
