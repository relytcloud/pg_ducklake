-- Upstream: test/sql/comments/comments.test
-- Skip: PostgreSQL views do not have DuckLake view-comment metadata.
CREATE TABLE upstream_comments (i integer, j integer) USING ducklake;
COMMENT ON TABLE upstream_comments IS 'very gezellige table';
SELECT obj_description('upstream_comments'::regclass, 'pg_class');
COMMENT ON TABLE upstream_comments IS NULL;
SELECT obj_description('upstream_comments'::regclass, 'pg_class');
BEGIN;
COMMENT ON TABLE upstream_comments IS 'rolled back comment';
SELECT obj_description('upstream_comments'::regclass, 'pg_class');
ROLLBACK;
SELECT obj_description('upstream_comments'::regclass, 'pg_class');
DROP TABLE upstream_comments;
