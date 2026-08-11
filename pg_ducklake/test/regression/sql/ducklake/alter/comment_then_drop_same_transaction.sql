-- Upstream: test/sql/alter/comment_then_drop_same_transaction.test
-- Skip: PostgreSQL views are not represented as DuckLake catalog view entries.
CREATE TABLE upstream_comment_drop (i integer) USING ducklake;
INSERT INTO upstream_comment_drop VALUES (1), (2);
BEGIN;
COMMENT ON TABLE upstream_comment_drop IS 'foo';
DROP TABLE upstream_comment_drop;
COMMIT;
SELECT to_regclass('public.upstream_comment_drop') IS NULL AS table_was_dropped;
CREATE TABLE upstream_rename_drop (i integer) USING ducklake;
INSERT INTO upstream_rename_drop VALUES (1), (2);
BEGIN;
ALTER TABLE upstream_rename_drop RENAME TO upstream_rename_drop_final;
DROP TABLE upstream_rename_drop_final;
COMMIT;
SELECT to_regclass('public.upstream_rename_drop') IS NULL AS old_name_dropped,
       to_regclass('public.upstream_rename_drop_final') IS NULL AS new_name_dropped;
