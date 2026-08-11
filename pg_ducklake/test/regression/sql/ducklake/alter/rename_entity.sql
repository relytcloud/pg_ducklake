-- Upstream: test/sql/alter/rename_entity.test
-- Skip: PostgreSQL views are not represented as DuckLake catalog view entries.
CREATE TABLE upstream_rename_entity_base (i integer) USING ducklake;
CREATE TABLE upstream_rename_entity_original (i integer) USING ducklake;
INSERT INTO upstream_rename_entity_original SELECT g FROM generate_series(1, 84) AS g;
BEGIN;
ALTER TABLE upstream_rename_entity_original RENAME TO upstream_rename_entity_new;
SELECT relname
FROM pg_class
WHERE relnamespace = 'public'::regnamespace
  AND relname IN ('upstream_rename_entity_original', 'upstream_rename_entity_new')
ORDER BY relname;
SELECT count(*) FROM upstream_rename_entity_new;
COMMIT;
DROP TABLE upstream_rename_entity_base, upstream_rename_entity_new;
