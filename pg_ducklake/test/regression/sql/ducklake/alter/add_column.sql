-- Upstream: test/sql/alter/add_column.test
CREATE TABLE upstream_alter_add_column (col1 integer) USING ducklake;
ALTER TABLE upstream_alter_add_column ADD COLUMN new_col2 integer;
ALTER TABLE upstream_alter_add_column ADD COLUMN new_col2 integer;
ALTER TABLE upstream_alter_add_column ADD COLUMN IF NOT EXISTS new_col2 integer;
INSERT INTO upstream_alter_add_column VALUES (1, 2), (NULL, 3);
ALTER TABLE upstream_alter_add_column ADD COLUMN new_col3 text;
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'upstream_alter_add_column'
ORDER BY ordinal_position;
INSERT INTO upstream_alter_add_column VALUES (1, 2, 'hello'), (NULL, 3, 'world');
SELECT * FROM upstream_alter_add_column ORDER BY col1 NULLS LAST, new_col3 NULLS FIRST;
SELECT * FROM upstream_alter_add_column WHERE new_col3 = 'hello' ORDER BY col1;
DROP TABLE upstream_alter_add_column;
