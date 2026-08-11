-- Upstream: test/sql/alter/rename_column.test
CREATE TABLE upstream_rename_column (col1 integer, col2 integer) USING ducklake;
ALTER TABLE upstream_rename_column RENAME COLUMN col1 TO new_col1;
INSERT INTO upstream_rename_column VALUES (1, 2), (NULL, 3);
ALTER TABLE upstream_rename_column RENAME COLUMN col2 TO new_col2;
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'upstream_rename_column'
ORDER BY ordinal_position;
SELECT * FROM upstream_rename_column ORDER BY new_col1 NULLS LAST;
ALTER TABLE upstream_rename_column RENAME COLUMN blablabla TO k;
DROP TABLE upstream_rename_column;
