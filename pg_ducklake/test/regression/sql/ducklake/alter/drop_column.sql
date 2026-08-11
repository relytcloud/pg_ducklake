-- Upstream: test/sql/alter/drop_column.test
CREATE TABLE upstream_drop_column (col1 integer, col2 integer, col3 integer) USING ducklake;
ALTER TABLE upstream_drop_column DROP COLUMN col3;
ALTER TABLE upstream_drop_column DROP COLUMN col2;
ALTER TABLE upstream_drop_column DROP COLUMN col2;
ALTER TABLE upstream_drop_column DROP COLUMN IF EXISTS col2;
ALTER TABLE upstream_drop_column DROP COLUMN col1;
INSERT INTO upstream_drop_column VALUES (1), (2), (3);
SELECT col1 FROM upstream_drop_column ORDER BY col1;
ALTER TABLE upstream_drop_column DROP COLUMN nonexistent_column;
DROP TABLE upstream_drop_column;
