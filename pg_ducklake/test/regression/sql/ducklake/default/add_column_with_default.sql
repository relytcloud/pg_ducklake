-- Upstream: test/sql/default/add_column_with_default.test
CREATE TABLE upstream_default_add (i integer) USING ducklake;
INSERT INTO upstream_default_add VALUES (1), (2);
BEGIN;
ALTER TABLE upstream_default_add ADD COLUMN j integer DEFAULT 42;
INSERT INTO upstream_default_add VALUES (100, 100);
SELECT * FROM upstream_default_add ORDER BY i NULLS LAST, j NULLS LAST;
COMMIT;
INSERT INTO upstream_default_add DEFAULT VALUES;
BEGIN;
ALTER TABLE upstream_default_add ALTER i SET DEFAULT 1000;
ALTER TABLE upstream_default_add ALTER j DROP DEFAULT;
INSERT INTO upstream_default_add DEFAULT VALUES;
SELECT * FROM upstream_default_add ORDER BY i NULLS LAST, j NULLS LAST;
ROLLBACK;
BEGIN;
ALTER TABLE upstream_default_add ALTER i SET DEFAULT 1000;
ALTER TABLE upstream_default_add ALTER j DROP DEFAULT;
INSERT INTO upstream_default_add DEFAULT VALUES;
COMMIT;
INSERT INTO upstream_default_add DEFAULT VALUES;
SELECT * FROM upstream_default_add ORDER BY i NULLS LAST, j NULLS LAST;
SELECT c.column_name, c.initial_default, c.default_value
FROM ducklake.ducklake_column c
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_default_add'
ORDER BY c.column_id, c.begin_snapshot;
ALTER TABLE upstream_default_add ALTER nonexistent_column SET DEFAULT 1000;
ALTER TABLE upstream_default_add ALTER nonexistent_column DROP DEFAULT;
DROP TABLE upstream_default_add;
