-- Test ALTER TABLE and RENAME for DuckLake tables

-- CREATE TABLE
CREATE TABLE test_ddl (id INT, name TEXT) USING ducklake;
INSERT INTO test_ddl VALUES (1, 'Alice');
SELECT * FROM test_ddl;

-- RENAME TABLE
ALTER TABLE test_ddl RENAME TO test_ddl_renamed;
SELECT * FROM test_ddl_renamed;

-- ADD COLUMN
ALTER TABLE test_ddl_renamed ADD COLUMN age INT DEFAULT 20;
INSERT INTO test_ddl_renamed VALUES (2, 'Bob', 30);
SELECT * FROM test_ddl_renamed ORDER BY id;

-- RENAME COLUMN
ALTER TABLE test_ddl_renamed RENAME COLUMN name TO full_name;

-- ALTER COLUMN TYPE
ALTER TABLE test_ddl_renamed ALTER COLUMN id TYPE BIGINT;

-- Verify RENAME TABLE, RENAME COLUMN, ALTER COLUMN TYPE, ADD COLUMN in metadata
SELECT c.column_name, c.column_type, c.nulls_allowed, c.default_value
FROM ducklake.ducklake_column c
JOIN ducklake.ducklake_table t ON c.table_id = t.table_id
WHERE t.table_name = 'test_ddl_renamed' AND t.end_snapshot IS NULL
  AND c.end_snapshot IS NULL
ORDER BY c.column_order;

SELECT * FROM test_ddl_renamed ORDER BY id;

-- SET DEFAULT
ALTER TABLE test_ddl_renamed ALTER COLUMN age SET DEFAULT 99;

-- Verify default_value in metadata
SELECT c.column_name, c.default_value
FROM ducklake.ducklake_column c
JOIN ducklake.ducklake_table t ON c.table_id = t.table_id
WHERE t.table_name = 'test_ddl_renamed' AND t.end_snapshot IS NULL
  AND c.end_snapshot IS NULL AND c.column_name = 'age';

INSERT INTO test_ddl_renamed (id, full_name) VALUES (3, 'Charlie');
SELECT * FROM test_ddl_renamed ORDER BY id;

-- DROP DEFAULT
ALTER TABLE test_ddl_renamed ALTER COLUMN age DROP DEFAULT;

-- Verify default_value cleared in metadata
SELECT c.column_name, c.default_value
FROM ducklake.ducklake_column c
JOIN ducklake.ducklake_table t ON c.table_id = t.table_id
WHERE t.table_name = 'test_ddl_renamed' AND t.end_snapshot IS NULL
  AND c.end_snapshot IS NULL AND c.column_name = 'age';

INSERT INTO test_ddl_renamed (id, full_name) VALUES (4, 'Dave');
SELECT * FROM test_ddl_renamed ORDER BY id;

-- SET NOT NULL
ALTER TABLE test_ddl_renamed ALTER COLUMN full_name SET NOT NULL;

-- Verify nulls_allowed in metadata
SELECT c.column_name, c.nulls_allowed
FROM ducklake.ducklake_column c
JOIN ducklake.ducklake_table t ON c.table_id = t.table_id
WHERE t.table_name = 'test_ddl_renamed' AND t.end_snapshot IS NULL
  AND c.end_snapshot IS NULL AND c.column_name = 'full_name';

-- verify NOT NULL constraint is enforced
INSERT INTO test_ddl_renamed (id) VALUES (5);

-- DROP NOT NULL
ALTER TABLE test_ddl_renamed ALTER COLUMN full_name DROP NOT NULL;

-- Verify nulls_allowed restored in metadata
SELECT c.column_name, c.nulls_allowed
FROM ducklake.ducklake_column c
JOIN ducklake.ducklake_table t ON c.table_id = t.table_id
WHERE t.table_name = 'test_ddl_renamed' AND t.end_snapshot IS NULL
  AND c.end_snapshot IS NULL AND c.column_name = 'full_name';

INSERT INTO test_ddl_renamed (id) VALUES (5);
SELECT * FROM test_ddl_renamed ORDER BY id;

-- DROP COLUMN
ALTER TABLE test_ddl_renamed DROP COLUMN age;

-- Verify dropped column no longer in current metadata
SELECT c.column_name
FROM ducklake.ducklake_column c
JOIN ducklake.ducklake_table t ON c.table_id = t.table_id
WHERE t.table_name = 'test_ddl_renamed' AND t.end_snapshot IS NULL
  AND c.end_snapshot IS NULL
ORDER BY c.column_order;

SELECT * FROM test_ddl_renamed ORDER BY id;

-- COMMENT ON TABLE
COMMENT ON TABLE test_ddl_renamed IS 'A test table for DDL operations';
SELECT obj_description('test_ddl_renamed'::regclass, 'pg_class');

-- COMMENT ON COLUMN
COMMENT ON COLUMN test_ddl_renamed.id IS 'The primary identifier';
SELECT col_description('test_ddl_renamed'::regclass, 1);

-- Remove table comment
COMMENT ON TABLE test_ddl_renamed IS NULL;
SELECT obj_description('test_ddl_renamed'::regclass, 'pg_class');

-- Remove column comment
COMMENT ON COLUMN test_ddl_renamed.id IS NULL;
SELECT col_description('test_ddl_renamed'::regclass, 1);

-- COMMENT with special characters
COMMENT ON TABLE test_ddl_renamed IS 'it''s a "quoted" value & more';
SELECT obj_description('test_ddl_renamed'::regclass, 'pg_class');
COMMENT ON TABLE test_ddl_renamed IS NULL;

-- DROP TABLE
DROP TABLE test_ddl_renamed;
