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
SELECT * FROM test_ddl_renamed ORDER BY id;

-- ALTER COLUMN TYPE
ALTER TABLE test_ddl_renamed ALTER COLUMN id TYPE BIGINT;
SELECT * FROM test_ddl_renamed ORDER BY id;

-- DROP TABLE
DROP TABLE test_ddl_renamed;
