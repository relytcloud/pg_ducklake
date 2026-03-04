-- Regression tests for ducklake_fdw (regular FDW mode)

-- Setup: create a managed DuckLake table with test data
CREATE TABLE fdw_source (id int, name text, score float8) USING ducklake;
INSERT INTO fdw_source VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.3), (3, 'Charlie', 92.1);

-- Create FDW server pointing to the same database
CREATE SERVER ducklake_test_server
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (metadata_schema 'ducklake');

-- Error: columns specified in CREATE FOREIGN TABLE
CREATE FOREIGN TABLE fdw_bad (id int, name text)
    SERVER ducklake_test_server
    OPTIONS (table_name 'fdw_source');

-- Create foreign table with auto-inferred columns
CREATE FOREIGN TABLE fdw_t ()
    SERVER ducklake_test_server
    OPTIONS (table_name 'fdw_source');

-- SELECT from foreign table
SELECT * FROM fdw_t ORDER BY id;

-- Aggregation
SELECT count(*), avg(score) FROM fdw_t;

-- Subquery with EXISTS
SELECT id, name FROM fdw_t WHERE EXISTS (
    SELECT 1 FROM fdw_t sub WHERE sub.score > 90
) ORDER BY id;

-- JOIN between managed and foreign table
SELECT s.id, s.name, f.score
FROM fdw_source s
JOIN fdw_t f ON s.id = f.id
ORDER BY s.id;

-- Second foreign table for join test
CREATE FOREIGN TABLE fdw_t2 ()
    SERVER ducklake_test_server
    OPTIONS (table_name 'fdw_source');

-- JOIN between two foreign tables
SELECT a.name, b.score
FROM fdw_t a
JOIN fdw_t2 b ON a.id = b.id
WHERE b.score > 90
ORDER BY a.name;

-- READ-ONLY enforcement
INSERT INTO fdw_t VALUES (4, 'Dave', 88.0);
UPDATE fdw_t SET score = 100 WHERE id = 1;
DELETE FROM fdw_t WHERE id = 1;

-- Error: non-existent table
CREATE FOREIGN TABLE fdw_nonexistent ()
    SERVER ducklake_test_server
    OPTIONS (table_name 'no_such_table');

-- Cleanup
DROP FOREIGN TABLE fdw_t2;
DROP FOREIGN TABLE fdw_t;
DROP SERVER ducklake_test_server;
DROP TABLE fdw_source;
