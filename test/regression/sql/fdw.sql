-- Regression tests for ducklake_fdw (regular FDW mode)

-- Setup: create a managed DuckLake table with test data
CREATE TABLE fdw_source (id int, name text, score float8) USING ducklake;
INSERT INTO fdw_source VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.3), (3, 'Charlie', 92.1);

-- Create FDW server pointing to the same database
CREATE SERVER ducklake_test_server
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (metadata_schema 'ducklake');

-- Explicit columns: allowed (subset of remote columns)
CREATE FOREIGN TABLE fdw_explicit (id int, name text)
    SERVER ducklake_test_server
    OPTIONS (table_name 'fdw_source');

SELECT * FROM fdw_explicit ORDER BY id;

DROP FOREIGN TABLE fdw_explicit;

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

-- DML on regular FDW tables
INSERT INTO fdw_t VALUES (4, 'Dave', 88.0);
SELECT * FROM fdw_t ORDER BY id;

UPDATE fdw_t SET score = 99.9 WHERE id = 4;
SELECT id, score FROM fdw_t WHERE id = 4;

DELETE FROM fdw_t WHERE id = 4;
SELECT * FROM fdw_t ORDER BY id;

-- Cross-check: writes through FDW are visible on the base table
INSERT INTO fdw_t VALUES (5, 'Eve', 77.0);
SELECT * FROM fdw_source ORDER BY id;

-- updatable option: table-level override
CREATE FOREIGN TABLE fdw_readonly ()
    SERVER ducklake_test_server
    OPTIONS (table_name 'fdw_source', updatable 'false');

INSERT INTO fdw_readonly VALUES (6, 'Frank', 60.0);
SELECT * FROM fdw_readonly ORDER BY id;

DROP FOREIGN TABLE fdw_readonly;

-- updatable option: server-level
CREATE SERVER ducklake_readonly_server
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (metadata_schema 'ducklake', updatable 'false');

CREATE FOREIGN TABLE fdw_srv_ro ()
    SERVER ducklake_readonly_server
    OPTIONS (table_name 'fdw_source');

INSERT INTO fdw_srv_ro VALUES (6, 'Frank', 60.0);

-- table-level updatable 'true' overrides server-level 'false'
CREATE FOREIGN TABLE fdw_srv_override ()
    SERVER ducklake_readonly_server
    OPTIONS (table_name 'fdw_source', updatable 'true');

INSERT INTO fdw_srv_override VALUES (6, 'Frank', 60.0);
DELETE FROM fdw_srv_override WHERE id = 6;

DROP FOREIGN TABLE fdw_srv_override;
DROP FOREIGN TABLE fdw_srv_ro;
DROP SERVER ducklake_readonly_server;

-- updatable validation: invalid value
CREATE SERVER ducklake_bad_updatable
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (metadata_schema 'ducklake', updatable 'yes');

-- Error: non-existent table
CREATE FOREIGN TABLE fdw_nonexistent ()
    SERVER ducklake_test_server
    OPTIONS (table_name 'no_such_table');

-- Cleanup
DROP FOREIGN TABLE fdw_t2;
DROP FOREIGN TABLE fdw_t;
DROP SERVER ducklake_test_server;
DROP TABLE fdw_source;
