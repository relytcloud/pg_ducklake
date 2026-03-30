-- Regression tests for ducklake_fdw connection_string option

-- Mutual exclusivity: connection_string + dbname -> error
CREATE SERVER cs_bad_1
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (connection_string 'host=localhost dbname=mydb', dbname 'mydb');

-- Mutual exclusivity: connection_string + frozen_url -> error
CREATE SERVER cs_bad_2
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (connection_string 'host=localhost dbname=mydb',
             frozen_url 'https://example.com/test.ducklake');

-- Setup: create a managed DuckLake table with test data
CREATE TABLE cs_source (id int, val text) USING ducklake;
INSERT INTO cs_source VALUES (1, 'alpha'), (2, 'beta');

-- Valid: create server with connection_string (loopback to same DB)
CREATE SERVER cs_loopback
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (connection_string 'dbname=regression',
             metadata_schema 'ducklake');

-- Create foreign table via connection_string server
CREATE FOREIGN TABLE cs_foreign ()
    SERVER cs_loopback
    OPTIONS (table_name 'cs_source');

-- Query through connection_string server
SELECT * FROM cs_foreign ORDER BY id;

-- Cleanup
DROP FOREIGN TABLE cs_foreign;
DROP SERVER cs_loopback;
DROP TABLE cs_source;
