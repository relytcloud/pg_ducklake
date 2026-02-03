-- Test DuckLake Foreign Data Wrapper
--
-- This tests the ability to access DuckLake tables from other databases
-- using the ducklake_fdw foreign data wrapper.

-- Create a managed DuckLake table in the current database
CREATE TABLE managed_test_table (
    id INT,
    name TEXT,
    amount DECIMAL(10,2)
) USING ducklake;

INSERT INTO managed_test_table VALUES
    (1, 'Alice', 100.50),
    (2, 'Bob', 200.75),
    (3, 'Charlie', 150.25);

-- Query the managed table to verify it works
SELECT * FROM managed_test_table ORDER BY id;

-- Create a foreign server pointing to the same database
CREATE SERVER ducklake_fdw_test
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (metadata_schema 'ducklake');

-- ERROR if columns are specified
CREATE FOREIGN TABLE foreign_test_table (id INT, name TEXT, amount DECIMAL(10,2))
    SERVER ducklake_fdw_test
    OPTIONS (schema_name 'public', table_name 'managed_test_table');

-- Create a foreign table pointing to the managed table
CREATE FOREIGN TABLE foreign_test_table ()
    SERVER ducklake_fdw_test
    OPTIONS (schema_name 'public', table_name 'managed_test_table');

-- Query the foreign table (should return same data)
SELECT * FROM foreign_test_table ORDER BY id;

-- Test read-only enforcement: INSERT should fail
INSERT INTO foreign_test_table VALUES (4, 'David', 300.00);

-- Test read-only enforcement: UPDATE should fail
UPDATE foreign_test_table SET amount = 999.99 WHERE id = 1;

-- Test read-only enforcement: DELETE should fail
DELETE FROM foreign_test_table WHERE id = 1;

-- Test join between managed and foreign tables
SELECT
    m.id,
    m.name as managed_name,
    f.name as foreign_name,
    m.amount + f.amount as total
FROM managed_test_table m
JOIN foreign_test_table f ON m.id = f.id
ORDER BY m.id;

-- Test aggregation on foreign table
SELECT COUNT(*), SUM(amount), AVG(amount)
FROM foreign_test_table;

-- Create a second managed table for testing joins between two foreign tables
CREATE TABLE managed_test_table_2 (
    id INT,
    category TEXT,
    value DECIMAL(10,2)
) USING ducklake;

INSERT INTO managed_test_table_2 VALUES
    (1, 'Electronics', 500.00),
    (2, 'Clothing', 150.00),
    (3, 'Food', 75.00);

-- Create a second foreign table
CREATE FOREIGN TABLE foreign_test_table_1 ()
    SERVER ducklake_fdw_test
    OPTIONS (schema_name 'public', table_name 'managed_test_table_2');

-- Test join between two foreign tables (implicit cross join)
SELECT * FROM foreign_test_table, foreign_test_table_1
WHERE foreign_test_table.id = foreign_test_table_1.id
ORDER BY foreign_test_table.id;

-- Test explicit join between two foreign tables
SELECT
    f1.id,
    f1.name,
    f2.category,
    f1.amount + f2.value as total
FROM foreign_test_table f1
JOIN foreign_test_table_1 f2 ON f1.id = f2.id
ORDER BY f1.id;

-- Cleanup second foreign table
DROP FOREIGN TABLE foreign_test_table_1;
DROP TABLE managed_test_table_2;

-- Test subquery in WHERE clause (EXISTS subquery)
-- EXISTS subqueries work because they reference foreign table in the subquery's FROM clause
SELECT * FROM managed_test_table m
WHERE EXISTS (SELECT 1 FROM foreign_test_table f WHERE f.id = m.id AND f.amount > 100)
ORDER BY id;

-- Test error case: non-existent table
CREATE FOREIGN TABLE foreign_nonexistent ()
    SERVER ducklake_fdw_test
    OPTIONS (schema_name 'public', table_name 'nonexistent_table');

-- Cleanup same-database tests
DROP FOREIGN TABLE foreign_test_table;
DROP SERVER ducklake_fdw_test;

-- Test cross-database access (same instance)
-- Create a separate database with DuckLake data
CREATE DATABASE ducklake_fdw_testdb WITH ENCODING 'UTF8' TEMPLATE template0;
\c ducklake_fdw_testdb
CREATE EXTENSION pg_duckdb;

-- Create a DuckLake table in the test database
CREATE TABLE archive_data (
    product_id INT,
    product_name TEXT,
    price DECIMAL(10,2)
) USING ducklake;

INSERT INTO archive_data VALUES
    (1, 'Widget', 9.99),
    (2, 'Gadget', 19.99),
    (3, 'Doohickey', 14.99);

-- Verify data in test database
SELECT * FROM archive_data ORDER BY product_id;

-- Switch back to main database and access test database via FDW
\c regression
CREATE SERVER archive_server
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (dbname 'ducklake_fdw_testdb', metadata_schema 'ducklake');

-- Create foreign table pointing to archive database
CREATE FOREIGN TABLE foreign_archive_data ()
    SERVER archive_server
    OPTIONS (schema_name 'public', table_name 'archive_data');

-- Query data from archive database
SELECT * FROM foreign_archive_data ORDER BY product_id;

-- Test join across databases (managed table in current DB, foreign table from archive DB)
SELECT
    m.id,
    m.name,
    f.product_name,
    m.amount + f.price as total
FROM managed_test_table m
CROSS JOIN foreign_archive_data f
WHERE m.id = 1 AND f.product_id = 1;

-- Cleanup cross-database tests
DROP FOREIGN TABLE foreign_archive_data;
DROP SERVER archive_server;
DROP TABLE managed_test_table;

-- Clean up test database
\c regression
DROP DATABASE ducklake_fdw_testdb;
