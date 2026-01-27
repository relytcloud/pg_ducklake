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

-- Test subquery in WHERE clause (EXISTS subquery)
-- EXISTS subqueries work because they reference foreign table in the subquery's FROM clause
SELECT * FROM managed_test_table m
WHERE EXISTS (SELECT 1 FROM foreign_test_table f WHERE f.id = m.id AND f.amount > 100)
ORDER BY id;

-- Cleanup
DROP FOREIGN TABLE foreign_test_table;
DROP SERVER ducklake_fdw_test;
DROP TABLE managed_test_table;
