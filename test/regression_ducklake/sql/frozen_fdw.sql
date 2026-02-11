-- Test Frozen DuckLake Foreign Data Wrapper
--
-- This tests the ability to access frozen (read-only, HTTP-based) DuckLake
-- tables using the ducklake_fdw foreign data wrapper.

-- Test validation: frozen_url cannot be used with dbname
CREATE SERVER frozen_invalid_1
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (frozen_url 'https://example.com/test.ducklake', dbname 'mydb');

-- Test validation: frozen_url cannot be used with metadata_schema
CREATE SERVER frozen_invalid_2
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (frozen_url 'https://example.com/test.ducklake', metadata_schema 'ducklake');

-- Test validation: frozen_url cannot be used with both dbname and metadata_schema
CREATE SERVER frozen_invalid_3
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (frozen_url 'https://example.com/test.ducklake', dbname 'mydb', metadata_schema 'ducklake');

-- Create a server pointing to a frozen ducklake URL
CREATE SERVER frozen_space
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (frozen_url 'https://raw.githubusercontent.com/marhar/frozen/fce88027556c98161ac3406c39e5a6d60561d34d/space.ducklake');

-- ERROR if columns are specified
CREATE FOREIGN TABLE frozen_test_cols (id INT, name TEXT)
    SERVER frozen_space
    OPTIONS (schema_name 'main', table_name 'astronauts');

-- Create a foreign table with auto-inferred columns
CREATE FOREIGN TABLE frozen_astronauts ()
    SERVER frozen_space
    OPTIONS (schema_name 'main', table_name 'astronauts');

SET duckdb.force_execution = false;

-- Query the frozen foreign table
SELECT * FROM frozen_astronauts ORDER BY 1 LIMIT 5;

-- Test read-only enforcement: INSERT should fail
INSERT INTO frozen_astronauts VALUES (999, 'Test', 'Test', '2000-01-01', 'Test', '2000-01-01', 0, 0, 0, 'Test');

-- Test read-only enforcement: UPDATE should fail
UPDATE frozen_astronauts SET name = 'Modified' WHERE name = 'Test';

-- Test read-only enforcement: DELETE should fail
DELETE FROM frozen_astronauts WHERE name = 'Test';

-- Test aggregation on frozen foreign table
SELECT COUNT(*) FROM frozen_astronauts;

-- Test error case: non-existent table in frozen ducklake
CREATE FOREIGN TABLE frozen_nonexistent ()
    SERVER frozen_space
    OPTIONS (schema_name 'main', table_name 'nonexistent_table');

-- Cleanup
DROP FOREIGN TABLE frozen_astronauts;
DROP SERVER frozen_space;
