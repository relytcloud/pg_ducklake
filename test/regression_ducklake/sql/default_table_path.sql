-- Test ducklake.default_table_path GUC
-- This test verifies that tables are created in custom paths when the GUC is set

-- Test 1: Create table without GUC set (should use default path)
CREATE TABLE test_default_path (i INT, j VARCHAR) USING ducklake;

-- Verify the table path uses default DuckLake data directory
SELECT
    table_name,
    path,
    path_is_relative,
    substring(path from 1 for 50) as path_prefix
FROM ducklake.ducklake_table
WHERE table_name = 'test_default_path';

-- Test 2: Set custom path and create table
SET ducklake.default_table_path = '/tmp/ducklake_custom_tables';

CREATE TABLE test_custom_path (i INT, j VARCHAR) USING ducklake;

-- Verify the table path uses custom path
SELECT
    table_name,
    path,
    path_is_relative,
    starts_with(path, '/tmp/ducklake_custom_tables') as uses_custom_path
FROM ducklake.ducklake_table
WHERE table_name = 'test_custom_path';

-- Test 3: Insert data and verify it works
INSERT INTO test_custom_path VALUES (1, 'one'), (2, 'two');

SELECT * FROM test_custom_path ORDER BY i;

-- Test 4: CTAS with custom path
CREATE TABLE test_ctas_custom_path USING ducklake
AS SELECT * FROM test_custom_path;

-- Verify CTAS table also uses custom path
SELECT
    table_name,
    path,
    starts_with(path, '/tmp/ducklake_custom_tables') as uses_custom_path
FROM ducklake.ducklake_table
WHERE table_name = 'test_ctas_custom_path';

-- Verify data was copied
SELECT * FROM test_ctas_custom_path ORDER BY i;

-- Test 5: Change path to another location
SET ducklake.default_table_path = '/tmp/ducklake_another_location';

CREATE TABLE test_another_path (i INT) USING ducklake;

-- Verify the new table uses the new path
SELECT
    table_name,
    path,
    starts_with(path, '/tmp/ducklake_another_location') as uses_another_path
FROM ducklake.ducklake_table
WHERE table_name = 'test_another_path';

-- Test 6: Reset to empty string (should use default path)
SET ducklake.default_table_path = '';

CREATE TABLE test_reset_path (i INT) USING ducklake;

-- Verify the table uses default path again
SELECT
    table_name,
    path,
    path_is_relative
FROM ducklake.ducklake_table
WHERE table_name = 'test_reset_path';

-- Test 7: Verify all tables created
SELECT
    table_name,
    substring(path from 1 for 60) as path_prefix,
    path_is_relative
FROM ducklake.ducklake_table
WHERE table_name LIKE 'test_%'
ORDER BY table_name;

-- Cleanup
DROP TABLE test_default_path;
DROP TABLE test_custom_path;
DROP TABLE test_ctas_custom_path;
DROP TABLE test_another_path;
DROP TABLE test_reset_path;

-- Test 8: Set before ddb initialized
DROP EXTENSION pg_duckdb;
CREATE EXTENSION pg_duckdb;
-- ddb session is not initialized here
SET ducklake.default_table_path = 's3://my_bucket';
CREATE TABLE t(a INT) USING ducklake;

SELECT
    table_name,
    path,
    path_is_relative,
    substring(path from 1 for 50) as path_prefix
FROM ducklake.ducklake_table
WHERE table_name = 't';

DROP TABLE t;
