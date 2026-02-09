-- Test ducklake.options() function

-- Disable force execution to prevent pg_duckdb from trying to execute this function in DuckDB
-- which causes warnings because it can't find 'options' function in DuckDB catalog
SET duckdb.force_execution = false;

-- 1. Verify the function returns expected columns
SELECT option_name, description, value, scope, scope_entry
FROM ducklake.options()
WHERE option_name = 'data_inlining_row_limit';

-- 2. Set an option and verify it shows up in the options list
CALL ducklake.set_option('data_inlining_row_limit', 100);

SELECT option_name, description, value, scope, scope_entry
FROM ducklake.options()
WHERE option_name = 'data_inlining_row_limit';

-- 3. Set a table-scoped option and verify
CREATE TABLE options_test (a int) USING ducklake;

CALL ducklake.set_option('data_inlining_row_limit', 50, 'options_test'::regclass);

SELECT option_name, description, value, scope, scope_entry
FROM ducklake.options()
WHERE option_name = 'data_inlining_row_limit'
ORDER BY scope;

-- Cleanup
DROP TABLE options_test;
