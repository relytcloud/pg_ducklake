-- Test for INSERT ... SELECT UNNEST with parameterized arrays (inline bypass)
-- This tests the optimization that bypasses DuckDB execution for the pattern:
-- INSERT INTO ducklake_table SELECT UNNEST($1::type[]), UNNEST($2::type[]), ...

-- Enable data inlining first
CALL ducklake.set_option('data_inlining_row_limit', 100);

-- Create test table with only int and text types
CREATE TABLE inline_bypass_test (
    id INT,
    name TEXT
) USING ducklake;

-- Test 1: Basic parameterized UNNEST insert using PREPARE/EXECUTE
PREPARE insert_arrays(int[], text[]) AS
INSERT INTO inline_bypass_test SELECT UNNEST($1), UNNEST($2);

EXECUTE insert_arrays(ARRAY[1, 2, 3], ARRAY['one', 'two', 'three']);

SELECT * FROM inline_bypass_test ORDER BY id;

-- Test 2: Another batch insert
EXECUTE insert_arrays(ARRAY[4, 5], ARRAY['four', 'five']);

SELECT * FROM inline_bypass_test ORDER BY id;

-- Test 3: Verify data is correctly inlined
SELECT COUNT(*) AS inlined_table_count
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'inline_bypass_test');

-- Clean up
DEALLOCATE insert_arrays;
DROP TABLE inline_bypass_test;

-- Reset the option
CALL ducklake.set_option('data_inlining_row_limit', 0);
