-- Test ducklake.data_inlining_row_limit GUC
-- This option controls the row limit for data inlining in DuckLake.
-- Inserts with fewer rows than the limit are stored inline in the metadata catalog
-- instead of creating Parquet files.

-- Test 1: Check default value (should be 0 = disabled)
SHOW ducklake.data_inlining_row_limit;

-- Test 2: Create a table with inlining disabled (default) and insert data
CREATE TABLE test_no_inlining (i INT, j VARCHAR) USING ducklake;
INSERT INTO test_no_inlining VALUES (1, 'one'), (2, 'two');

-- Verify data is readable
SELECT * FROM test_no_inlining ORDER BY i;

-- Check that no inlined data table exists for this table
SELECT COUNT(*) AS inlined_table_count
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'test_no_inlining');

DROP TABLE test_no_inlining;

-- Test 3: Enable data inlining and create a table
SET ducklake.data_inlining_row_limit = 100;
SHOW ducklake.data_inlining_row_limit;

CREATE TABLE test_inlining (i INT, j VARCHAR) USING ducklake;
INSERT INTO test_inlining VALUES (1, 'one'), (2, 'two');

-- Verify data is readable
SELECT * FROM test_inlining ORDER BY i;

-- Check that inlined data table exists for this table
SELECT COUNT(*) AS inlined_table_count
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'test_inlining');

-- Get the inlined table name
SELECT table_name AS inlined_table_name
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'test_inlining')
\gset

-- Query the inlined data table content directly using the dynamic table name
-- The inlined data table stores row_id, begin_snapshot, end_snapshot, then user columns
SELECT row_id, i, j FROM ducklake.:inlined_table_name ORDER BY row_id;

-- Test 4: Insert more data within inlining limit - should still be inlined
INSERT INTO test_inlining VALUES (3, 'three'), (4, 'four');
SELECT * FROM test_inlining ORDER BY i;

-- Verify inlined data table now has more rows
SELECT COUNT(*) AS total_inlined_rows FROM ducklake.:inlined_table_name;

-- Test 5: Disable inlining and insert more data
SET ducklake.data_inlining_row_limit = 0;
SHOW ducklake.data_inlining_row_limit;

INSERT INTO test_inlining VALUES (5, 'five'), (6, 'six');
SELECT * FROM test_inlining ORDER BY i;

-- Inlined table should still have same row count (new data goes to parquet)
SELECT COUNT(*) AS inlined_rows_after_disable FROM ducklake.:inlined_table_name;

-- Cleanup
DROP TABLE test_inlining;

-- Test 6: Test flush_inlined_data function
-- First re-enable inlining and add some data
SET ducklake.data_inlining_row_limit = 100;
CREATE TABLE test_flush (i INT) USING ducklake;
INSERT INTO test_flush VALUES (1), (2), (3);

-- Verify inlined data exists
SELECT COUNT(*) AS inlined_tables_before_flush
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'test_flush');

-- Flush all inlined data to parquet files
SELECT ducklake.flush_inlined_data();

-- Verify inlined data was flushed (count should be 0 after flush)
SELECT COUNT(*) AS inlined_tables_after_flush
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'test_flush');

-- Data should still be readable
SELECT * FROM test_flush ORDER BY i;

-- Test flush_inlined_data with table_name parameter
INSERT INTO test_flush VALUES (4), (5);
SELECT COUNT(*) AS inlined_tables_before_specific_flush
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'test_flush');

SELECT ducklake.flush_inlined_data('public.test_flush'::regclass);

SELECT COUNT(*) AS inlined_tables_after_specific_flush
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'test_flush');

-- Test flush_inlined_data with positional arguments
INSERT INTO test_flush VALUES (6), (7);
SELECT ducklake.flush_inlined_data('test_flush'::regclass);

DROP TABLE test_flush;
