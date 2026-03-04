-- Test ducklake.freeze() — export metadata to a standalone .ducklake file

-- Disable data inlining so we get actual Parquet files
CALL ducklake.set_option('data_inlining_row_limit', 0);

-- Setup: create a table with data
CREATE TABLE freeze_test (id int, name text) USING ducklake;
INSERT INTO freeze_test VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM freeze_test ORDER BY id;

-- Test 1: Basic freeze
CALL ducklake.freeze('/tmp/test_freeze.ducklake');

-- Verify file was created
\! test -f /tmp/test_freeze.ducklake && echo 'frozen file exists'

-- Test 2: Freeze with custom data_path
CALL ducklake.freeze('/tmp/test_freeze_custom.ducklake', '/custom/data/path/');

-- Test 3: Freeze with inlined data (verifies flush-before-freeze)
CALL ducklake.set_option('data_inlining_row_limit', 100);
CREATE TABLE freeze_inlined (a int) USING ducklake;
INSERT INTO freeze_inlined VALUES (1), (2), (3);
SELECT * FROM freeze_inlined ORDER BY a;

-- Verify inlined data exists before freeze
SELECT count(*) > 0 AS has_inlined
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table
                  WHERE table_name = 'freeze_inlined' AND end_snapshot IS NULL);

CALL ducklake.freeze('/tmp/test_freeze_inlined.ducklake');

-- Test 4: Error — NULL output_path
CALL ducklake.freeze(NULL);

-- Cleanup
DROP TABLE freeze_test;
DROP TABLE freeze_inlined;
CALL ducklake.set_option('data_inlining_row_limit', 0);
\! rm -f /tmp/test_freeze.ducklake /tmp/test_freeze_custom.ducklake /tmp/test_freeze_inlined.ducklake
