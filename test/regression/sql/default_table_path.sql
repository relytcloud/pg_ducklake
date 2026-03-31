-- Test ducklake.default_table_path end-to-end:
-- set a custom path, create a table, insert data, flush, verify file location.

-- Disable data inlining so inserts write parquet files immediately
CALL ducklake.set_option('data_inlining_row_limit', 0);

-- Use a custom directory as the default table path (simulates s3:// bucket)
SET ducklake.default_table_path = '/tmp/ducklake_test_custom_path';

CREATE TABLE dtp_test (id int, val text) USING ducklake;
INSERT INTO dtp_test VALUES (1, 'one'), (2, 'two');

-- Verify data is readable
SELECT * FROM dtp_test ORDER BY id;

-- Verify files are under the custom path
SELECT * FROM duckdb.query($$
  SELECT bool_and(starts_with(data_file, '/tmp/ducklake_test_custom_path/')) AS path_ok
  FROM ducklake_list_files('pgducklake', 'dtp_test', schema => 'public')
$$);

-- Cleanup
RESET ducklake.default_table_path;
DROP TABLE dtp_test;
CALL ducklake.set_option('data_inlining_row_limit', 0);
