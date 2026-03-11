-- Test list_files and table_info functions

-- Setup
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE lf_test (id int, val text) USING ducklake;
INSERT INTO lf_test VALUES (1, 'one');

-- 1. list_files returns at least one file
SELECT count(*) > 0 AS has_files FROM ducklake.list_files('public', 'lf_test');

-- 2. table_info returns rows
SELECT count(*) > 0 AS has_tables FROM ducklake.table_info();

-- Cleanup
DROP TABLE lf_test;
CALL ducklake.set_option('data_inlining_row_limit', 0);
