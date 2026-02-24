-- Test DuckLake GUCs
SHOW ducklake.default_table_path;
SHOW ducklake.vacuum_delete_threshold;
SHOW ducklake.as_of_timestamp;
SHOW ducklake.enable_inline_bypass;

-- Test setting GUCs
SET ducklake.default_table_path = '/tmp/test_path';
SHOW ducklake.default_table_path;
RESET ducklake.default_table_path;
SHOW ducklake.default_table_path;

SET ducklake.vacuum_delete_threshold = 0.5;
SHOW ducklake.vacuum_delete_threshold;
RESET ducklake.vacuum_delete_threshold;

SET ducklake.enable_inline_bypass = false;
SHOW ducklake.enable_inline_bypass;
RESET ducklake.enable_inline_bypass;
