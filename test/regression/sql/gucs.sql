-- Test DuckLake GUCs
SHOW ducklake.default_table_path;
SHOW ducklake.vacuum_delete_threshold;
SHOW ducklake.enable_direct_insert;

-- Test setting GUCs
SET ducklake.default_table_path = '/tmp/test_path';
SHOW ducklake.default_table_path;
RESET ducklake.default_table_path;
SHOW ducklake.default_table_path;

SET ducklake.vacuum_delete_threshold = 0.5;
SHOW ducklake.vacuum_delete_threshold;
RESET ducklake.vacuum_delete_threshold;

SET ducklake.enable_direct_insert = false;
SHOW ducklake.enable_direct_insert;
RESET ducklake.enable_direct_insert;

SHOW ducklake.enable_metadata_sync;
SET ducklake.enable_metadata_sync = false;
SHOW ducklake.enable_metadata_sync;

-- DML should still work with metadata sync disabled
CREATE TABLE guc_sync_test (id int, val text) USING ducklake;
INSERT INTO guc_sync_test VALUES (1, 'hello');
SELECT * FROM guc_sync_test;
DROP TABLE guc_sync_test;

RESET ducklake.enable_metadata_sync;
