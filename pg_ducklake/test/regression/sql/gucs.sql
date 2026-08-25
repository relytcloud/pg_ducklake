-- Test DuckLake GUCs
SHOW ducklake.azure_transport_option_type;
SHOW ducklake.default_table_path;
SHOW ducklake.vacuum_delete_threshold;
SHOW ducklake.enable_direct_insert;
SHOW ducklake.native_writer_reservation_queue;
SHOW ducklake.native_writer_reservation_queue_capacity;
SHOW ducklake.native_writer_reservation_queue_wait_ms;
SHOW ducklake.native_writer_max_retry_count;
SHOW ducklake.native_writer_retry_wait_ms;
SHOW ducklake.native_writer_retry_backoff;
SHOW ducklake.threads;

-- Test setting GUCs
SET ducklake.azure_transport_option_type = 'curl';
SHOW ducklake.azure_transport_option_type;
RESET ducklake.azure_transport_option_type;
SHOW ducklake.azure_transport_option_type;

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

SET ducklake.native_writer_reservation_queue = true;
SHOW ducklake.native_writer_reservation_queue;
RESET ducklake.native_writer_reservation_queue;

SET ducklake.native_writer_reservation_queue_wait_ms = 25;
SHOW ducklake.native_writer_reservation_queue_wait_ms;
RESET ducklake.native_writer_reservation_queue_wait_ms;

SET ducklake.native_writer_max_retry_count = 20;
SET ducklake.native_writer_retry_wait_ms = 5;
SET ducklake.native_writer_retry_backoff = 2;
SHOW ducklake.native_writer_max_retry_count;
SHOW ducklake.native_writer_retry_wait_ms;
SHOW ducklake.native_writer_retry_backoff;
RESET ducklake.native_writer_max_retry_count;
RESET ducklake.native_writer_retry_wait_ms;
RESET ducklake.native_writer_retry_backoff;

SET ducklake.threads = 4;
SHOW ducklake.threads;
RESET ducklake.threads;
SHOW ducklake.threads;

SHOW ducklake.enable_metadata_sync;
SET ducklake.enable_metadata_sync = false;
SHOW ducklake.enable_metadata_sync;

-- DML should still work with metadata sync disabled
CREATE TABLE guc_sync_test (id int, val text) USING ducklake;
INSERT INTO guc_sync_test VALUES (1, 'hello');
SELECT * FROM guc_sync_test;
DROP TABLE guc_sync_test;

RESET ducklake.enable_metadata_sync;
