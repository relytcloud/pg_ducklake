#pragma once

namespace pgducklake {

extern char *default_table_path;
extern char *azure_transport_option_type;
extern double vacuum_delete_threshold;
extern bool enable_direct_insert;
extern bool ctas_skip_data;
extern bool native_writer_reservation_queue;
extern int native_writer_reservation_queue_capacity;
extern int native_writer_reservation_queue_wait_ms;
extern int native_writer_max_retry_count;
extern int native_writer_retry_wait_ms;
extern double native_writer_retry_backoff;
extern bool test_native_writer_force_client_retry_before_rebase;

enum NativeWriterTestFault {
	NATIVE_WRITER_TEST_FAULT_OFF = 0,
	NATIVE_WRITER_TEST_FAULT_AFTER_PREWRITE,
	NATIVE_WRITER_TEST_FAULT_AFTER_CLAIM,
	NATIVE_WRITER_TEST_FAULT_AFTER_RETAG,
	NATIVE_WRITER_TEST_FAULT_AFTER_TABLE_STATS,
	NATIVE_WRITER_TEST_FAULT_AFTER_COLUMN_STATS,
	NATIVE_WRITER_TEST_FAULT_AFTER_CHANGE_RECORD,
	NATIVE_WRITER_TEST_FAULT_AFTER_PUBLICATION,
};
extern int native_writer_test_fault;

extern bool enable_metadata_sync;

extern int threads;

extern char *superuser_role;
extern char *writer_role;
extern char *reader_role;

extern bool maintenance_enabled;
extern int maintenance_naptime;
extern int maintenance_max_workers;
extern bool maintenance_flush_inlined_data;
extern bool maintenance_expire_snapshots;
extern bool maintenance_cleanup_old_files;

} // namespace pgducklake
