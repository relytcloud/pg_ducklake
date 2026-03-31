-- Test ducklake.options() and ducklake.set_option()

-- 0. options() should return rows without crashing (issue #136)
SELECT count(*) > 0 AS has_options FROM ducklake.options();

-- 1. Set a global option
CALL ducklake.set_option('data_inlining_row_limit', 100);

-- 2. Set a table-scoped option
CREATE TABLE options_test (a int) USING ducklake;

CALL ducklake.set_option('data_inlining_row_limit', 50, 'options_test'::regclass);

-- 3. Verify data insertion works with inlining enabled
INSERT INTO options_test VALUES (1), (2), (3);
SELECT * FROM options_test ORDER BY a;

-- 4. Test invalid option name
CALL ducklake.set_option('nonexistent_option', 42);

-- 5. Error: set_option on a non-ducklake table
CREATE TABLE regular_table (id int);
CALL ducklake.set_option('data_inlining_row_limit', 50, 'regular_table'::regclass);
DROP TABLE regular_table;

-- 6. Schema-scoped option (use target_file_size to avoid affecting data
--    inlining in later tests -- schema-scoped options can't be removed)
CALL ducklake.set_option('target_file_size', 1048576, 'public'::regnamespace);

-- 7. Error: schema-scoped option on nonexistent schema
CALL ducklake.set_option('target_file_size', 1048576, 'nonexistent_schema'::regnamespace);

-- Cleanup
CALL ducklake.set_option('data_inlining_row_limit', 0);
DROP TABLE options_test;
