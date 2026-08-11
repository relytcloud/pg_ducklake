-- Upstream: test/sql/functions/ducklake_table_info.test

CREATE TABLE upstream_table_info (i integer) USING ducklake;
INSERT INTO upstream_table_info SELECT g FROM generate_series(0, 999) AS g;
DELETE FROM upstream_table_info WHERE i % 2 = 0;
SELECT r['table_name']::text,
       r['file_count']::bigint > 0 AS has_files,
       r['file_size_bytes']::bigint > 0 AS has_file_bytes,
       r['delete_file_count']::bigint > 0 AS has_delete_files
FROM ducklake.table_info() AS r
WHERE r['table_name']::text = 'upstream_table_info';
DROP TABLE upstream_table_info;
