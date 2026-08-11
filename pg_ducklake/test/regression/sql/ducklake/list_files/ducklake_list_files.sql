-- Upstream: test/sql/list_files/ducklake_list_files.test
-- The PostgreSQL API exposes current file lists through text and regclass overloads.

CREATE TABLE upstream_list_files (i integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 0, 'upstream_list_files'::regclass);
INSERT INTO upstream_list_files SELECT g FROM generate_series(0, 99) AS g;
INSERT INTO upstream_list_files SELECT g FROM generate_series(100, 199) AS g;
INSERT INTO upstream_list_files SELECT g FROM generate_series(200, 299) AS g;
SELECT count(*) FROM ducklake.list_files('public', 'upstream_list_files');
SELECT count(*) FROM ducklake.list_files('upstream_list_files'::regclass);
SELECT min(i), max(i), count(*), avg(i) FROM upstream_list_files;
DELETE FROM upstream_list_files WHERE i % 2 = 0 AND i < 150;
SELECT count(*) > 0 AS has_delete_files
FROM ducklake.list_files('upstream_list_files'::regclass) AS r
WHERE r['delete_file']::text IS NOT NULL;
SELECT count(*) FROM ducklake.list_files('public', 'upstream_list_files_missing');
DROP TABLE upstream_list_files;
