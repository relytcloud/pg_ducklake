-- Test VACUUM on DuckLake tables
CREATE TABLE vacuum_test (a int, b text) USING ducklake;

INSERT INTO vacuum_test VALUES (1, 'one'), (2, 'two'), (3, 'three');
SELECT * FROM vacuum_test ORDER BY a;

DELETE FROM vacuum_test WHERE a = 2;
SELECT * FROM vacuum_test ORDER BY a;

-- Should trigger ducklake_rewrite_data_files and ducklake_merge_adjacent_files
VACUUM vacuum_test;

SELECT * FROM vacuum_test ORDER BY a;

-- VACUUM FULL also triggers ducklake_cleanup_old_files
VACUUM FULL vacuum_test;

SELECT * FROM vacuum_test ORDER BY a;

DROP TABLE vacuum_test;
