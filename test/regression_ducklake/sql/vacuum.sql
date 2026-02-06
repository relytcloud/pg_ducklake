-- Test VACUUM on DuckLake tables

-- Ensure data inlining is disabled so we create actual files
SELECT ducklake.set_option('data_inlining_row_limit', 0);

-- Scenario 1: Merge adjacent files
CREATE TABLE vacuum_merge (a int, b text) USING ducklake;

-- Create multiple parquet files
INSERT INTO vacuum_merge VALUES (1, 'one');
INSERT INTO vacuum_merge VALUES (2, 'two');
INSERT INTO vacuum_merge VALUES (3, 'three');
INSERT INTO vacuum_merge VALUES (4, 'four');
INSERT INTO vacuum_merge VALUES (5, 'five');

SELECT * FROM vacuum_merge ORDER BY a;

-- Check file count before vacuum
SELECT count(*)
FROM ducklake.ducklake_data_file ddf
JOIN ducklake.ducklake_table dt ON ddf.table_id = dt.table_id
JOIN ducklake.ducklake_schema ds ON dt.schema_id = ds.schema_id
WHERE dt.table_name = 'vacuum_merge'
  AND ds.schema_name = 'public'
  AND ddf.end_snapshot IS NULL;

-- Should trigger ducklake_merge_adjacent_files
VACUUM VERBOSE vacuum_merge;

SELECT * FROM vacuum_merge ORDER BY a;

-- Check file count after vacuum
SELECT count(*)
FROM ducklake.ducklake_data_file ddf
JOIN ducklake.ducklake_table dt ON ddf.table_id = dt.table_id
JOIN ducklake.ducklake_schema ds ON dt.schema_id = ds.schema_id
WHERE dt.table_name = 'vacuum_merge'
  AND ds.schema_name = 'public'
  AND ddf.end_snapshot IS NULL;

DROP TABLE vacuum_merge;

-- Scenario 2: Rewrite data files
CREATE TABLE vacuum_rewrite (a int, b text) USING ducklake;

INSERT INTO vacuum_rewrite SELECT i, 'val' || i FROM generate_series(1, 100) i;

-- Trigger rewrite threshold
DELETE FROM vacuum_rewrite WHERE a <= 20;

SELECT count(*) FROM vacuum_rewrite;

-- Check file count before vacuum
SELECT count(*)
FROM ducklake.ducklake_data_file ddf
JOIN ducklake.ducklake_table dt ON ddf.table_id = dt.table_id
JOIN ducklake.ducklake_schema ds ON dt.schema_id = ds.schema_id
WHERE dt.table_name = 'vacuum_rewrite'
  AND ds.schema_name = 'public'
  AND ddf.end_snapshot IS NULL;

-- Check delete file count before vacuum (should be > 0)
SELECT count(*)
FROM ducklake.ducklake_delete_file ddf
JOIN ducklake.ducklake_table dt ON ddf.table_id = dt.table_id
JOIN ducklake.ducklake_schema ds ON dt.schema_id = ds.schema_id
WHERE dt.table_name = 'vacuum_rewrite'
  AND ds.schema_name = 'public'
  AND ddf.end_snapshot IS NULL;

-- Should trigger ducklake_rewrite_data_files
VACUUM VERBOSE vacuum_rewrite;

SELECT count(*) FROM vacuum_rewrite;

-- Check file count after vacuum
SELECT count(*)
FROM ducklake.ducklake_data_file ddf
JOIN ducklake.ducklake_table dt ON ddf.table_id = dt.table_id
JOIN ducklake.ducklake_schema ds ON dt.schema_id = ds.schema_id
WHERE dt.table_name = 'vacuum_rewrite'
  AND ds.schema_name = 'public'
  AND ddf.end_snapshot IS NULL;

-- Check delete file count after vacuum (should be 0)
SELECT count(*)
FROM ducklake.ducklake_delete_file ddf
JOIN ducklake.ducklake_table dt ON ddf.table_id = dt.table_id
JOIN ducklake.ducklake_schema ds ON dt.schema_id = ds.schema_id
WHERE dt.table_name = 'vacuum_rewrite'
  AND ds.schema_name = 'public'
  AND ddf.end_snapshot IS NULL;

DROP TABLE vacuum_rewrite;
