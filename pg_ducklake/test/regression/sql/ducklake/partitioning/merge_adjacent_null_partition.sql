-- Upstream: test/sql/partitioning/merge_adjacent_null_partition.test
-- Skip: add_data_files with Hive NULL partition paths is not exposed by the PostgreSQL API.
-- NULL and mixed-NULL partition groups compact independently without changing rows.
CREATE TABLE upstream_null_partition_merge (id integer, tag text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_null_partition_merge'::regclass);
CALL ducklake.set_partition('upstream_null_partition_merge'::regclass, 'tag');
INSERT INTO upstream_null_partition_merge VALUES (1, NULL);
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_null_partition_merge'::regclass);
INSERT INTO upstream_null_partition_merge VALUES (2, NULL);
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_null_partition_merge'::regclass);
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_null_partition_merge'::regclass);
SELECT count(*) AS active_files,
       count(*) FILTER (WHERE fpv.partition_value IS NULL) AS null_partition_values
FROM ducklake.ducklake_data_file f
JOIN ducklake.ducklake_file_partition_value fpv USING (data_file_id)
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_null_partition_merge'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT id, tag FROM upstream_null_partition_merge ORDER BY id;

CREATE TABLE upstream_multi_null_merge (id integer, a text, b text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_multi_null_merge'::regclass);
CALL ducklake.set_partition('upstream_multi_null_merge'::regclass, 'a', 'b');
INSERT INTO upstream_multi_null_merge VALUES (1, NULL, NULL);
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_multi_null_merge'::regclass);
INSERT INTO upstream_multi_null_merge VALUES (2, NULL, NULL);
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_multi_null_merge'::regclass);
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_multi_null_merge'::regclass);
SELECT count(DISTINCT f.data_file_id) AS active_files,
       count(*) FILTER (WHERE fpv.partition_value IS NULL) AS null_partition_values
FROM ducklake.ducklake_data_file f
JOIN ducklake.ducklake_file_partition_value fpv USING (data_file_id)
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_multi_null_merge'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT id, a, b FROM upstream_multi_null_merge ORDER BY id;

CREATE TABLE upstream_mixed_null_merge (id integer, a text, b text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_mixed_null_merge'::regclass);
CALL ducklake.set_partition('upstream_mixed_null_merge'::regclass, 'a', 'b');
INSERT INTO upstream_mixed_null_merge VALUES (1,NULL,'x');
SELECT count(*) FROM ducklake.flush_inlined_data('upstream_mixed_null_merge'::regclass);
INSERT INTO upstream_mixed_null_merge VALUES (2,NULL,'x');
SELECT count(*) FROM ducklake.flush_inlined_data('upstream_mixed_null_merge'::regclass);
INSERT INTO upstream_mixed_null_merge VALUES (3,'y',NULL);
SELECT count(*) FROM ducklake.flush_inlined_data('upstream_mixed_null_merge'::regclass);
INSERT INTO upstream_mixed_null_merge VALUES (4,'y',NULL);
SELECT count(*) FROM ducklake.flush_inlined_data('upstream_mixed_null_merge'::regclass);
INSERT INTO upstream_mixed_null_merge VALUES (5,NULL,NULL);
SELECT count(*) FROM ducklake.flush_inlined_data('upstream_mixed_null_merge'::regclass);
INSERT INTO upstream_mixed_null_merge VALUES (6,'y','x');
SELECT count(*) FROM ducklake.flush_inlined_data('upstream_mixed_null_merge'::regclass);
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_mixed_null_merge'::regclass);
SELECT count(DISTINCT f.data_file_id) AS active_files,
       count(*) FILTER (WHERE fpv.partition_value IS NULL) AS null_partition_values,
       count(*) FILTER (WHERE fpv.partition_value IS NOT NULL) AS nonnull_partition_values
FROM ducklake.ducklake_data_file f
JOIN ducklake.ducklake_file_partition_value fpv USING (data_file_id)
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_mixed_null_merge'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT id, a, b FROM upstream_mixed_null_merge ORDER BY id;
DROP TABLE upstream_null_partition_merge, upstream_multi_null_merge, upstream_mixed_null_merge;
