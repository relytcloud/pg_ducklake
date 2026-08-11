-- Upstream: test/sql/partitioning/multi_key_partition.test
-- Skip: Physical partition paths and per-file pruning counts are not exposed by the PostgreSQL API.
-- Multiple identity keys are retained in order and predicates remain correct.
CREATE TABLE upstream_multi_key_partition (a integer, b integer, c integer, val text) USING ducklake;
CALL ducklake.set_partition('upstream_multi_key_partition'::regclass, 'a', 'b', 'c');
INSERT INTO upstream_multi_key_partition VALUES (10, 100, 1000, 'data 1'), (20, 200, 2000, 'data 2');
SELECT * FROM ducklake.get_partition('upstream_multi_key_partition'::regclass);
SELECT fpv.partition_key_index, fpv.partition_value
FROM ducklake.ducklake_file_partition_value fpv
JOIN ducklake.ducklake_data_file f USING (data_file_id)
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_multi_key_partition'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL
ORDER BY fpv.partition_key_index, fpv.partition_value;
SELECT a, b, c, val FROM upstream_multi_key_partition WHERE a = 10 ORDER BY a, b, c;
SELECT a, b, c, val FROM upstream_multi_key_partition WHERE b = 200 ORDER BY a, b, c;
DROP TABLE upstream_multi_key_partition;
