-- Upstream: test/sql/partitioning/bucket_pruning.test
-- Skip: Per-file pruning counts are not exposed by PostgreSQL EXPLAIN; keep this pruning test unscheduled.
-- Equality and range predicates on bucket-partitioned data must not lose rows.
CREATE TABLE upstream_bucket_pruning (i integer) USING ducklake;
CALL ducklake.set_partition('upstream_bucket_pruning'::regclass, 'bucket(10, i)');
INSERT INTO upstream_bucket_pruning SELECT i FROM generate_series(-100, 100) AS g(i);
SELECT count(*) FROM upstream_bucket_pruning WHERE i = 0;
SELECT min(i), max(i), count(*) FROM upstream_bucket_pruning WHERE i BETWEEN -3 AND 3;
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_bucket_pruning'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT count(DISTINCT fpv.partition_value) AS bucket_values,
       min(fpv.partition_value::integer) AS min_bucket,
       max(fpv.partition_value::integer) AS max_bucket
FROM ducklake.ducklake_file_partition_value fpv
JOIN ducklake.ducklake_data_file f USING (data_file_id)
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_bucket_pruning'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT * FROM ducklake.get_partition('upstream_bucket_pruning'::regclass);
DROP TABLE upstream_bucket_pruning;
