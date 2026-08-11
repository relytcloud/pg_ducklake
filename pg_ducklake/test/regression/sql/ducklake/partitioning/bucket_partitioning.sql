-- Upstream: test/sql/partitioning/bucket_partitioning.test
-- Skip: Physical bucket/hash placement is not exposed by the PostgreSQL API; keep this layout test unscheduled.
-- Bucket transforms preserve data across inserts and inline-data flushes.
CREATE TABLE upstream_bucket_partition (user_id text, value integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_bucket_partition'::regclass);
CALL ducklake.set_partition('upstream_bucket_partition'::regclass, 'bucket(4, user_id)');
INSERT INTO upstream_bucket_partition VALUES ('alice', 1), ('bob', 2), ('charlie', 3);
INSERT INTO upstream_bucket_partition VALUES ('alice', 4), ('bob', 5);
SELECT user_id, sum(value) FROM upstream_bucket_partition GROUP BY user_id ORDER BY user_id;
SELECT * FROM ducklake.get_partition('upstream_bucket_partition'::regclass);
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_bucket_partition'::regclass);
SELECT count(*) AS active_files,
       count(DISTINCT fpv.partition_value) AS represented_buckets,
       bool_and(fpv.partition_value::integer BETWEEN 0 AND 3) AS buckets_in_range
FROM ducklake.ducklake_data_file f
JOIN ducklake.ducklake_file_partition_value fpv USING (data_file_id)
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_bucket_partition'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT user_id, sum(value) FROM upstream_bucket_partition GROUP BY user_id ORDER BY user_id;
DROP TABLE upstream_bucket_partition;
