-- Upstream: test/sql/update/update_partitioning.test
-- Skip: PostgreSQL EXPLAIN does not expose DuckDB's exact per-partition files-read count.
-- Updating a partition key must move rows while preserving historical data.

SET datestyle TO ISO;
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_update_partitioned (part_key integer, val text) USING ducklake;
CALL ducklake.set_partition('upstream_update_partitioned'::regclass, 'part_key');
INSERT INTO upstream_update_partitioned
SELECT i % 2, 'thisisastring_' || i FROM generate_series(0, 9999) AS g(i);
SELECT max(snapshot_id) AS before_partition_update FROM ducklake.ducklake_snapshot \gset

UPDATE upstream_update_partitioned SET part_key = 2 WHERE part_key = 0;

SELECT count(*) AS file_versions,
       count(*) FILTER (WHERE f.path LIKE '%part_key=0/%') AS old_zero_files,
       count(*) FILTER (WHERE f.path LIKE '%part_key=1/%') AS one_files,
       count(*) FILTER (WHERE f.path LIKE '%part_key=2/%') AS new_two_files
FROM ducklake.ducklake_data_file f
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_update_partitioned';
SELECT count(*) FROM upstream_update_partitioned;
SELECT part_key, count(*) FROM upstream_update_partitioned GROUP BY part_key ORDER BY part_key;
SELECT count(*) FROM ducklake.time_travel(
    'upstream_update_partitioned', :before_partition_update
) AS r WHERE r['part_key']::integer = 0;
SELECT * FROM ducklake.get_partition('upstream_update_partitioned'::regclass);

DROP TABLE upstream_update_partitioned;
