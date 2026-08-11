-- Upstream: test/sql/merge/merge_partition_update.test
-- MERGE and UPDATE must write replacement files into the correct partition.

CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_merge_partition_update (
    ts timestamp, x double precision
) USING ducklake;
CALL ducklake.set_partition('upstream_merge_partition_update'::regclass, 'year(ts)');
INSERT INTO upstream_merge_partition_update VALUES (TIMESTAMP '2025-09-17', 42);

MERGE INTO upstream_merge_partition_update AS old
USING (VALUES (TIMESTAMP '2025-09-17', 43::double precision)) AS new(ts, x)
ON old.ts = new.ts
WHEN MATCHED THEN UPDATE SET x = new.x;

SELECT count(*) AS files_after_merge_in_2025_partition
FROM ducklake.ducklake_data_file f
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_merge_partition_update'
  AND f.path LIKE '%year=2025/%';

UPDATE upstream_merge_partition_update
SET x = 43 WHERE ts = TIMESTAMP '2025-09-17';

SELECT count(*) AS files_after_update_in_2025_partition
FROM ducklake.ducklake_data_file f
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_merge_partition_update'
  AND f.path LIKE '%year=2025/%';
SELECT * FROM upstream_merge_partition_update;

DROP TABLE upstream_merge_partition_update;
