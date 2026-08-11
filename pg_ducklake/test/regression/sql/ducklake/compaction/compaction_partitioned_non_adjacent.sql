-- Upstream: test/sql/compaction/compaction_partitioned_non_adjacent.test
-- Non-adjacent files for each partition compact without crossing partitions.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_compact_nonadj (part_key integer, value integer) USING ducklake;
CALL ducklake.set_partition('upstream_compact_nonadj'::regclass, 'part_key');
INSERT INTO upstream_compact_nonadj VALUES (1, 10);
INSERT INTO upstream_compact_nonadj VALUES (2, 100);
INSERT INTO upstream_compact_nonadj VALUES (1, 20);
INSERT INTO upstream_compact_nonadj VALUES (2, 200);
SELECT * FROM ducklake.merge_adjacent_files('upstream_compact_nonadj'::regclass);
SELECT * FROM upstream_compact_nonadj ORDER BY part_key, value;
SELECT count(DISTINCT partition_id) FROM ducklake.ducklake_data_file
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'upstream_compact_nonadj' AND end_snapshot IS NULL)
  AND end_snapshot IS NULL;
DROP TABLE upstream_compact_nonadj;
