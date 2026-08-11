-- Upstream: test/sql/compaction/compaction_partitioned_table.test
-- Adjacent small files are compacted independently in a partitioned table.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_compact_partitioned (part_key integer, value integer) USING ducklake;
CALL ducklake.set_partition('upstream_compact_partitioned'::regclass, 'part_key');
INSERT INTO upstream_compact_partitioned VALUES (1, 10);
INSERT INTO upstream_compact_partitioned VALUES (1, 20);
INSERT INTO upstream_compact_partitioned VALUES (2, 100);
INSERT INTO upstream_compact_partitioned VALUES (2, 200);
SELECT * FROM ducklake.merge_adjacent_files('upstream_compact_partitioned'::regclass);
SELECT * FROM upstream_compact_partitioned ORDER BY part_key, value;
DROP TABLE upstream_compact_partitioned;
