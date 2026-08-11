-- Upstream: test/sql/rewrite_data_files/test_rewrite_partitioning.test
-- Delete rewrite preserves partition membership and MERGE results.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CALL ducklake.set_option('rewrite_delete_threshold', 0);
CREATE TABLE upstream_rewrite_partitioned (part_key integer, id integer, value integer) USING ducklake;
CALL ducklake.set_partition('upstream_rewrite_partitioned'::regclass, 'part_key');
INSERT INTO upstream_rewrite_partitioned VALUES (1,1,10), (1,2,20), (2,1,100), (2,2,200);
CREATE TEMP TABLE upstream_rewrite_source(part_key integer, id integer, value integer);
INSERT INTO upstream_rewrite_source VALUES (1,1,15), (1,3,30), (2,1,150), (2,3,300);
MERGE INTO upstream_rewrite_partitioned AS target
USING upstream_rewrite_source AS source
ON target.part_key = source.part_key AND target.id = source.id
WHEN MATCHED THEN UPDATE SET value = source.value
WHEN NOT MATCHED THEN INSERT VALUES (source.part_key, source.id, source.value);
SELECT * FROM ducklake.rewrite_data_files('upstream_rewrite_partitioned'::regclass);
SELECT * FROM ducklake.merge_adjacent_files('upstream_rewrite_partitioned'::regclass);
SELECT * FROM upstream_rewrite_partitioned ORDER BY part_key, id;
DROP TABLE upstream_rewrite_source;
DROP TABLE upstream_rewrite_partitioned;
