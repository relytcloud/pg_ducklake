-- Upstream: test/sql/partitioning/multi_table_partition.test
-- Partition metadata for multiple tables can be set in one transaction.
CREATE TABLE upstream_multi_partition_one (part_key integer, val text) USING ducklake;
CREATE TABLE upstream_multi_partition_two (part_key integer, val text) USING ducklake;
BEGIN;
CALL ducklake.set_partition('upstream_multi_partition_one'::regclass, 'part_key');
CALL ducklake.set_partition('upstream_multi_partition_two'::regclass, 'part_key');
COMMIT;
SELECT * FROM ducklake.get_partition('upstream_multi_partition_one'::regclass);
SELECT * FROM ducklake.get_partition('upstream_multi_partition_two'::regclass);
DROP TABLE upstream_multi_partition_one, upstream_multi_partition_two;
