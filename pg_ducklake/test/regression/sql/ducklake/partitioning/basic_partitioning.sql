-- Upstream: test/sql/partitioning/basic_partitioning.test
-- Partition metadata survives renames, appends, and transactional setup.
CREATE TABLE upstream_basic_partition (part_key integer, val text) USING ducklake;
CALL ducklake.set_partition('upstream_basic_partition'::regclass, 'part_key');
INSERT INTO upstream_basic_partition
SELECT i % 2, 'value_' || i FROM generate_series(0, 9) AS g(i);
ALTER TABLE upstream_basic_partition RENAME TO upstream_basic_partition_renamed;
SELECT * FROM ducklake.get_partition('upstream_basic_partition_renamed'::regclass);
INSERT INTO upstream_basic_partition_renamed VALUES (2, 'appended');
SELECT part_key, count(*) FROM upstream_basic_partition_renamed GROUP BY part_key ORDER BY part_key;
CREATE TABLE upstream_basic_partition_tx (part_key integer, val text) USING ducklake;
BEGIN;
CALL ducklake.set_partition('upstream_basic_partition_tx'::regclass, 'part_key');
INSERT INTO upstream_basic_partition_tx VALUES (0, 'zero'), (1, 'one');
COMMIT;
SELECT * FROM ducklake.get_partition('upstream_basic_partition_tx'::regclass);
SELECT * FROM upstream_basic_partition_tx ORDER BY part_key;
DROP TABLE upstream_basic_partition_renamed, upstream_basic_partition_tx;
