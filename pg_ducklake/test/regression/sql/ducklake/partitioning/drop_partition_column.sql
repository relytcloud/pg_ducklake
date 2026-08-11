-- Upstream: test/sql/partitioning/drop_partition_column.test
-- A partition column can be dropped only after partitioning is reset.
CREATE TABLE upstream_drop_partition_column (part_key integer, val text) USING ducklake;
CALL ducklake.set_partition('upstream_drop_partition_column'::regclass, 'part_key');
INSERT INTO upstream_drop_partition_column VALUES (0, 'a'), (1, 'b');
ALTER TABLE upstream_drop_partition_column DROP COLUMN part_key;
CALL ducklake.reset_partition('upstream_drop_partition_column'::regclass);
ALTER TABLE upstream_drop_partition_column DROP COLUMN part_key;
SELECT * FROM upstream_drop_partition_column ORDER BY val;
DROP TABLE upstream_drop_partition_column;
