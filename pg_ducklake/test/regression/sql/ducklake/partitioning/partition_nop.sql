-- Upstream: test/sql/partitioning/partition_nop.test
-- Repeating identical set/reset operations is a metadata and snapshot no-op.
CREATE TABLE upstream_partition_nop (part_key integer, val text) USING ducklake;
CREATE TABLE upstream_partition_nop_other (part_key integer, val text) USING ducklake;
CALL ducklake.set_partition('upstream_partition_nop'::regclass, 'part_key');
SELECT max(snapshot_id) AS after_first_set FROM ducklake.ducklake_snapshot \gset
CALL ducklake.set_partition('upstream_partition_nop'::regclass, 'part_key');
SELECT max(snapshot_id) = :after_first_set AS identical_set_is_noop FROM ducklake.ducklake_snapshot;
SELECT * FROM ducklake.get_partition('upstream_partition_nop'::regclass);
CALL ducklake.set_partition('upstream_partition_nop'::regclass, 'part_key', 'val');
SELECT max(snapshot_id) AS after_two_keys FROM ducklake.ducklake_snapshot \gset
CALL ducklake.set_partition('upstream_partition_nop'::regclass, 'part_key', 'val');
SELECT max(snapshot_id) = :after_two_keys AS identical_two_key_set_is_noop FROM ducklake.ducklake_snapshot;
SELECT * FROM ducklake.get_partition('upstream_partition_nop'::regclass);
CALL ducklake.set_partition('upstream_partition_nop'::regclass, 'val', 'part_key');
SELECT * FROM ducklake.get_partition('upstream_partition_nop'::regclass);
CALL ducklake.set_partition('upstream_partition_nop'::regclass, 'part_key', 'val');
SELECT * FROM ducklake.get_partition('upstream_partition_nop'::regclass);
CALL ducklake.set_partition('upstream_partition_nop_other'::regclass, 'val');
SELECT * FROM ducklake.get_partition('upstream_partition_nop_other'::regclass);
CALL ducklake.reset_partition('upstream_partition_nop'::regclass);
SELECT max(snapshot_id) AS after_reset FROM ducklake.ducklake_snapshot \gset
CALL ducklake.reset_partition('upstream_partition_nop'::regclass);
SELECT max(snapshot_id) = :after_reset AS identical_reset_is_noop FROM ducklake.ducklake_snapshot;
SELECT count(*) AS active_partition_keys FROM ducklake.get_partition('upstream_partition_nop'::regclass);
SELECT * FROM ducklake.get_partition('upstream_partition_nop_other'::regclass);
DROP TABLE upstream_partition_nop, upstream_partition_nop_other;
