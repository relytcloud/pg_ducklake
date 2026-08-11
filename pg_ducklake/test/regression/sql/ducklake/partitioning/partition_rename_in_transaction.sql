-- Upstream: test/sql/partitioning/partition_rename_in_transaction.test
-- Renaming a newly partitioned table must preserve its partition metadata.

SET datestyle TO ISO;

BEGIN;
CREATE TABLE upstream_partition_rename (dt date) USING ducklake;
CALL ducklake.set_partition('upstream_partition_rename'::regclass, 'dt');
INSERT INTO upstream_partition_rename VALUES
    (DATE '2026-04-30'),
    (DATE '2026-04-29');
ALTER TABLE upstream_partition_rename RENAME TO upstream_partition_renamed;
COMMIT;

SELECT * FROM ducklake.get_partition('upstream_partition_renamed'::regclass);
SELECT * FROM upstream_partition_renamed ORDER BY dt;

DROP TABLE upstream_partition_renamed;
