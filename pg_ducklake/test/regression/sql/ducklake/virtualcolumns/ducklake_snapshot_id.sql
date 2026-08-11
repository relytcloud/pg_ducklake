-- Upstream: test/sql/virtualcolumns/ducklake_snapshot_id.test

CREATE TABLE upstream_virtual_snapshot (i integer) USING ducklake;
INSERT INTO upstream_virtual_snapshot VALUES (1);
SELECT max(r['snapshot_id']::bigint) AS s1 FROM ducklake.snapshots() AS r \gset
INSERT INTO upstream_virtual_snapshot VALUES (2);
SELECT max(r['snapshot_id']::bigint) AS s2 FROM ducklake.snapshots() AS r \gset
INSERT INTO upstream_virtual_snapshot VALUES (3);
SELECT max(r['snapshot_id']::bigint) AS s3 FROM ducklake.snapshots() AS r \gset
INSERT INTO upstream_virtual_snapshot VALUES (NULL);
SELECT max(r['snapshot_id']::bigint) AS s4 FROM ducklake.snapshots() AS r \gset
SELECT CASE i WHEN 1 THEN ducklake.snapshot_id() = :s1
              WHEN 2 THEN ducklake.snapshot_id() = :s2
              WHEN 3 THEN ducklake.snapshot_id() = :s3
              ELSE ducklake.snapshot_id() = :s4 END AS expected_snapshot,
       i
FROM upstream_virtual_snapshot ORDER BY i NULLS LAST;
SELECT i FROM upstream_virtual_snapshot WHERE ducklake.snapshot_id() = :s2;
BEGIN;
INSERT INTO upstream_virtual_snapshot VALUES (10), (11);
SELECT ducklake.snapshot_id() IS NULL AS transaction_local, i
FROM upstream_virtual_snapshot WHERE i >= 10 ORDER BY i;
COMMIT;
SELECT max(r['snapshot_id']::bigint) AS scommit FROM ducklake.snapshots() AS r \gset
SELECT ducklake.snapshot_id() = :scommit AS committed_to_expected_snapshot, i
FROM upstream_virtual_snapshot WHERE i >= 10 ORDER BY i;
DROP TABLE upstream_virtual_snapshot;
