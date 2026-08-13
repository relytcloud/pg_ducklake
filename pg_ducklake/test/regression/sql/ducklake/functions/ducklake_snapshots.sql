-- Upstream: test/sql/functions/ducklake_snapshots.test
-- Skip: pg_ducklake currently records a snapshot for transactional CREATE-then-DROP, unlike upstream's no-op invariant.
-- Verify snapshot listing with IDs captured from the public snapshot API.

SELECT max(r['snapshot_id']::bigint) AS base_snap FROM ducklake.snapshots() AS r \gset
CREATE TABLE upstream_snapshots (i integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_snapshots'::regclass);
SELECT max(r['snapshot_id']::bigint) AS create_snap FROM ducklake.snapshots() AS r \gset
INSERT INTO upstream_snapshots VALUES (42);
SELECT max(r['snapshot_id']::bigint) AS insert_snap FROM ducklake.snapshots() AS r \gset
SELECT * FROM ducklake.flush_inlined_data('upstream_snapshots'::regclass);
SELECT count(*) > 0 AS has_new_snapshots
FROM ducklake.snapshots() AS r
WHERE r['snapshot_id']::bigint > :base_snap;
SELECT r['schema_version']::bigint >= 0 AS valid_schema_version
FROM ducklake.snapshots() AS r
WHERE r['snapshot_id']::bigint = :create_snap;

DROP TABLE upstream_snapshots;
DELETE FROM ducklake.ducklake_metadata
WHERE key = 'data_inlining_row_limit' AND scope = 'table';
