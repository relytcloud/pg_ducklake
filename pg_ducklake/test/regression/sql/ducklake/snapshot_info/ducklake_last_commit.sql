-- Upstream: test/sql/snapshot_info/ducklake_last_commit.test
-- Last committed snapshot follows commits and ignores rollbacks.

SELECT r['id']::bigint IS NULL AS initially_unset
FROM ducklake.last_committed_snapshot() AS r;
CREATE TABLE upstream_last_snapshot (i integer) USING ducklake;
SELECT r['id']::bigint AS created_snapshot
FROM ducklake.last_committed_snapshot() AS r \gset
SELECT r['id']::bigint = :created_snapshot AS create_visible
FROM ducklake.last_committed_snapshot() AS r;

BEGIN;
INSERT INTO upstream_last_snapshot VALUES (0);
SELECT r['id']::bigint = :created_snapshot AS uncommitted_ignored
FROM ducklake.last_committed_snapshot() AS r;
ROLLBACK;
SELECT r['id']::bigint = :created_snapshot AS rollback_ignored
FROM ducklake.last_committed_snapshot() AS r;

INSERT INTO upstream_last_snapshot VALUES (1);
SELECT r['id']::bigint = :created_snapshot + 1 AS commit_visible
FROM ducklake.last_committed_snapshot() AS r;
DROP TABLE upstream_last_snapshot;
