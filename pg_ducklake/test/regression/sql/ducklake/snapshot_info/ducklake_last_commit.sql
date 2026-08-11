-- Upstream: test/sql/snapshot_info/ducklake_last_commit.test
-- Last committed snapshot follows commits and ignores rollbacks.

SELECT r['id']::bigint AS before_last
FROM ducklake.last_committed_snapshot() AS r \gset
CREATE TABLE upstream_last_snapshot (i integer) USING ducklake;
SELECT (r['id']::bigint = :before_last + 1) AS advanced
FROM ducklake.last_committed_snapshot() AS r;
BEGIN;
INSERT INTO upstream_last_snapshot VALUES (0);
ROLLBACK;
SELECT (r['id']::bigint = :before_last + 1) AS rollback_ignored
FROM ducklake.last_committed_snapshot() AS r;
INSERT INTO upstream_last_snapshot VALUES (1);
SELECT (r['id']::bigint = :before_last + 2) AS commit_visible
FROM ducklake.last_committed_snapshot() AS r;
DROP TABLE upstream_last_snapshot;
