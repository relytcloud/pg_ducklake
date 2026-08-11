-- Upstream: test/sql/snapshot_info/ducklake_current_commit.test
-- Current snapshot advances only when a DuckLake transaction commits.

SELECT r['id']::bigint AS before_current
FROM ducklake.current_snapshot() AS r \gset
CREATE TABLE upstream_current_snapshot (i integer) USING ducklake;
SELECT (r['id']::bigint = :before_current + 1) AS advanced
FROM ducklake.current_snapshot() AS r;

BEGIN;
INSERT INTO upstream_current_snapshot VALUES (0);
SELECT (r['id']::bigint = :before_current + 1) AS unchanged_in_transaction
FROM ducklake.current_snapshot() AS r;
ROLLBACK;
SELECT (r['id']::bigint = :before_current + 1) AS unchanged_after_rollback
FROM ducklake.current_snapshot() AS r;

INSERT INTO upstream_current_snapshot VALUES (1);
SELECT (r['id']::bigint = :before_current + 2) AS advanced_after_commit
FROM ducklake.current_snapshot() AS r;
DROP TABLE upstream_current_snapshot;
