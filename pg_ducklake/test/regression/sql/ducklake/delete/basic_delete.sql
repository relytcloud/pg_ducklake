-- Upstream: test/sql/delete/basic_delete.test
-- DELETE visibility must be transactional and historical snapshots remain readable.

CREATE TABLE upstream_basic_delete USING ducklake AS
SELECT i AS id FROM generate_series(0, 999) AS g(i);
INSERT INTO upstream_basic_delete
SELECT i FROM generate_series(15000, 15999) AS g(i);
SELECT max(snapshot_id) AS before_delete FROM ducklake.ducklake_snapshot \gset

BEGIN;
DELETE FROM upstream_basic_delete WHERE id % 2 = 0;
SELECT count(*), count(*) FILTER (WHERE id % 2 = 0) FROM upstream_basic_delete;
COMMIT;

SELECT count(*), count(*) FILTER (WHERE id % 2 = 0) FROM upstream_basic_delete;
SELECT count(*), count(*) FILTER (WHERE r['id']::integer % 2 = 0)
FROM ducklake.time_travel('upstream_basic_delete', :before_delete) AS r;

DROP TABLE upstream_basic_delete;
