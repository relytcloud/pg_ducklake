-- Upstream: test/sql/update/basic_update.test
-- UPDATE must be visible in its transaction, persist on commit, and preserve history.

CREATE TABLE upstream_basic_update USING ducklake AS
SELECT 1000 + i AS id, i % 10 AS val FROM generate_series(0, 999) AS g(i);
SELECT max(snapshot_id) AS before_update FROM ducklake.ducklake_snapshot \gset

BEGIN;
SELECT count(*), sum(id), sum(val) FROM upstream_basic_update;
UPDATE upstream_basic_update SET id = id + 2 WHERE id % 2 = 0;
SELECT count(*), sum(id), sum(val) FROM upstream_basic_update;
COMMIT;

SELECT count(*), sum(id), sum(val) FROM upstream_basic_update;
SELECT count(*), sum(r['id']::bigint), sum(r['val']::bigint)
FROM ducklake.time_travel('upstream_basic_update', :before_update) AS r;

DROP TABLE upstream_basic_update;
