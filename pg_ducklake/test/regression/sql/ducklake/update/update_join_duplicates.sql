-- Upstream: test/sql/update/update_join_duplicates.test
-- Duplicate source matches must update each target row at most once.

CREATE TABLE upstream_update_join USING ducklake AS
SELECT i AS id FROM generate_series(0, 4) AS g(i);
CREATE TEMP TABLE upstream_updated_rows AS
SELECT i AS update_id FROM generate_series(0, 8, 2) AS g(i)
UNION ALL
SELECT i FROM generate_series(0, 8, 2) AS g(i);

BEGIN;
INSERT INTO upstream_update_join SELECT i FROM generate_series(5, 9) AS g(i);
UPDATE upstream_update_join t SET id = t.id + 1000
FROM upstream_updated_rows u WHERE t.id = u.update_id;
SELECT count(*), sum(id), avg(id) FROM upstream_update_join;
COMMIT;
SELECT count(*), sum(id), avg(id) FROM upstream_update_join;

DROP TABLE upstream_update_join;
CREATE TABLE upstream_update_join USING ducklake AS
SELECT i AS id FROM generate_series(0, 4) AS g(i);
INSERT INTO upstream_update_join SELECT i FROM generate_series(5, 9) AS g(i);
UPDATE upstream_update_join t SET id = t.id + 1000
FROM (SELECT DISTINCT update_id FROM upstream_updated_rows) u
WHERE t.id = u.update_id;
SELECT count(*), sum(id), avg(id) FROM upstream_update_join;

DROP TABLE upstream_update_join;
CREATE TABLE upstream_update_join USING ducklake AS SELECT 1 AS id;
CREATE TEMP TABLE upstream_dup_source AS
SELECT 1 AS update_id FROM generate_series(1, 10000);
UPDATE upstream_update_join t SET id = t.id + 1000
FROM upstream_dup_source u WHERE t.id = u.update_id;
SELECT id FROM upstream_update_join;

DROP TABLE upstream_update_join;
DROP TABLE upstream_dup_source;
DROP TABLE upstream_updated_rows;
