-- Upstream: test/sql/time_travel/basic_time_travel.test
-- Exercise version- and timestamp-based history using dynamic snapshot IDs.

SELECT max(r['snapshot_id']::bigint) AS vbase FROM ducklake.snapshots() AS r \gset
CREATE TABLE upstream_time_travel (i integer, j integer) USING ducklake;
SELECT max(r['snapshot_id']::bigint) AS vcreate FROM ducklake.snapshots() AS r \gset
INSERT INTO upstream_time_travel VALUES (1, 2), (NULL, 3);
SELECT max(r['snapshot_id']::bigint) AS vinsert FROM ducklake.snapshots() AS r \gset

SELECT r['i']::integer, r['j']::integer
FROM ducklake.time_travel('upstream_time_travel'::regclass, :vcreate) AS r;
SELECT r['i']::integer, r['j']::integer
FROM ducklake.time_travel('upstream_time_travel'::regclass, :vinsert) AS r
ORDER BY 1 NULLS LAST;
SELECT count(*) FROM ducklake.time_travel('upstream_time_travel'::regclass, now());
SELECT count(*) FROM ducklake.time_travel('upstream_time_travel'::regclass, :vbase);
SELECT * FROM ducklake.time_travel('upstream_time_travel'::regclass, :vinsert + 1000000);

SELECT r['snapshot_time']::text AS insert_time
FROM ducklake.snapshots() AS r
WHERE r['snapshot_id']::bigint = :vinsert \gset
SELECT count(*)
FROM ducklake.time_travel('upstream_time_travel'::regclass, :'insert_time'::timestamptz);

DROP TABLE upstream_time_travel;
SELECT r['i']::integer, r['j']::integer
FROM ducklake.time_travel('public', 'upstream_time_travel', :vinsert) AS r
ORDER BY 1 NULLS LAST;
