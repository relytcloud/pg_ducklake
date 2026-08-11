-- Upstream: test/sql/data_inlining/data_inlining_table_changes.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_changes (i integer) USING ducklake;
SELECT max(snapshot_id) AS vcreate FROM ducklake.ducklake_snapshot \gset
INSERT INTO upstream_inline_changes SELECT g FROM generate_series(0, 2) g;
SELECT max(snapshot_id) AS vins FROM ducklake.ducklake_snapshot \gset
UPDATE upstream_inline_changes SET i = i + 100;
SELECT max(snapshot_id) AS vup1 FROM ducklake.ducklake_snapshot \gset
UPDATE upstream_inline_changes SET i = i + 100;
SELECT max(snapshot_id) AS vup2 FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_inline_changes;
SELECT max(snapshot_id) AS vdel FROM ducklake.ducklake_snapshot \gset
SELECT bool_and(r['snapshot_id']::bigint = :vins) AS correct_snapshot,
       r['change_type']::text AS change_type, count(*) AS rows,
       min(r['i']::integer) AS min_i, max(r['i']::integer) AS max_i
FROM ducklake.table_changes('upstream_inline_changes'::regclass, :vcreate, :vins) AS r
GROUP BY r['change_type']::text ORDER BY change_type;
SELECT bool_and(r['snapshot_id']::bigint = :vup1) AS correct_snapshot,
       r['change_type']::text AS change_type, count(*) AS rows,
       min(r['i']::integer) AS min_i, max(r['i']::integer) AS max_i
FROM ducklake.table_changes('upstream_inline_changes'::regclass, :vup1, :vup1) AS r
GROUP BY r['change_type']::text ORDER BY change_type;
SELECT bool_and(r['snapshot_id']::bigint = :vup2) AS correct_snapshot,
       r['change_type']::text AS change_type, count(*) AS rows,
       min(r['i']::integer) AS min_i, max(r['i']::integer) AS max_i
FROM ducklake.table_changes('upstream_inline_changes'::regclass, :vup2, :vup2) AS r
GROUP BY r['change_type']::text ORDER BY change_type;
SELECT bool_and(r['snapshot_id']::bigint = :vdel) AS correct_snapshot,
       r['change_type']::text AS change_type, count(*) AS rows,
       min(r['i']::integer) AS min_i, max(r['i']::integer) AS max_i
FROM ducklake.table_changes('upstream_inline_changes'::regclass, :vdel, :vdel) AS r
GROUP BY r['change_type']::text ORDER BY change_type;
SELECT count(*) AS total_changes,
       count(DISTINCT r['snapshot_id']::bigint) AS change_snapshots
FROM ducklake.table_changes('upstream_inline_changes'::regclass, :vcreate, :vdel) AS r;
DROP TABLE upstream_inline_changes;
CALL ducklake.set_option('data_inlining_row_limit', 0);
