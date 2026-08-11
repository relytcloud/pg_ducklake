-- Upstream: test/sql/table_changes/ducklake_table_changes.test
-- Use captured snapshots and relative assertions rather than catalog-global snapshot numbers.

CREATE TABLE upstream_table_changes (i integer) USING ducklake;
SELECT max(r['snapshot_id']::bigint) AS screate FROM ducklake.snapshots() AS r \gset
INSERT INTO upstream_table_changes SELECT g FROM generate_series(0, 2) AS g;
SELECT max(r['snapshot_id']::bigint) AS sinsert FROM ducklake.snapshots() AS r \gset
UPDATE upstream_table_changes SET i = i + 100;
SELECT max(r['snapshot_id']::bigint) AS supdate1 FROM ducklake.snapshots() AS r \gset
UPDATE upstream_table_changes SET i = i + 100;
SELECT max(r['snapshot_id']::bigint) AS supdate2 FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_table_changes;
SELECT max(r['snapshot_id']::bigint) AS sdelete FROM ducklake.snapshots() AS r \gset

SELECT r['snapshot_id']::bigint = :sinsert AS expected_snapshot,
       r['rowid']::bigint, r['change_type']::text, r['i']::integer
FROM ducklake.table_changes('upstream_table_changes'::regclass, :screate, :sinsert) AS r
ORDER BY 2;
SELECT r['snapshot_id']::bigint = :supdate1 AS expected_snapshot,
       r['rowid']::bigint, r['change_type']::text, r['i']::integer
FROM ducklake.table_changes('upstream_table_changes'::regclass, :supdate1, :supdate1) AS r
ORDER BY 2, 3, 4;
SELECT r['snapshot_id']::bigint = :supdate2 AS expected_snapshot,
       r['rowid']::bigint, r['change_type']::text, r['i']::integer
FROM ducklake.table_changes('upstream_table_changes'::regclass, :supdate2, :supdate2) AS r
ORDER BY 2, 3, 4;
SELECT r['snapshot_id']::bigint = :sdelete AS expected_snapshot,
       r['rowid']::bigint, r['change_type']::text, r['i']::integer
FROM ducklake.table_changes('upstream_table_changes'::regclass, :sdelete, :sdelete) AS r
ORDER BY 2;
SELECT count(*) = 18 AS complete_change_feed
FROM ducklake.table_changes('upstream_table_changes'::regclass, :screate, :sdelete);
SELECT r['snapshot_time']::text AS insert_time
FROM ducklake.snapshots() AS r
WHERE r['snapshot_id']::bigint = :sinsert \gset
SELECT count(*) = 3 AS timestamp_overload_matches
FROM ducklake.table_changes(
  'upstream_table_changes'::regclass,
  :'insert_time'::timestamptz, :'insert_time'::timestamptz
);
DROP TABLE upstream_table_changes;
