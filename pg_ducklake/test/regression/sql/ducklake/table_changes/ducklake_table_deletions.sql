-- Upstream: test/sql/table_changes/ducklake_table_deletions.test
-- Skip: pg_ducklake.table_deletions does not expose upstream's rowid or per-row snapshot_id virtual columns.

CREATE TABLE upstream_table_deletions (i integer) USING ducklake;
INSERT INTO upstream_table_deletions VALUES (1), (2), (3), (NULL), (10), (11), (12), (13), (14);
SELECT max(r['snapshot_id']::bigint) AS sinsert FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_table_deletions WHERE i IN (2, 11, 12);
SELECT max(r['snapshot_id']::bigint) AS sdelete FROM ducklake.snapshots() AS r \gset

SELECT r['i']::integer
FROM ducklake.table_deletions('upstream_table_deletions'::regclass, :sdelete, :sdelete) AS r
ORDER BY 1;
UPDATE upstream_table_deletions SET i = i + 100 WHERE i < 13;
SELECT max(r['snapshot_id']::bigint) AS supdate FROM ducklake.snapshots() AS r \gset
SELECT r['i']::integer
FROM ducklake.table_deletions('upstream_table_deletions'::regclass, :supdate, :supdate) AS r
ORDER BY 1;
SELECT count(*)
FROM ducklake.table_deletions('upstream_table_deletions'::regclass, :sinsert, :supdate);
DROP TABLE upstream_table_deletions;
