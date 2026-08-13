-- Upstream: test/sql/table_changes/ducklake_table_insertions.test
-- Capture insert snapshots dynamically and expose row lineage through ducklake.rowid().

CALL ducklake.set_option('data_inlining_row_limit', 100);

CREATE TABLE upstream_table_insertions (i integer) USING ducklake;
INSERT INTO upstream_table_insertions VALUES (1);
SELECT max(r['snapshot_id']::bigint) AS s1 FROM ducklake.snapshots() AS r \gset
SELECT r['schema_name']::text AS schema_name, r['table_name']::text AS table_name,
       r['rows_flushed']::bigint AS rows_flushed
FROM ducklake.flush_inlined_data('upstream_table_insertions'::regclass) AS r;
INSERT INTO upstream_table_insertions VALUES (2);
SELECT max(r['snapshot_id']::bigint) AS s2 FROM ducklake.snapshots() AS r \gset
SELECT r['schema_name']::text AS schema_name, r['table_name']::text AS table_name,
       r['rows_flushed']::bigint AS rows_flushed
FROM ducklake.flush_inlined_data('upstream_table_insertions'::regclass) AS r;
INSERT INTO upstream_table_insertions VALUES (3);
SELECT max(r['snapshot_id']::bigint) AS s3 FROM ducklake.snapshots() AS r \gset
SELECT r['schema_name']::text AS schema_name, r['table_name']::text AS table_name,
       r['rows_flushed']::bigint AS rows_flushed
FROM ducklake.flush_inlined_data('upstream_table_insertions'::regclass) AS r;
INSERT INTO upstream_table_insertions VALUES (NULL);
SELECT max(r['snapshot_id']::bigint) AS s4 FROM ducklake.snapshots() AS r \gset
SELECT r['schema_name']::text AS schema_name, r['table_name']::text AS table_name,
       r['rows_flushed']::bigint AS rows_flushed
FROM ducklake.flush_inlined_data('upstream_table_insertions'::regclass) AS r;
INSERT INTO upstream_table_insertions VALUES (10), (11);
SELECT max(r['snapshot_id']::bigint) AS s5 FROM ducklake.snapshots() AS r \gset
SELECT r['schema_name']::text AS schema_name, r['table_name']::text AS table_name,
       r['rows_flushed']::bigint AS rows_flushed
FROM ducklake.flush_inlined_data('upstream_table_insertions'::regclass) AS r;
SELECT r['table_name']::text AS table_name,
       r['files_processed']::bigint AS files_processed,
       r['files_created']::bigint AS files_created
FROM ducklake.merge_adjacent_files('upstream_table_insertions'::regclass) AS r;

SELECT ducklake.rowid() AS rowid, r['i']::integer AS i
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :s1, :s1) AS r;
SELECT ducklake.rowid() AS rowid, r['i']::integer AS i
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :s1, :s2) AS r
ORDER BY rowid;
SELECT ducklake.rowid() AS rowid, r['i']::integer AS i
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :s1, :s3) AS r
ORDER BY rowid;
SELECT ducklake.rowid() AS rowid, r['i']::integer AS i
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :s3, :s4) AS r
ORDER BY rowid;
SELECT ducklake.rowid() AS rowid, r['i']::integer AS i
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :s4, :s4) AS r;

UPDATE upstream_table_insertions SET i = i + 100 WHERE i < 11;
SELECT max(r['snapshot_id']::bigint) AS supdate FROM ducklake.snapshots() AS r \gset
SELECT ducklake.rowid() AS rowid, r['i']::integer AS i
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :supdate, :supdate) AS r
ORDER BY rowid;
SELECT ducklake.rowid() AS rowid, r['i']::integer AS i
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :s1, :supdate) AS r
ORDER BY rowid, i NULLS FIRST;
DROP TABLE upstream_table_insertions;
DELETE FROM ducklake.ducklake_metadata
WHERE key = 'data_inlining_row_limit' AND scope IS NULL;
