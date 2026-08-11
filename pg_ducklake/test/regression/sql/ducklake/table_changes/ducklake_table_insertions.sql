-- Upstream: test/sql/table_changes/ducklake_table_insertions.test
-- Capture each separately flushed insert snapshot rather than assuming absolute IDs.

CREATE TABLE upstream_table_insertions (i integer) USING ducklake;
INSERT INTO upstream_table_insertions VALUES (1);
SELECT max(r['snapshot_id']::bigint) AS s1 FROM ducklake.snapshots() AS r \gset
SELECT * FROM ducklake.flush_inlined_data('upstream_table_insertions'::regclass);
INSERT INTO upstream_table_insertions VALUES (2);
SELECT max(r['snapshot_id']::bigint) AS s2 FROM ducklake.snapshots() AS r \gset
SELECT * FROM ducklake.flush_inlined_data('upstream_table_insertions'::regclass);
INSERT INTO upstream_table_insertions VALUES (3);
SELECT max(r['snapshot_id']::bigint) AS s3 FROM ducklake.snapshots() AS r \gset
SELECT * FROM ducklake.flush_inlined_data('upstream_table_insertions'::regclass);
INSERT INTO upstream_table_insertions VALUES (NULL);
SELECT max(r['snapshot_id']::bigint) AS s4 FROM ducklake.snapshots() AS r \gset
SELECT * FROM ducklake.flush_inlined_data('upstream_table_insertions'::regclass);
INSERT INTO upstream_table_insertions VALUES (10), (11);
SELECT max(r['snapshot_id']::bigint) AS s5 FROM ducklake.snapshots() AS r \gset
SELECT * FROM ducklake.flush_inlined_data('upstream_table_insertions'::regclass);
SELECT * FROM ducklake.merge_adjacent_files('upstream_table_insertions'::regclass);

SELECT r['rowid']::bigint, r['i']::integer
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :s1, :s1) AS r;
SELECT r['rowid']::bigint, r['i']::integer
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :s1, :s3) AS r
ORDER BY 1;
SELECT r['rowid']::bigint, r['i']::integer
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :s3, :s4) AS r
ORDER BY 1;
SELECT r['rowid']::bigint, r['i']::integer
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :s4, :s4) AS r;
UPDATE upstream_table_insertions SET i = i + 100 WHERE i < 11;
SELECT max(r['snapshot_id']::bigint) AS supdate FROM ducklake.snapshots() AS r \gset
SELECT r['rowid']::bigint, r['i']::integer
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :supdate, :supdate) AS r
ORDER BY 1;
SELECT r['rowid']::bigint, r['i']::integer
FROM ducklake.table_insertions('upstream_table_insertions'::regclass, :s1, :supdate) AS r
ORDER BY 1, 2 NULLS FIRST;
DROP TABLE upstream_table_insertions;
