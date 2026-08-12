-- Upstream: test/sql/rewrite_data_files/test_rewrite_preserves_row_id.test
-- Rewriting physical files preserves stable DuckLake row lineage identifiers.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CALL ducklake.set_option('rewrite_delete_threshold', 0);

CREATE TABLE upstream_rewrite_rowid (a integer, b integer) USING ducklake;
INSERT INTO upstream_rewrite_rowid SELECT i, i * 10 FROM generate_series(1, 10) AS g(i);
SELECT ducklake.rowid() AS rowid, a, b FROM upstream_rewrite_rowid ORDER BY a;
DELETE FROM upstream_rewrite_rowid WHERE a % 2 = 0;
SELECT ducklake.rowid() AS rowid, a, b FROM upstream_rewrite_rowid ORDER BY a;

CREATE TEMP TABLE upstream_rewrite_saved_rowids AS
SELECT a, ducklake.rowid() AS saved_rowid FROM upstream_rewrite_rowid;
SELECT count(*) = 1 AS delete_file_present
FROM ducklake.ducklake_delete_file df
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_rewrite_rowid' AND t.end_snapshot IS NULL
  AND df.end_snapshot IS NULL;
SELECT r['table_name']::text AS table_name,
       r['files_processed']::bigint AS files_processed,
       r['files_created']::bigint AS files_created
FROM ducklake.rewrite_data_files('upstream_rewrite_rowid'::regclass) AS r;
SELECT ducklake.rowid() AS rowid, a, b FROM upstream_rewrite_rowid ORDER BY a;
SELECT count(*) = 0 AS no_rowids_changed
FROM upstream_rewrite_rowid r
JOIN upstream_rewrite_saved_rowids s USING (a)
WHERE ducklake.rowid() <> s.saved_rowid;
SELECT count(*) = 0 AS delete_file_removed
FROM ducklake.ducklake_delete_file df
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_rewrite_rowid' AND t.end_snapshot IS NULL
  AND df.end_snapshot IS NULL;

CREATE TABLE upstream_rewrite_rowid_large (a integer, b integer) USING ducklake;
INSERT INTO upstream_rewrite_rowid_large
SELECT i, i * 10 FROM generate_series(1, 200) AS g(i);
SELECT count(*), min(ducklake.rowid()), max(ducklake.rowid())
FROM upstream_rewrite_rowid_large;
DELETE FROM upstream_rewrite_rowid_large WHERE a % 2 = 0;
SELECT count(*), min(ducklake.rowid()), max(ducklake.rowid())
FROM upstream_rewrite_rowid_large;
SELECT ducklake.rowid() AS rowid, a, b
FROM upstream_rewrite_rowid_large WHERE a <= 5 ORDER BY a;
SELECT ducklake.rowid() AS rowid, a, b
FROM upstream_rewrite_rowid_large WHERE a >= 195 ORDER BY a;
SELECT r['table_name']::text AS table_name,
       r['files_processed']::bigint AS files_processed,
       r['files_created']::bigint AS files_created
FROM ducklake.rewrite_data_files('upstream_rewrite_rowid_large'::regclass) AS r;
SELECT count(*), min(ducklake.rowid()), max(ducklake.rowid())
FROM upstream_rewrite_rowid_large;
SELECT ducklake.rowid() AS rowid, a, b
FROM upstream_rewrite_rowid_large WHERE a <= 5 ORDER BY a;
SELECT ducklake.rowid() AS rowid, a, b
FROM upstream_rewrite_rowid_large WHERE a >= 195 ORDER BY a;
SELECT count(*) FROM upstream_rewrite_rowid_large WHERE ducklake.rowid() % 2 <> 0;

DELETE FROM upstream_rewrite_rowid_large WHERE a % 4 = 1;
SELECT count(*) FROM upstream_rewrite_rowid_large;
SELECT ducklake.rowid() AS rowid, a, b
FROM upstream_rewrite_rowid_large WHERE a <= 15 ORDER BY a;
SELECT r['table_name']::text AS table_name,
       r['files_processed']::bigint AS files_processed,
       r['files_created']::bigint AS files_created
FROM ducklake.rewrite_data_files('upstream_rewrite_rowid_large'::regclass) AS r;
SELECT ducklake.rowid() AS rowid, a, b
FROM upstream_rewrite_rowid_large WHERE a <= 15 ORDER BY a;
SELECT count(*) FROM upstream_rewrite_rowid_large;

INSERT INTO upstream_rewrite_rowid VALUES (11, 110), (12, 120);
SELECT ducklake.rowid() AS rowid, a, b
FROM upstream_rewrite_rowid WHERE a > 10 ORDER BY a;
DELETE FROM upstream_rewrite_rowid WHERE a = 3;
SELECT ducklake.rowid() AS rowid, a, b FROM upstream_rewrite_rowid ORDER BY a;
SELECT r['table_name']::text AS table_name,
       r['files_processed']::bigint AS files_processed,
       r['files_created']::bigint AS files_created
FROM ducklake.rewrite_data_files('upstream_rewrite_rowid'::regclass) AS r;
SELECT ducklake.rowid() AS rowid, a, b FROM upstream_rewrite_rowid ORDER BY a;
SELECT count(*) >= 0 AS cleanup_completed FROM ducklake.cleanup_old_files();
SELECT ducklake.rowid() AS rowid, a, b FROM upstream_rewrite_rowid ORDER BY a;

DROP TABLE upstream_rewrite_saved_rowids;
DROP TABLE upstream_rewrite_rowid_large;
DROP TABLE upstream_rewrite_rowid;
