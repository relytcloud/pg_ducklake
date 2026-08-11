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
SELECT a, ducklake.rowid() AS rowid FROM upstream_rewrite_rowid;
SELECT count(*) = 1 AS delete_file_present
FROM ducklake.ducklake_delete_file df
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_rewrite_rowid' AND t.end_snapshot IS NULL
  AND df.end_snapshot IS NULL;
SELECT * FROM ducklake.rewrite_data_files('upstream_rewrite_rowid'::regclass);
SELECT ducklake.rowid() AS rowid, a, b FROM upstream_rewrite_rowid ORDER BY a;
SELECT count(*) = 0 AS no_rowids_changed
FROM upstream_rewrite_rowid r
JOIN upstream_rewrite_saved_rowids s USING (a)
WHERE ducklake.rowid() <> s.rowid;
INSERT INTO upstream_rewrite_rowid VALUES (11, 110), (12, 120);
DELETE FROM upstream_rewrite_rowid WHERE a = 3;
SELECT * FROM ducklake.rewrite_data_files('upstream_rewrite_rowid'::regclass);
SELECT ducklake.rowid() AS rowid, a, b FROM upstream_rewrite_rowid ORDER BY a;
DROP TABLE upstream_rewrite_saved_rowids;
DROP TABLE upstream_rewrite_rowid;
