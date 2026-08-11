-- Upstream: test/sql/data_inlining/inlined_data_table_leak.test
-- Skip: Physical internal inlined-table cleanup is not exposed by the PostgreSQL API.
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_leak (i integer) USING ducklake;
INSERT INTO upstream_inline_leak VALUES (1), (2), (3);
ALTER TABLE upstream_inline_leak ADD COLUMN j integer;
INSERT INTO upstream_inline_leak VALUES (4, 40);
ALTER TABLE upstream_inline_leak ADD COLUMN k integer;
INSERT INTO upstream_inline_leak VALUES (5, 50, 500);
SELECT count(*) AS inline_versions
FROM ducklake.ducklake_inlined_data_tables d JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_leak' AND t.end_snapshot IS NULL;
DROP TABLE upstream_inline_leak;
CALL ducklake.set_option('expire_older_than', '0 seconds');
SELECT count(*) >= 0 AS expired FROM ducklake.expire_snapshots();
SELECT count(*) >= 0 AS cleaned FROM ducklake.cleanup_old_files();
SELECT count(*) FROM ducklake.ducklake_inlined_data_tables d
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_leak';
CALL ducklake.set_option('expire_older_than', '7 days');
CALL ducklake.set_option('data_inlining_row_limit', 0);
