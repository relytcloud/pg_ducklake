-- Upstream: test/sql/deletion_inlining/test_deletion_from_file_and_inserted_inlining.test
-- Skip: Per-snapshot inlined insertion/deletion row metadata is not exposed by the PostgreSQL API.
CALL ducklake.set_option('data_inlining_row_limit', 5);
CREATE TABLE upstream_delete_mixed USING ducklake AS SELECT g AS a FROM generate_series(0, 9) g;
INSERT INTO upstream_delete_mixed VALUES (11), (12), (13);
DELETE FROM upstream_delete_mixed WHERE a IN (5, 12);
SELECT count(*), sum(a) FROM upstream_delete_mixed;
SELECT count(*) AS active_delete_files
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_mixed' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_mixed'::regclass);
SELECT count(*) AS active_delete_files
FROM ducklake.ducklake_delete_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_mixed' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT * FROM upstream_delete_mixed ORDER BY a;
DROP TABLE upstream_delete_mixed;
CALL ducklake.set_option('data_inlining_row_limit', 0);
