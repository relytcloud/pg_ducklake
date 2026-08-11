-- Upstream: test/sql/data_inlining/basic_data_inlining.test
-- Small batches stay inline; a batch above the limit becomes a data file.
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_basic (i integer, j integer) USING ducklake;
BEGIN;
INSERT INTO upstream_inline_basic VALUES (1, 2), (NULL, 3);
SELECT i, j FROM upstream_inline_basic ORDER BY j;
SELECT count(*) FROM upstream_inline_basic;
SELECT ducklake.rowid() >= 1000000000000000000 AS transaction_local_rowid,
       ducklake.snapshot_id() IS NULL AS transaction_local_snapshot,
       ducklake.filename() = '__ducklake_inlined_transaction_local_data' AS transaction_local_filename,
       ducklake.file_row_number(), ducklake.file_index(), i, j
FROM upstream_inline_basic ORDER BY ducklake.rowid();
COMMIT;
SELECT ducklake.rowid(), ducklake.snapshot_id() IS NOT NULL AS committed, i, j
FROM upstream_inline_basic ORDER BY ducklake.rowid();
SELECT count(DISTINCT ducklake.filename()) AS inline_sources,
       min(ducklake.file_row_number()) AS first_file_row,
       max(ducklake.file_row_number()) AS last_file_row,
       count(DISTINCT ducklake.file_index()) AS file_indexes
FROM upstream_inline_basic;
SELECT i, j FROM upstream_inline_basic WHERE i IS NULL;
SELECT i, j FROM upstream_inline_basic WHERE i = 1;
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_basic' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
INSERT INTO upstream_inline_basic SELECT g, 100 + g FROM generate_series(0, 10) g;
SELECT count(*), sum(i), sum(j) FROM upstream_inline_basic;
SELECT count(*) AS active_files
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_basic' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
DROP TABLE upstream_inline_basic;
CALL ducklake.set_option('data_inlining_row_limit', 0);
