-- Upstream: test/sql/delete/truncate_table.test
-- Skip: absence of transaction-local physical parquet files is not observable from PostgreSQL.
-- Deleting every row must handle both transaction-local and committed files.

BEGIN;
CREATE TABLE upstream_truncate_local USING ducklake AS
SELECT i AS id FROM generate_series(0, 9999) AS g(i);
SELECT count(*) FROM upstream_truncate_local;
DELETE FROM upstream_truncate_local;
SELECT count(*) FROM upstream_truncate_local;
COMMIT;
SELECT count(*) FROM upstream_truncate_local;
DROP TABLE upstream_truncate_local;

CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_truncate_committed USING ducklake AS
SELECT i AS id FROM generate_series(0, 9999) AS g(i);
BEGIN;
DELETE FROM upstream_truncate_committed;
SELECT count(*) FROM upstream_truncate_committed;
COMMIT;
SELECT count(*) FROM upstream_truncate_committed;
DELETE FROM upstream_truncate_committed;

SELECT count(*) AS active_delete_files
FROM ducklake.ducklake_delete_file f
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_truncate_committed'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;

DROP TABLE upstream_truncate_committed;
