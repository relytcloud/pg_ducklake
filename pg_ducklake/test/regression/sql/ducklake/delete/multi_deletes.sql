-- Upstream: test/sql/delete/multi_deletes.test
-- Skip: scheduled and physically removed delete files are not observable through PostgreSQL metadata.
-- Repeated deletes must consolidate correctly within and across transactions.

CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_multi_deletes USING ducklake AS
SELECT i AS id FROM generate_series(0, 9999) AS g(i);

BEGIN;
DELETE FROM upstream_multi_deletes WHERE id % 8 = 0;
SELECT count(*), sum(id) FROM upstream_multi_deletes;
DELETE FROM upstream_multi_deletes WHERE id % 4 = 0;
SELECT count(*), sum(id) FROM upstream_multi_deletes;
COMMIT;

SELECT count(*), sum(id) FROM upstream_multi_deletes;
DELETE FROM upstream_multi_deletes WHERE id % 2 = 0;
SELECT count(*), sum(id) FROM upstream_multi_deletes;

SELECT count(*) AS active_delete_files
FROM ducklake.ducklake_delete_file f
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_multi_deletes'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;

DROP TABLE upstream_multi_deletes;
