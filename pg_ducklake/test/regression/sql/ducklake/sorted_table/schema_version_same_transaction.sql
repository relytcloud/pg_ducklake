-- Upstream: test/sql/sorted_table/schema_version_same_transaction.test
-- Multiple sort changes are version-neutral; a same-transaction ADD advances schema_version once.
CREATE TABLE upstream_sort_same_tx (a integer, b text) USING ducklake;
SELECT max(sv.schema_version) AS schema_before
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_same_tx' AND t.end_snapshot IS NULL \gset
BEGIN;
CALL ducklake.set_sort('upstream_sort_same_tx'::regclass, 'a ASC');
CALL ducklake.set_sort('upstream_sort_same_tx'::regclass, 'b DESC');
CALL ducklake.set_sort('upstream_sort_same_tx'::regclass, 'a DESC', 'b ASC');
COMMIT;
SELECT max(sv.schema_version) = :schema_before AS sort_only_version_neutral
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_same_tx' AND t.end_snapshot IS NULL;
SELECT * FROM ducklake.get_sort('upstream_sort_same_tx'::regclass);
BEGIN;
CALL ducklake.set_sort('upstream_sort_same_tx'::regclass, 'a ASC');
ALTER TABLE upstream_sort_same_tx ADD COLUMN c bigint;
CALL ducklake.set_sort('upstream_sort_same_tx'::regclass, 'a ASC', 'c DESC');
COMMIT;
SELECT max(sv.schema_version) = :schema_before + 2 AS add_advanced_once
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_same_tx' AND t.end_snapshot IS NULL;
SELECT * FROM ducklake.get_sort('upstream_sort_same_tx'::regclass);
DROP TABLE upstream_sort_same_tx;
