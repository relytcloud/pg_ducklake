-- Upstream: test/sql/sorted_table/schema_version_mixed_operations.test
-- Sort changes are version-neutral while ADD, RENAME, and DROP each advance schema_version.
CREATE TABLE upstream_sort_mixed_schema (a integer, b text) USING ducklake;
SELECT max(sv.schema_version) AS schema_before
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_mixed_schema' AND t.end_snapshot IS NULL \gset
CALL ducklake.set_sort('upstream_sort_mixed_schema'::regclass, 'a ASC');
SELECT max(sv.schema_version) = :schema_before AS sort_is_version_neutral
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_mixed_schema' AND t.end_snapshot IS NULL;
ALTER TABLE upstream_sort_mixed_schema ADD COLUMN c bigint;
SELECT max(sv.schema_version) AS schema_after_add
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_mixed_schema' AND t.end_snapshot IS NULL \gset
SELECT :schema_after_add = :schema_before + 2 AS add_advanced_once;
CALL ducklake.set_sort('upstream_sort_mixed_schema'::regclass, 'a DESC', 'c ASC');
SELECT max(sv.schema_version) = :schema_after_add AS second_sort_is_version_neutral
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_mixed_schema' AND t.end_snapshot IS NULL;
ALTER TABLE upstream_sort_mixed_schema RENAME COLUMN b TO b_renamed;
SELECT max(sv.schema_version) AS schema_after_rename
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_mixed_schema' AND t.end_snapshot IS NULL \gset
SELECT :schema_after_rename = :schema_after_add + 2 AS rename_advanced_once;
CALL ducklake.set_sort('upstream_sort_mixed_schema'::regclass, 'b_renamed ASC');
SELECT max(sv.schema_version) = :schema_after_rename AS third_sort_is_version_neutral
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_mixed_schema' AND t.end_snapshot IS NULL;
ALTER TABLE upstream_sort_mixed_schema DROP COLUMN c;
SELECT max(sv.schema_version) = :schema_after_rename + 2 AS drop_advanced_once
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_mixed_schema' AND t.end_snapshot IS NULL;
SELECT * FROM ducklake.get_sort('upstream_sort_mixed_schema'::regclass);
SELECT attname FROM pg_attribute WHERE attrelid = 'upstream_sort_mixed_schema'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum;
DROP TABLE upstream_sort_mixed_schema;
