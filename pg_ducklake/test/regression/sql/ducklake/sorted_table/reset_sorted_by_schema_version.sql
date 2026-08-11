-- Upstream: test/sql/sorted_table/reset_sorted_by_schema_version.test
-- Resetting sorting clears active sort metadata without incrementing schema_version.
CREATE TABLE upstream_reset_sort_schema (a integer, b text) USING ducklake;
SELECT max(sv.schema_version) AS schema_before
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_reset_sort_schema' AND t.end_snapshot IS NULL \gset
CALL ducklake.set_sort('upstream_reset_sort_schema'::regclass, 'a ASC');
SELECT max(sv.schema_version) = :schema_before AS set_sort_version_neutral
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_reset_sort_schema' AND t.end_snapshot IS NULL;
SELECT * FROM ducklake.get_sort('upstream_reset_sort_schema'::regclass);
CALL ducklake.reset_sort('upstream_reset_sort_schema'::regclass);
SELECT max(sv.schema_version) = :schema_before AS reset_sort_version_neutral
FROM ducklake.ducklake_schema_versions sv
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_reset_sort_schema' AND t.end_snapshot IS NULL;
SELECT count(*) AS active_sort_keys FROM ducklake.get_sort('upstream_reset_sort_schema'::regclass);
SELECT attname FROM pg_attribute WHERE attrelid = 'upstream_reset_sort_schema'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum;
DROP TABLE upstream_reset_sort_schema;
