-- Upstream: test/sql/sorted_table/set_sorted_by_rollback_mixed.test
-- Sort metadata and ordinary ALTER operations must roll back together.

CREATE TABLE upstream_sort_rollback (a integer, b text) USING ducklake;
SELECT max(snapshot_id) AS snapshot_before FROM ducklake.ducklake_snapshot \gset
SELECT max(sv.schema_version) AS schema_before
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_rollback' AND t.end_snapshot IS NULL \gset

BEGIN;
CALL ducklake.set_sort('upstream_sort_rollback'::regclass, 'a ASC NULLS LAST');
ALTER TABLE upstream_sort_rollback ADD COLUMN c bigint;
SELECT attname FROM pg_attribute
WHERE attrelid = 'upstream_sort_rollback'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum;
ROLLBACK;
SELECT max(snapshot_id) = :snapshot_before AS rollback_created_no_snapshot FROM ducklake.ducklake_snapshot;
SELECT max(sv.schema_version) = :schema_before AS rollback_preserved_schema_version
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_rollback' AND t.end_snapshot IS NULL;
SELECT attname FROM pg_attribute
WHERE attrelid = 'upstream_sort_rollback'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum;
SELECT count(*) AS active_sort_keys FROM ducklake.get_sort('upstream_sort_rollback'::regclass);

BEGIN;
ALTER TABLE upstream_sort_rollback ADD COLUMN c bigint;
CALL ducklake.set_sort('upstream_sort_rollback'::regclass, 'a ASC NULLS LAST', 'c DESC NULLS LAST');
ALTER TABLE upstream_sort_rollback ADD COLUMN d text;
ROLLBACK;
SELECT max(snapshot_id) = :snapshot_before AS second_rollback_created_no_snapshot FROM ducklake.ducklake_snapshot;
SELECT max(sv.schema_version) = :schema_before AS second_rollback_preserved_schema_version
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_rollback' AND t.end_snapshot IS NULL;
SELECT attname FROM pg_attribute
WHERE attrelid = 'upstream_sort_rollback'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum;
SELECT count(*) AS active_sort_keys FROM ducklake.get_sort('upstream_sort_rollback'::regclass);

ALTER TABLE upstream_sort_rollback ADD COLUMN c bigint;
CALL ducklake.set_sort('upstream_sort_rollback'::regclass, 'a ASC NULLS LAST', 'c DESC NULLS LAST');
SELECT attname FROM pg_attribute
WHERE attrelid = 'upstream_sort_rollback'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum;
SELECT * FROM ducklake.get_sort('upstream_sort_rollback'::regclass);
SELECT max(sv.schema_version) = :schema_before + 2 AS committed_add_advanced_once
FROM ducklake.ducklake_schema_versions sv JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_sort_rollback' AND t.end_snapshot IS NULL;

DROP TABLE upstream_sort_rollback;
