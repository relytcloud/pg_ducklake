-- Test ducklake.variant type support.

-- ============================================================
-- 1. Create ducklake table with variant column
-- ============================================================
CREATE TABLE variant_test (id int, v ducklake.variant) USING ducklake;

-- Verify no inlined data table is created for variant columns
-- (variant disables data inlining)
SELECT count(*) AS inlined_tables
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table t ON idt.table_id = t.table_id
WHERE t.table_name = 'variant_test' AND t.end_snapshot IS NULL;

-- ============================================================
-- 2. INSERT and SELECT variant data
-- ============================================================
INSERT INTO variant_test VALUES (1, 'hello'), (2, '42'), (3, 'true');

-- Complex objects: JSON, nested structs, arrays, maps
INSERT INTO variant_test VALUES
  (4, '{"name": "alice", "age": 30}'),
  (5, '[1, 2, 3]'),
  (6, '{"nested": {"a": [1, 2], "b": {"c": true}}}'),
  (7, '{"tags": ["x", "y"], "counts": [10, 20]}'),
  (8, NULL);

SELECT * FROM variant_test ORDER BY id;

-- ============================================================
-- 3. Variant field extraction with -> and ->> operators
-- ============================================================

-- Extract by key (returns variant -- opaque binary in PG)
SELECT v -> 'name' FROM variant_test WHERE id = 4;

-- Extract as text via ->> (returns text)
SELECT v ->> 'name' FROM variant_test WHERE id = 4;

-- Function call syntax (equivalent to -> operator)
SELECT ducklake.variant_extract(v, 'name') FROM variant_test WHERE id = 4;

-- Missing key returns NULL
SELECT v -> 'nonexistent' FROM variant_test WHERE id = 4;
SELECT v ->> 'nonexistent' FROM variant_test WHERE id = 4;

-- ============================================================
-- 4. Variant column on non-ducklake table must fail (error cases)
-- ============================================================
CREATE TABLE variant_heap (v ducklake.variant);

-- ALTER TABLE ADD COLUMN must also fail
CREATE TABLE regular_heap (id int);
ALTER TABLE regular_heap ADD COLUMN v ducklake.variant;
DROP TABLE regular_heap;

-- ============================================================
-- 5. Metadata sync: variant column synced from DuckLake metadata
-- ============================================================
-- Get current metadata state
SELECT snapshot_id AS cur_snap, next_catalog_id AS cur_cat_id,
       next_file_id AS cur_file_id, schema_version AS cur_sv
FROM ducklake.ducklake_snapshot
ORDER BY snapshot_id DESC LIMIT 1 \gset

SELECT schema_id AS pub_sid
FROM ducklake.ducklake_schema
WHERE schema_name = 'public' AND end_snapshot IS NULL \gset

BEGIN;

INSERT INTO ducklake.ducklake_table
  (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id,
   table_name, path, path_is_relative)
VALUES
  (:cur_cat_id, gen_random_uuid(), :cur_snap + 1, NULL,
   :pub_sid, 'ext_variant_table', NULL, true);

INSERT INTO ducklake.ducklake_column
  (column_id, begin_snapshot, end_snapshot, table_id, column_order,
   column_name, column_type, initial_default, default_value,
   nulls_allowed, parent_column)
VALUES
  (:cur_cat_id + 1, :cur_snap + 1, NULL, :cur_cat_id, 0,
   'id', 'int32', NULL, NULL, true, NULL),
  (:cur_cat_id + 2, :cur_snap + 1, NULL, :cur_cat_id, 1,
   'data', 'variant', NULL, NULL, true, NULL);

INSERT INTO ducklake.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES
  (:cur_snap + 1, now(), :cur_sv, :cur_cat_id + 3, :cur_file_id);

COMMIT;

-- Verify synced table has variant column
SELECT attname, format_type(atttypid, atttypmod)
FROM pg_attribute
WHERE attrelid = 'ext_variant_table'::regclass AND attnum > 0
ORDER BY attnum;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE variant_test;

-- Re-query current state for cleanup
SELECT snapshot_id AS cleanup_snap, next_catalog_id AS cleanup_cat_id,
       next_file_id AS cleanup_file_id, schema_version AS cleanup_sv
FROM ducklake.ducklake_snapshot
ORDER BY snapshot_id DESC LIMIT 1 \gset

BEGIN;

UPDATE ducklake.ducklake_table
SET end_snapshot = :cleanup_snap + 1
WHERE table_name = 'ext_variant_table' AND end_snapshot IS NULL;

UPDATE ducklake.ducklake_column
SET end_snapshot = :cleanup_snap + 1
WHERE table_id = :cur_cat_id AND end_snapshot IS NULL;

INSERT INTO ducklake.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES
  (:cleanup_snap + 1, now(), :cleanup_sv, :cleanup_cat_id, :cleanup_file_id);

COMMIT;

SELECT relname FROM pg_class WHERE relname = 'ext_variant_table';
