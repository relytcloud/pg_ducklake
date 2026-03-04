-- Test metadata sync: DuckDB→PG catalog synchronization.
-- Simulates an external DuckDB client by directly inserting into metadata tables.
-- Each simulated operation is wrapped in a transaction, as a real DuckDB client would.

-- ============================================================
-- 0. Non-regression: PG→DuckDB DDL path still works
--    (must run BEFORE direct metadata inserts corrupt DuckDB state)
-- ============================================================
CREATE TABLE regular_dl (x int) USING ducklake;
SELECT relname FROM pg_class
WHERE relname = 'regular_dl'
  AND relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake');
DROP TABLE regular_dl;

-- Get current metadata state (schema was created by the table above)
SELECT snapshot_id AS cur_snap, next_catalog_id AS cur_cat_id,
       next_file_id AS cur_file_id, schema_version AS cur_sv
FROM ducklake.ducklake_snapshot
ORDER BY snapshot_id DESC LIMIT 1 \gset

SELECT schema_id AS pub_sid
FROM ducklake.ducklake_schema
WHERE schema_name = 'public' AND end_snapshot IS NULL \gset

-- ============================================================
-- 1. Create: simulate DuckDB client creating 'ext_table'
--    DuckLake uses its own type names (int32, varchar, etc.)
-- ============================================================
BEGIN;

INSERT INTO ducklake.ducklake_table
  (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id,
   table_name, path, path_is_relative)
VALUES
  (:cur_cat_id, gen_random_uuid(), :cur_snap + 1, NULL,
   :pub_sid, 'ext_table', NULL, true);

INSERT INTO ducklake.ducklake_column
  (column_id, begin_snapshot, end_snapshot, table_id, column_order,
   column_name, column_type, initial_default, default_value,
   nulls_allowed, parent_column)
VALUES
  (:cur_cat_id + 1, :cur_snap + 1, NULL, :cur_cat_id, 0,
   'id', 'int32', NULL, NULL, true, NULL),
  (:cur_cat_id + 2, :cur_snap + 1, NULL, :cur_cat_id, 1,
   'name', 'varchar', NULL, NULL, false, NULL);

-- Snapshot insert fires the trigger → should create pg_class entry
INSERT INTO ducklake.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES
  (:cur_snap + 1, now(), :cur_sv, :cur_cat_id + 3, :cur_file_id);

COMMIT;

-- Verify table appeared in pg_class with ducklake AM
SELECT relname, am.amname
FROM pg_class c
JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'ext_table';

-- Verify column definitions and NOT NULL constraint
SELECT attname, format_type(atttypid, atttypmod), attnotnull
FROM pg_attribute
WHERE attrelid = 'ext_table'::regclass AND attnum > 0
ORDER BY attnum;

-- ============================================================
-- 2. Idempotent: re-inserting a snapshot for an existing table is a no-op
-- ============================================================
BEGIN;

INSERT INTO ducklake.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES
  (:cur_snap + 2, now(), :cur_sv, :cur_cat_id + 3, :cur_file_id);

COMMIT;

-- Still exactly one ext_table
SELECT count(*) FROM pg_class WHERE relname = 'ext_table';

-- ============================================================
-- 3. Type mapping: DuckLake-specific types → PG types
-- ============================================================
BEGIN;

INSERT INTO ducklake.ducklake_table
  (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id,
   table_name, path, path_is_relative)
VALUES
  (:cur_cat_id + 3, gen_random_uuid(), :cur_snap + 3, NULL,
   :pub_sid, 'typed_table', NULL, true);

INSERT INTO ducklake.ducklake_column
  (column_id, begin_snapshot, end_snapshot, table_id, column_order,
   column_name, column_type, initial_default, default_value,
   nulls_allowed, parent_column)
VALUES
  (:cur_cat_id + 4, :cur_snap + 3, NULL, :cur_cat_id + 3, 0,
   'f', 'float32', NULL, NULL, true, NULL),
  (:cur_cat_id + 5, :cur_snap + 3, NULL, :cur_cat_id + 3, 1,
   'd', 'float64', NULL, NULL, true, NULL),
  (:cur_cat_id + 6, :cur_snap + 3, NULL, :cur_cat_id + 3, 2,
   'b', 'blob', NULL, NULL, true, NULL),
  (:cur_cat_id + 7, :cur_snap + 3, NULL, :cur_cat_id + 3, 3,
   'h', 'int128', NULL, NULL, true, NULL);

INSERT INTO ducklake.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES
  (:cur_snap + 3, now(), :cur_sv, :cur_cat_id + 8, :cur_file_id);

COMMIT;

-- Verify type mapping: float32→real, float64→double precision, blob→bytea, int128→numeric
SELECT attname, format_type(atttypid, atttypmod)
FROM pg_attribute
WHERE attrelid = 'typed_table'::regclass AND attnum > 0
ORDER BY attnum;

-- ============================================================
-- 4. Drop: simulate DuckDB client dropping 'ext_table'
-- ============================================================
BEGIN;

UPDATE ducklake.ducklake_table
SET end_snapshot = :cur_snap + 4
WHERE table_name = 'ext_table' AND end_snapshot IS NULL;

UPDATE ducklake.ducklake_column
SET end_snapshot = :cur_snap + 4
WHERE table_id = :cur_cat_id AND end_snapshot IS NULL;

INSERT INTO ducklake.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES
  (:cur_snap + 4, now(), :cur_sv, :cur_cat_id + 8, :cur_file_id);

COMMIT;

-- Verify ext_table is gone from pg_class
SELECT relname FROM pg_class WHERE relname = 'ext_table';

-- ============================================================
-- Cleanup: drop typed_table via simulated metadata
-- ============================================================
BEGIN;

UPDATE ducklake.ducklake_table
SET end_snapshot = :cur_snap + 5
WHERE table_name = 'typed_table' AND end_snapshot IS NULL;

UPDATE ducklake.ducklake_column
SET end_snapshot = :cur_snap + 5
WHERE table_id = :cur_cat_id + 3 AND end_snapshot IS NULL;

INSERT INTO ducklake.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES
  (:cur_snap + 5, now(), :cur_sv, :cur_cat_id + 8, :cur_file_id);

COMMIT;

SELECT relname FROM pg_class WHERE relname = 'typed_table';

-- ============================================================
-- 5. Unknown type: falls back to text
-- ============================================================
BEGIN;

INSERT INTO ducklake.ducklake_table
  (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id,
   table_name, path, path_is_relative)
VALUES
  (:cur_cat_id + 8, gen_random_uuid(), :cur_snap + 6, NULL,
   :pub_sid, 'unknown_type_table', NULL, true);

INSERT INTO ducklake.ducklake_column
  (column_id, begin_snapshot, end_snapshot, table_id, column_order,
   column_name, column_type, initial_default, default_value,
   nulls_allowed, parent_column)
VALUES
  (:cur_cat_id + 9, :cur_snap + 6, NULL, :cur_cat_id + 8, 0,
   'normal_col', 'int32', NULL, NULL, true, NULL),
  (:cur_cat_id + 10, :cur_snap + 6, NULL, :cur_cat_id + 8, 1,
   'weird_col', 'some_future_type', NULL, NULL, true, NULL);

INSERT INTO ducklake.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES
  (:cur_snap + 6, now(), :cur_sv, :cur_cat_id + 11, :cur_file_id);

COMMIT;

-- Verify: normal_col is integer, weird_col falls back to text
SELECT attname, format_type(atttypid, atttypmod)
FROM pg_attribute
WHERE attrelid = 'unknown_type_table'::regclass AND attnum > 0
ORDER BY attnum;

-- ============================================================
-- 6. Table with no columns
-- ============================================================
BEGIN;

INSERT INTO ducklake.ducklake_table
  (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id,
   table_name, path, path_is_relative)
VALUES
  (:cur_cat_id + 11, gen_random_uuid(), :cur_snap + 7, NULL,
   :pub_sid, 'empty_table', NULL, true);

-- No column inserts

INSERT INTO ducklake.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES
  (:cur_snap + 7, now(), :cur_sv, :cur_cat_id + 12, :cur_file_id);

COMMIT;

-- Verify: table exists with zero user columns
SELECT relname FROM pg_class
WHERE relname = 'empty_table'
  AND relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake');

SELECT count(*) FROM pg_attribute
WHERE attrelid = 'empty_table'::regclass AND attnum > 0;

-- ============================================================
-- 7. Drop of already-dropped table: no-op
-- ============================================================
BEGIN;

-- ext_table was already dropped in test 4 — dropping again should be harmless
UPDATE ducklake.ducklake_table
SET end_snapshot = :cur_snap + 8
WHERE table_name = 'ext_table' AND end_snapshot = :cur_snap + 4;

INSERT INTO ducklake.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES
  (:cur_snap + 8, now(), :cur_sv, :cur_cat_id + 12, :cur_file_id);

COMMIT;

-- No error, table remains absent
SELECT relname FROM pg_class WHERE relname = 'ext_table';

-- ============================================================
-- Cleanup: drop remaining tables via simulated metadata
-- ============================================================
BEGIN;

UPDATE ducklake.ducklake_table
SET end_snapshot = :cur_snap + 9
WHERE table_name IN ('unknown_type_table', 'empty_table')
  AND end_snapshot IS NULL;

UPDATE ducklake.ducklake_column
SET end_snapshot = :cur_snap + 9
WHERE table_id IN (:cur_cat_id + 8, :cur_cat_id + 11)
  AND end_snapshot IS NULL;

INSERT INTO ducklake.ducklake_snapshot
  (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
VALUES
  (:cur_snap + 9, now(), :cur_sv, :cur_cat_id + 12, :cur_file_id);

COMMIT;

SELECT relname FROM pg_class
WHERE relname IN ('unknown_type_table', 'empty_table');
