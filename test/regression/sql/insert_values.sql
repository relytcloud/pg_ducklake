-- Test for INSERT ... VALUES direct insert optimization
-- Regression test for #176: direct insert must create ducklake_table_stats

SET ducklake.enable_direct_insert = true;
CALL ducklake.set_option('data_inlining_row_limit', 1000);

-- ============================================================
-- Test 1: Stats creation and row_id correctness (#176)
-- ============================================================
CREATE TABLE iv_stats (id int, val text) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_stats'::regclass);

-- First direct insert should create ducklake_table_stats row
INSERT INTO iv_stats VALUES (1, 'one');

SELECT record_count, next_row_id
FROM ducklake.ducklake_table_stats
JOIN ducklake.ducklake_table dt USING (table_id)
WHERE dt.table_name = 'iv_stats' AND dt.end_snapshot IS NULL;

-- Second insert should increment stats
INSERT INTO iv_stats VALUES (2, 'two'), (3, 'three');

SELECT record_count, next_row_id
FROM ducklake.ducklake_table_stats
JOIN ducklake.ducklake_table dt USING (table_id)
WHERE dt.table_name = 'iv_stats' AND dt.end_snapshot IS NULL;

-- No duplicate row_ids
SELECT id, val FROM iv_stats ORDER BY id;

-- UPDATE/DELETE work after direct insert
UPDATE iv_stats SET val = 'ONE' WHERE id = 1;
SELECT id, val FROM iv_stats ORDER BY id;

DELETE FROM iv_stats WHERE id = 2;
SELECT id, val FROM iv_stats ORDER BY id;

DROP TABLE iv_stats;

-- ============================================================
-- Test 2: EXPLAIN shows Pattern: VALUES
-- ============================================================
CREATE TABLE iv_explain (id int, val text) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_explain'::regclass);

EXPLAIN INSERT INTO iv_explain VALUES (1, 'hello');
EXPLAIN INSERT INTO iv_explain VALUES (1, 'a'), (2, 'b'), (3, 'c');

DROP TABLE iv_explain;

-- ============================================================
-- Test 3: GUC toggle (disable -> falls through to DuckDB)
-- ============================================================
CREATE TABLE iv_guc (id int, val text) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_guc'::regclass);

INSERT INTO iv_guc VALUES (1, 'direct');
SET ducklake.enable_direct_insert = false;
INSERT INTO iv_guc VALUES (2, 'duckdb');
SET ducklake.enable_direct_insert = true;

SELECT id, val FROM iv_guc ORDER BY id;
DROP TABLE iv_guc;

-- ============================================================
-- Test 4: Partial columns (omitted columns get NULL)
-- ============================================================
CREATE TABLE iv_partial (id int, name text, score double precision) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_partial'::regclass);

INSERT INTO iv_partial (id) VALUES (1);
INSERT INTO iv_partial (id, name) VALUES (2, 'alice');
INSERT INTO iv_partial (id, score) VALUES (3, 9.5);

SELECT id, name, score FROM iv_partial ORDER BY id;
DROP TABLE iv_partial;

-- ============================================================
-- Test 5: Explicit NULLs
-- ============================================================
CREATE TABLE iv_nulls (id int, val text) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_nulls'::regclass);

INSERT INTO iv_nulls VALUES (1, NULL), (NULL, 'hello'), (NULL, NULL);
SELECT id, val FROM iv_nulls ORDER BY id NULLS LAST;
DROP TABLE iv_nulls;

-- ============================================================
-- Test 6: Native integer types (smallint, int, bigint)
-- ============================================================
CREATE TABLE iv_integers (a smallint, b int, c bigint) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_integers'::regclass);

INSERT INTO iv_integers VALUES (1, 100, 1000000000000);
INSERT INTO iv_integers VALUES (-32768, -2147483648, -9223372036854775808);
INSERT INTO iv_integers VALUES (32767, 2147483647, 9223372036854775807);
SELECT * FROM iv_integers ORDER BY a;
DROP TABLE iv_integers;

-- ============================================================
-- Test 7: Floating point types (real, double precision)
-- ============================================================
CREATE TABLE iv_floats (a real, b double precision) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_floats'::regclass);

INSERT INTO iv_floats VALUES (1.5, 2.5);
INSERT INTO iv_floats VALUES (-3.14, 2.718281828459045);
SELECT * FROM iv_floats ORDER BY a;
DROP TABLE iv_floats;

-- ============================================================
-- Test 8: Boolean
-- ============================================================
CREATE TABLE iv_bool (id int, flag boolean) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_bool'::regclass);

INSERT INTO iv_bool VALUES (1, true), (2, false), (3, NULL);
SELECT * FROM iv_bool ORDER BY id;
DROP TABLE iv_bool;

-- ============================================================
-- Test 9: Text/VARCHAR (DuckDB VARCHAR -> BYTEA in inlined table)
-- ============================================================
CREATE TABLE iv_text (a text, b varchar) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_text'::regclass);

INSERT INTO iv_text VALUES ('hello', 'world');
INSERT INTO iv_text VALUES ('', '');
INSERT INTO iv_text VALUES (NULL, 'only b');
SELECT * FROM iv_text ORDER BY a NULLS LAST;
DROP TABLE iv_text;

-- ============================================================
-- Test 10: Date (DuckDB DATE -> VARCHAR in inlined table)
-- ============================================================
CREATE TABLE iv_date (d date) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_date'::regclass);

INSERT INTO iv_date VALUES ('2024-01-15'), ('1970-01-01'), ('2099-12-31');
SELECT d FROM iv_date ORDER BY d;
DROP TABLE iv_date;

-- ============================================================
-- Test 11: Timestamp (DuckDB TIMESTAMP -> VARCHAR in inlined table)
-- ============================================================
CREATE TABLE iv_ts (ts timestamp) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_ts'::regclass);

INSERT INTO iv_ts VALUES ('2024-01-15 10:30:00'), ('1970-01-01 00:00:00');
SELECT ts FROM iv_ts ORDER BY ts;
DROP TABLE iv_ts;

-- ============================================================
-- Test 12: Type coercion (int literal into bigint, etc.)
-- ============================================================
CREATE TABLE iv_coerce (big bigint, small smallint, txt text) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_coerce'::regclass);

-- Integer literal 42 coerced to bigint; 1 coerced to smallint
INSERT INTO iv_coerce VALUES (42, 1, 'coerced');
SELECT big, small, txt FROM iv_coerce;
DROP TABLE iv_coerce;

-- ============================================================
-- Test 13: Multi-row with mixed NULLs
-- ============================================================
CREATE TABLE iv_mixed (id int, val text, flag boolean) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_mixed'::regclass);

INSERT INTO iv_mixed VALUES
  (1, 'alpha', true),
  (2, NULL, false),
  (3, 'gamma', NULL),
  (4, NULL, NULL);
SELECT * FROM iv_mixed ORDER BY id;
DROP TABLE iv_mixed;

-- ============================================================
-- Test 14: All supported types in one table
-- ============================================================
CREATE TABLE iv_all_types (
  c_bool boolean,
  c_int2 smallint,
  c_int4 int,
  c_int8 bigint,
  c_float4 real,
  c_float8 double precision,
  c_text text,
  c_varchar varchar,
  c_date date,
  c_ts timestamp
) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_all_types'::regclass);

INSERT INTO iv_all_types VALUES (
  true, 1, 100, 1000000, 1.5, 2.718281828459045,
  'hello', 'world', '2024-06-15', '2024-06-15 12:00:00'
);
INSERT INTO iv_all_types VALUES (
  false, -1, -100, -1000000, -1.5, -2.718281828459045,
  'foo', 'bar', '1970-01-01', '1970-01-01 00:00:00'
);
INSERT INTO iv_all_types VALUES (
  NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL
);

SELECT * FROM iv_all_types ORDER BY c_int4 NULLS LAST;
DROP TABLE iv_all_types;

-- ============================================================
-- Test 15: DDL on unrelated table must not block direct insert
-- A DDL on table B bumps the global schema_version in
-- ducklake_snapshot but must not disable direct insert on
-- table A whose per-table schema has not changed.
--
-- Use a dedicated schema so leftover DuckDB catalog entries
-- don't leak into import_foreign_schema tests.
-- ============================================================
CREATE SCHEMA iv_schema;
SET search_path = iv_schema;

CREATE TABLE iv_target (id int, val text) USING ducklake;
CREATE TABLE iv_other (x int) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_target'::regclass);

-- Direct insert works before any DDL
EXPLAIN INSERT INTO iv_target VALUES (1, 'before');
INSERT INTO iv_target VALUES (1, 'before');

-- Touch iv_other so DuckDB registers it, then ALTER it
INSERT INTO iv_other VALUES (1);
ALTER TABLE iv_other ADD COLUMN y text;

-- Direct insert on iv_target must still use the fast path
EXPLAIN INSERT INTO iv_target VALUES (2, 'after');
INSERT INTO iv_target VALUES (2, 'after');

SELECT id, val FROM iv_target ORDER BY id;

DROP TABLE iv_other;
DROP TABLE iv_target;

-- ============================================================
-- Test 16: DDL on the SAME table must disable direct insert
-- ALTER TABLE on iv_self bumps its per-table schema_version,
-- so EXPLAIN must NOT show the DuckLakeDirectInsert plan.
-- ============================================================
CREATE TABLE iv_self (id int, val text) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_self'::regclass);

-- Direct insert works before ALTER
EXPLAIN INSERT INTO iv_self VALUES (1, 'before');
INSERT INTO iv_self VALUES (1, 'before');

-- ALTER the same table
ALTER TABLE iv_self ADD COLUMN extra int;

-- Direct insert must NOT be used (schema version mismatch)
EXPLAIN INSERT INTO iv_self (id, val, extra) VALUES (2, 'after', 42);

-- Data still works via the DuckDB path
INSERT INTO iv_self (id, val, extra) VALUES (2, 'after', 42);
SELECT id, val, extra FROM iv_self ORDER BY id;

DROP TABLE iv_self;

-- ============================================================
-- Test 17: Direct insert must preserve the global catalog view
-- ducklake_snapshot.schema_version is global: setting it to a
-- per-table value would roll back the catalog and hide tables
-- created after the direct-insert target.  Regression test for
-- #182 where a later SELECT on a sibling table failed with
-- "Table with name X does not exist" after a direct insert.
-- ============================================================
CREATE TABLE iv_first (id int) USING ducklake;
CREATE TABLE iv_second (id int) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('iv_first'::regclass);

-- Direct insert into iv_first -- must not rewind the global schema_version
INSERT INTO iv_first VALUES (1);

-- iv_second was created after iv_first; a rolled-back snapshot would hide it
SELECT count(*) FROM iv_second;

DROP TABLE iv_first;
DROP TABLE iv_second;

-- Cleanup dedicated schema
RESET search_path;
DROP SCHEMA iv_schema;
