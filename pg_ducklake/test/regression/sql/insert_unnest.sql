-- Test for INSERT ... UNNEST

CREATE TABLE insert_unnest (
    id INT
) USING ducklake;

INSERT INTO insert_unnest
SELECT * FROM UNNEST(ARRAY[1, 2, 3]);

SELECT * FROM insert_unnest ORDER BY id;

DROP TABLE insert_unnest;

CREATE TABLE insert_unnest (
    id INT,
    val TEXT
) USING ducklake;

-- Test 1: Multi-column UNNEST (zipping)
INSERT INTO insert_unnest
SELECT UNNEST(ARRAY[1, 2, 3]), UNNEST(ARRAY['a', 'b', 'c']);

SELECT * FROM insert_unnest ORDER BY id;

-- Test 2: Array Literal Handling
INSERT INTO insert_unnest
SELECT UNNEST(ARRAY[4, 5]), UNNEST(ARRAY['d', 'e']::text[]);

SELECT * FROM insert_unnest WHERE id > 3 ORDER BY id;

-- Clean up
DROP TABLE insert_unnest;

-- Test 3: Parameterized UNNEST (direct insert optimization)
-- Enable optimization and create table with inlining
SET ducklake.enable_direct_insert = true;

CREATE TABLE insert_unnest_bypass (
    id INT,
    val TEXT
) USING ducklake;

-- Enable data inlining for this table
CALL ducklake.set_option('data_inlining_row_limit', 1000);

-- First insert via normal path to create inlined data table
INSERT INTO insert_unnest_bypass VALUES (0, 'init');
SELECT * FROM insert_unnest_bypass ORDER BY id;

-- Check if inlined data table exists
SELECT COUNT(*) > 0 AS has_inlined_data_table FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = 'ducklake' AND c.relname LIKE 'ducklake_inlined_data%'
AND c.relname NOT LIKE '%_tables';

-- Test 3a: Basic parameterized UNNEST (should use direct insert)
PREPARE insert_plan (int[], text[]) AS
INSERT INTO insert_unnest_bypass SELECT UNNEST($1), UNNEST($2);

EXECUTE insert_plan(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']);

SELECT * FROM insert_unnest_bypass ORDER BY id;

-- Reordered target columns must preserve the INSERT mapping.
CREATE TABLE insert_unnest_reordered (a int, b int) USING ducklake;
INSERT INTO insert_unnest_reordered VALUES (0, 0);
PREPARE insert_reordered (int[], int[]) AS
INSERT INTO insert_unnest_reordered (b, a) SELECT UNNEST($1), UNNEST($2);
SELECT ducklake.reset_direct_insert_stats();
EXECUTE insert_reordered(ARRAY[10, 20], ARRAY[1, 2]);
SELECT pattern, reason, count FROM ducklake.direct_insert_stats() WHERE count > 0;
SELECT * FROM insert_unnest_reordered ORDER BY a;
DEALLOCATE insert_reordered;
DROP TABLE insert_unnest_reordered;

-- Temporal UNNEST serialization must not depend on or alter DateStyle.
CREATE TABLE insert_unnest_temporal (d date, ts timestamp) USING ducklake;
INSERT INTO insert_unnest_temporal VALUES (DATE '2020-01-01', TIMESTAMP '2020-01-01');
PREPARE insert_temporal (date[], timestamp[]) AS
INSERT INTO insert_unnest_temporal SELECT UNNEST($1), UNNEST($2);
SET DateStyle = 'SQL, DMY';
EXECUTE insert_temporal(ARRAY[DATE '2030-12-31'], ARRAY[TIMESTAMP '2030-12-31 23:59:58']);
SHOW DateStyle;
RESET DateStyle;
SELECT extract(year FROM d) AS year, extract(second FROM ts) AS second
FROM insert_unnest_temporal ORDER BY d;
DEALLOCATE insert_temporal;
DROP TABLE insert_unnest_temporal;

-- Only a bare SELECT list of built-in pg_catalog.unnest(anyarray) calls is
-- eligible. Source clauses must not be ignored by the native writer.
CREATE TABLE insert_unnest_matcher (id int) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('insert_unnest_matcher'::regclass);
SELECT ducklake.reset_direct_insert_stats();

PREPARE unnest_where (int[]) AS
INSERT INTO insert_unnest_matcher SELECT unnest($1) WHERE false;
EXECUTE unnest_where(ARRAY[1, 2]);

PREPARE unnest_from (int[]) AS
INSERT INTO insert_unnest_matcher SELECT unnest($1) FROM generate_series(1, 2);
EXECUTE unnest_from(ARRAY[1, 2]);

PREPARE unnest_limit (int[]) AS
INSERT INTO insert_unnest_matcher SELECT unnest($1) LIMIT 1;
EXECUTE unnest_limit(ARRAY[1, 2]);

PREPARE unnest_offset (int[]) AS
INSERT INTO insert_unnest_matcher SELECT unnest($1) OFFSET 1;
EXECUTE unnest_offset(ARRAY[1, 2]);

PREPARE unnest_distinct (int[]) AS
INSERT INTO insert_unnest_matcher SELECT DISTINCT unnest($1);
EXECUTE unnest_distinct(ARRAY[1, 1]);

PREPARE unnest_group (int[]) AS
INSERT INTO insert_unnest_matcher SELECT unnest($1) GROUP BY ();
EXECUTE unnest_group(ARRAY[1, 2]);

PREPARE unnest_having (int[]) AS
INSERT INTO insert_unnest_matcher SELECT unnest($1) HAVING false;
EXECUTE unnest_having(ARRAY[1, 2]);

PREPARE unnest_window (int[]) AS
INSERT INTO insert_unnest_matcher SELECT unnest($1) ORDER BY row_number() OVER ();
EXECUTE unnest_window(ARRAY[1, 2]);

PREPARE unnest_setop (int[]) AS
INSERT INTO insert_unnest_matcher
SELECT unnest($1) UNION ALL SELECT unnest($1);
EXECUTE unnest_setop(ARRAY[1, 2]);

PREPARE unnest_cte (int[]) AS
WITH source_cte AS (SELECT 1)
INSERT INTO insert_unnest_matcher SELECT unnest($1);
EXECUTE unnest_cte(ARRAY[1, 2]);

CREATE SCHEMA unnest_matcher_user;
CREATE TABLE unnest_matcher_user.calls (call_count int);
CREATE FUNCTION unnest_matcher_user.unnest(int[]) RETURNS SETOF int
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
  INSERT INTO unnest_matcher_user.calls VALUES (1);
  RETURN QUERY SELECT value FROM pg_catalog.unnest($1) AS value;
END
$$;
PREPARE unnest_user (int[]) AS
INSERT INTO insert_unnest_matcher SELECT unnest_matcher_user.unnest($1);
EXECUTE unnest_user(ARRAY[1, 2]);

SELECT count(*) AS inserted_rows FROM insert_unnest_matcher;
SELECT count(*) AS user_function_calls FROM unnest_matcher_user.calls;
SELECT pattern, reason, count
FROM ducklake.direct_insert_stats() WHERE count > 0
ORDER BY pattern, reason;

DEALLOCATE unnest_where;
DEALLOCATE unnest_from;
DEALLOCATE unnest_limit;
DEALLOCATE unnest_offset;
DEALLOCATE unnest_distinct;
DEALLOCATE unnest_group;
DEALLOCATE unnest_having;
DEALLOCATE unnest_window;
DEALLOCATE unnest_setop;
DEALLOCATE unnest_cte;
DEALLOCATE unnest_user;
DROP SCHEMA unnest_matcher_user CASCADE;
DROP TABLE insert_unnest_matcher;

-- Test 3b: Verify EXPLAIN shows custom scan
EXPLAIN EXECUTE insert_plan(ARRAY[10, 20], ARRAY['x', 'y']);

-- Test 3c: Execute the previous plan
EXECUTE insert_plan(ARRAY[10, 20], ARRAY['x', 'y']);

SELECT * FROM insert_unnest_bypass ORDER BY id;

-- Test 3d: Parameterized UNNEST has no DuckDB fallback. Disabled-direct and
-- explicit-transaction execution fail before DuckDB with the same stable error.
SET ducklake.enable_direct_insert = false;
EXECUTE insert_plan(ARRAY[100, 200], ARRAY['p', 'q']);
SET ducklake.enable_direct_insert = true;
BEGIN;
EXECUTE insert_plan(ARRAY[100, 200], ARRAY['p', 'q']);
ROLLBACK;
SELECT COUNT(*) FROM insert_unnest_bypass;

-- Test 3e: Re-enable and test large batch
PREPARE insert_large (int[]) AS
INSERT INTO insert_unnest_bypass SELECT UNNEST($1), 'batch';

-- Execute with 100 rows
EXECUTE insert_large((SELECT array_agg(i) FROM generate_series(1000, 1099) i));
SELECT COUNT(*) FROM insert_unnest_bypass WHERE val = 'batch';

-- Parameter values and SRF cardinality are bound at execution so generic plans
-- preserve target-list lockstep semantics. Empty/NULL-only input does not
-- publish a snapshot, and shorter inputs are NULL-padded to the longest input.
CREATE TABLE insert_unnest_generic (id int, val text) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('insert_unnest_generic'::regclass);

SET plan_cache_mode = force_generic_plan;
PREPARE insert_force_generic (int[], text[]) AS
INSERT INTO insert_unnest_generic SELECT UNNEST($1), UNNEST($2);
SELECT max(snapshot_id) AS snapshot_before_empty
FROM ducklake.ducklake_snapshot \gset
EXECUTE insert_force_generic(ARRAY[]::int[], ARRAY[]::text[]);
EXECUTE insert_force_generic(NULL::int[], NULL::text[]);
SELECT max(snapshot_id) = :snapshot_before_empty AS no_empty_snapshot
FROM ducklake.ducklake_snapshot;
EXECUTE insert_force_generic(ARRAY[1, 2, 3], ARRAY['a']);
EXECUTE insert_force_generic(ARRAY[]::int[], ARRAY['x', 'y']);
EXECUTE insert_force_generic(NULL::int[], ARRAY['z']);
SELECT count(*) AS force_rows,
       count(*) FILTER (WHERE id IS NULL) AS padded_ids,
       count(*) FILTER (WHERE val IS NULL) AS padded_vals
FROM insert_unnest_generic;
SELECT generic_plans, custom_plans
FROM pg_prepared_statements WHERE name = 'insert_force_generic';
DEALLOCATE insert_force_generic;

SET plan_cache_mode = auto;
PREPARE insert_auto_generic (int[], text[]) AS
INSERT INTO insert_unnest_generic SELECT UNNEST($1), UNNEST($2);
EXECUTE insert_auto_generic(ARRAY[10], ARRAY['ten']);
EXECUTE insert_auto_generic(ARRAY[20, 21], ARRAY['twenty']);
EXECUTE insert_auto_generic(ARRAY[]::int[], ARRAY['thirty']);
EXECUTE insert_auto_generic(NULL::int[], ARRAY['forty', 'forty-one']);
EXECUTE insert_auto_generic(ARRAY[50], NULL::text[]);
EXECUTE insert_auto_generic(ARRAY[60, 61, 62], ARRAY['sixty', 'sixty-one']);
SELECT max(snapshot_id) AS snapshot_before_auto_empty
FROM ducklake.ducklake_snapshot \gset
EXECUTE insert_auto_generic(ARRAY[]::int[], ARRAY[]::text[]);
SELECT max(snapshot_id) = :snapshot_before_auto_empty AS no_auto_empty_snapshot
FROM ducklake.ducklake_snapshot;
SELECT generic_plans > 0 AS selected_generic, custom_plans
FROM pg_prepared_statements WHERE name = 'insert_auto_generic';
SELECT count(*) AS all_rows,
       count(*) FILTER (WHERE id IS NULL) AS padded_ids,
       count(*) FILTER (WHERE val IS NULL) AS padded_vals
FROM insert_unnest_generic;
DEALLOCATE insert_auto_generic;
RESET plan_cache_mode;
DROP TABLE insert_unnest_generic;

-- Clean up
DROP TABLE insert_unnest_bypass;
