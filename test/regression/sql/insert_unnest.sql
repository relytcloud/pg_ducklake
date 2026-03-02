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

-- Test 3b: Verify EXPLAIN shows custom scan
EXPLAIN EXECUTE insert_plan(ARRAY[10, 20], ARRAY['x', 'y']);

-- Test 3c: Execute the previous plan
EXECUTE insert_plan(ARRAY[10, 20], ARRAY['x', 'y']);

SELECT * FROM insert_unnest_bypass ORDER BY id;

-- Test 3d: Disable optimization (should fallback to DuckDB)
SET ducklake.enable_direct_insert = false;
EXECUTE insert_plan(ARRAY[100, 200], ARRAY['p', 'q']);
SELECT COUNT(*) FROM insert_unnest_bypass;

-- Test 3e: Re-enable and test large batch
SET ducklake.enable_direct_insert = true;
PREPARE insert_large (int[]) AS
INSERT INTO insert_unnest_bypass SELECT UNNEST($1), 'batch';

-- Execute with 100 rows
EXECUTE insert_large((SELECT array_agg(i) FROM generate_series(1000, 1099) i));
SELECT COUNT(*) FROM insert_unnest_bypass WHERE val = 'batch';

-- Clean up
DROP TABLE insert_unnest_bypass;
