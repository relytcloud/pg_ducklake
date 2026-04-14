-- Test for INSERT ... VALUES direct insert optimization

SET ducklake.enable_direct_insert = true;
CALL ducklake.set_option('data_inlining_row_limit', 1000);

-- Single-row INSERT
CREATE TABLE insert_values_basic (
    id INT,
    name TEXT,
    amount DOUBLE PRECISION
) USING ducklake;

-- Ensure inlined data table exists before direct insert
SELECT * FROM ducklake.ensure_inlined_data_table('insert_values_basic'::regclass);

INSERT INTO insert_values_basic VALUES (1, 'alice', 100.5);
SELECT * FROM insert_values_basic ORDER BY id;

-- Multi-row INSERT
INSERT INTO insert_values_basic VALUES (2, 'bob', 200.0), (3, 'charlie', 300.75);
SELECT * FROM insert_values_basic ORDER BY id;

-- NULL values
INSERT INTO insert_values_basic VALUES (4, NULL, NULL);
SELECT * FROM insert_values_basic WHERE id = 4;

-- EXPLAIN should show DuckLakeDirectInsert with VALUES pattern
EXPLAIN (COSTS OFF) INSERT INTO insert_values_basic VALUES (5, 'test', 1.0);

-- Type coercion (integer literal to double precision)
INSERT INTO insert_values_basic VALUES (6, 'typed', 42);
SELECT * FROM insert_values_basic WHERE id = 6;

-- Larger batch
INSERT INTO insert_values_basic VALUES
    (10, 'r1', 1.0), (11, 'r2', 2.0), (12, 'r3', 3.0),
    (13, 'r4', 4.0), (14, 'r5', 5.0);
SELECT COUNT(*) FROM insert_values_basic WHERE id >= 10;

DROP TABLE insert_values_basic;

-- Partial column insert: only some columns specified
CREATE TABLE insert_values_partial (
    id INT,
    name TEXT,
    score DOUBLE PRECISION
) USING ducklake;

SELECT * FROM ducklake.ensure_inlined_data_table('insert_values_partial'::regclass);

INSERT INTO insert_values_partial (id, score) VALUES (1, 99.5);
SELECT * FROM insert_values_partial ORDER BY id;

INSERT INTO insert_values_partial (id, name) VALUES (2, 'partial');
SELECT * FROM insert_values_partial ORDER BY id;

-- Full columns still work
INSERT INTO insert_values_partial VALUES (3, 'full', 88.0);
SELECT * FROM insert_values_partial ORDER BY id;

DROP TABLE insert_values_partial;

-- Disable optimization: should fallback to DuckDB path
SET ducklake.enable_direct_insert = false;

CREATE TABLE insert_values_fallback (
    id INT,
    name TEXT
) USING ducklake;

INSERT INTO insert_values_fallback VALUES (1, 'fallback');
SELECT * FROM insert_values_fallback ORDER BY id;

SET ducklake.enable_direct_insert = true;

SELECT * FROM ducklake.ensure_inlined_data_table('insert_values_fallback'::regclass);

INSERT INTO insert_values_fallback VALUES (2, 'direct');
SELECT * FROM insert_values_fallback ORDER BY id;

DROP TABLE insert_values_fallback;
