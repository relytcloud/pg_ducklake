-- Test COPY FROM STDIN for ducklake tables with inlined data.
--
-- COPY FROM file (Parquet/CSV) is handled by pg_duckdb and tested
-- separately. This file covers the COPY FROM STDIN path that
-- bypasses DuckDB and inserts directly into the inlined data table.

-- =============================================================
-- COPY FROM STDIN: inlined ducklake table
-- =============================================================

-- Create a table and ensure inlined data table exists
CREATE TABLE copy_stdin (id int, name text, val double precision) USING ducklake;

SELECT count(*) FROM ducklake.ensure_inlined_data_table('copy_stdin'::regclass);

-- Test 1: Basic COPY FROM STDIN (tab-delimited)
COPY copy_stdin FROM STDIN;
1	alice	1.5
2	bob	2.7
3	charlie	3.14
\.

SELECT * FROM copy_stdin ORDER BY id;

-- Test 2: COPY FROM STDIN with CSV format
COPY copy_stdin FROM STDIN WITH (FORMAT csv);
4,dave,4.0
5,eve,5.5
\.

SELECT count(*) FROM copy_stdin;

-- Test 3: COPY FROM STDIN with specific columns
COPY copy_stdin (id, name) FROM STDIN WITH (FORMAT csv);
6,frank
\.

SELECT id, name, val FROM copy_stdin WHERE id = 6;

-- Test 4: COPY FROM STDIN with NULL values
COPY copy_stdin FROM STDIN;
7	\N	7.7
\.

SELECT id, name, val FROM copy_stdin WHERE id = 7;

-- Test 5: Verify all data is queryable via DuckDB
SELECT count(*) FROM copy_stdin;

DROP TABLE copy_stdin;

-- Note: error case (COPY FROM STDIN without inlined data table) is not
-- tested here because a failed COPY FROM STDIN leaks its data as SQL,
-- preventing cleanup of the test table.
