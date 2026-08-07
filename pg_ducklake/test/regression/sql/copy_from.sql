-- Test COPY FROM STDIN for ducklake tables with inlined data.
--
-- COPY FROM file (Parquet/CSV) is handled by pg_duckdb and tested
-- separately. This file covers the COPY FROM STDIN path that
-- bypasses DuckDB and inserts directly into the inlined data table.

-- =============================================================
-- COPY FROM STDIN: inlined ducklake table
-- =============================================================

-- Create a table and ensure inlined data table exists
CALL ducklake.set_option('data_inlining_row_limit', 100);
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

-- COPY's parser evaluates omitted defaults once while consuming each row.
CREATE TABLE copy_defaults (id int, label text DEFAULT 'copied') USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('copy_defaults'::regclass);
COPY copy_defaults (id) FROM STDIN;
1
2
\.
SELECT * FROM copy_defaults ORDER BY id;
DROP TABLE copy_defaults;

-- Unsupported COPY semantics must fail before COPY enters streaming mode.
COPY copy_stdin FROM STDIN WITH (FREEZE true);
SELECT count(*) FROM copy_stdin;
COPY copy_stdin FROM STDIN WITH (ON_ERROR ignore);
SELECT count(*) FROM copy_stdin;

DROP TABLE copy_stdin;

-- Dropped PostgreSQL attributes must not shift or disable live-column stats.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE copy_dropped (id int, dropped_col int, v int) USING ducklake;
INSERT INTO copy_dropped SELECT i, i, i FROM generate_series(1, 200) AS i;
ALTER TABLE copy_dropped DROP COLUMN dropped_col;
CALL ducklake.set_option('data_inlining_row_limit', 100);
SELECT count(*) FROM ducklake.ensure_inlined_data_table('copy_dropped'::regclass);
COPY copy_dropped (id, v) FROM STDIN WITH (FORMAT csv);
1000,2000
\.
SELECT c.column_name, s.min_value, s.max_value
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
JOIN ducklake.ducklake_column c USING (table_id, column_id)
WHERE t.table_name = 'copy_dropped' AND t.end_snapshot IS NULL
  AND c.end_snapshot IS NULL
ORDER BY c.column_order;
SELECT it.table_name AS dropped_inl
FROM ducklake.ducklake_inlined_data_tables it
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'copy_dropped' AND t.end_snapshot IS NULL
ORDER BY it.schema_version DESC LIMIT 1 \gset
SELECT id, v FROM ducklake.:dropped_inl WHERE id = 1000;
DROP TABLE copy_dropped;

-- COPY temporal output must be ISO regardless of the session DateStyle.
CREATE TABLE copy_temporal (d date, ts timestamp) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('copy_temporal'::regclass);
SET DateStyle = 'SQL, DMY';
COPY copy_temporal FROM STDIN WITH (FORMAT csv);
31/12/2030,31/12/2030 23:59:58
\.
SHOW DateStyle;
RESET DateStyle;
SELECT extract(year FROM d) AS year, extract(second FROM ts) AS second
FROM copy_temporal;
DROP TABLE copy_temporal;

-- Nested descendant statistics cannot be derived from PostgreSQL array Datums
-- and must be invalidated rather than left stale.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE copy_nested (id int, values int[]) USING ducklake;
INSERT INTO copy_nested SELECT i, ARRAY[i, i + 1] FROM generate_series(1, 200) AS i;
CALL ducklake.set_option('data_inlining_row_limit', 100);
SELECT count(*) FROM ducklake.ensure_inlined_data_table('copy_nested'::regclass);
COPY copy_nested FROM STDIN WITH (FORMAT csv);
1000,"{1000,2000}"
\.
SELECT c.parent_column IS NOT NULL AS descendant,
       s.min_value IS NULL AND s.max_value IS NULL AND
       s.contains_null IS NULL AND s.contains_nan IS NULL AND
       s.extra_stats IS NULL AS invalidated
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
JOIN ducklake.ducklake_column c USING (table_id, column_id)
WHERE t.table_name = 'copy_nested' AND t.end_snapshot IS NULL
  AND c.end_snapshot IS NULL
ORDER BY c.column_id;
SELECT it.table_name AS nested_inl
FROM ducklake.ducklake_inlined_data_tables it
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'copy_nested' AND t.end_snapshot IS NULL
ORDER BY it.schema_version DESC LIMIT 1 \gset
SELECT id, values FROM ducklake.:nested_inl WHERE id = 1000;
DROP TABLE copy_nested;

CALL ducklake.set_option('data_inlining_row_limit', 0);

-- Note: error case (COPY FROM STDIN without inlined data table) is not
-- tested here because a failed COPY FROM STDIN leaks its data as SQL,
-- preventing cleanup of the test table.
