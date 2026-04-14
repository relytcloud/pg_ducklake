-- Test COPY FROM and COPY TO for ducklake tables

-- Disable data inlining so we get actual files
CALL ducklake.set_option('data_inlining_row_limit', 0);

-- =============================================================
-- COPY TO: export ducklake data to Parquet
-- =============================================================

CREATE TABLE copy_src (id int, val text) USING ducklake;
INSERT INTO copy_src VALUES (1, 'one'), (2, 'two'), (3, 'three');

COPY copy_src TO '/tmp/pg_ducklake_testdata/copy_test.parquet' (FORMAT parquet);

-- =============================================================
-- COPY FROM: import Parquet into ducklake
-- =============================================================

CREATE TABLE copy_parquet (id int, val text) USING ducklake;

COPY copy_parquet FROM '/tmp/pg_ducklake_testdata/copy_test.parquet';

SELECT * FROM copy_parquet ORDER BY id;

DROP TABLE copy_parquet;

-- =============================================================
-- COPY FROM: import CSV (titanic dataset)
-- =============================================================

CREATE TABLE copy_csv (
    "PassengerId" int,
    "Survived" int,
    "Pclass" int,
    "Name" text,
    "Sex" text,
    "Age" double precision,
    "SibSp" int,
    "Parch" int,
    "Ticket" text,
    "Fare" double precision,
    "Cabin" text,
    "Embarked" text
) USING ducklake;

COPY copy_csv FROM '/tmp/pg_ducklake_testdata/titanic.csv' (FORMAT csv, HEADER true);

SELECT count(*) FROM copy_csv;

SELECT "PassengerId", "Survived", "Pclass", "Name"
FROM copy_csv
WHERE "PassengerId" = 1;

DROP TABLE copy_csv;

-- =============================================================
-- COPY FROM query export: Parquet from SELECT
-- =============================================================

COPY (SELECT generate_series AS n FROM generate_series(1, 5)) TO '/tmp/pg_ducklake_testdata/copy_query.parquet' (FORMAT parquet);

CREATE TABLE copy_query (n int) USING ducklake;
COPY copy_query FROM '/tmp/pg_ducklake_testdata/copy_query.parquet';

SELECT * FROM copy_query ORDER BY n;

DROP TABLE copy_query;

-- Cleanup
DROP TABLE copy_src;

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

-- =============================================================
-- Error case: COPY FROM STDIN without inlined data table
-- =============================================================
CREATE TABLE copy_no_inline (id int) USING ducklake;
-- Should fail: no inlined data table
COPY copy_no_inline FROM STDIN;
1
\.
DROP TABLE copy_no_inline;

CALL ducklake.set_option('data_inlining_row_limit', 0);
