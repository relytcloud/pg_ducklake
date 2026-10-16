-- Direct insert (VALUES) and COPY FROM STDIN write the inlined table's own
-- representation: typmods applied, nested types never stored as PG text.
CALL ducklake.set_option('data_inlining_row_limit', 1000);
SELECT ducklake.reset_direct_insert_stats();

-- Unconstrained numeric is inlined as numeric(18,3).
CREATE TABLE itc_num (n numeric) USING ducklake;
INSERT INTO itc_num VALUES (5), (1.2), (100), (0.5), (1.23456);
COPY itc_num FROM STDIN;
7
2.5
1.23456
\.
SELECT n FROM itc_num ORDER BY n;

-- Arrays fall back to the DuckDB writer.
CREATE TABLE itc_arr (id int, ints int[], texts text[]) USING ducklake;
INSERT INTO itc_arr VALUES (1, ARRAY[1, 2], ARRAY['a', 'b']), (2, ARRAY[3], NULL);
SELECT * FROM itc_arr ORDER BY id;

-- COPY has no fallback writer, so it rejects non-NULL arrays.
COPY itc_arr FROM STDIN;
3	\N	\N
4	{4,5}	{c}
\.
COPY itc_arr FROM STDIN;
5	\N	\N
\.
SELECT * FROM itc_arr ORDER BY id;

-- Temporal values are written in ISO format whatever DateStyle is.
CREATE TABLE itc_dt (d date, ts timestamp) USING ducklake;
SET DateStyle = 'SQL, DMY';
INSERT INTO itc_dt VALUES ('02/01/2024', '02/01/2024 10:30:00');
COPY itc_dt FROM STDIN;
03/01/2024	03/01/2024 11:00:00
\.
SHOW DateStyle;
SET DateStyle = 'ISO, YMD';
SELECT d, ts FROM itc_dt ORDER BY d;
RESET DateStyle;

-- Only the numeric and date VALUES inserts took the direct-insert path.
SELECT pattern, reason, count FROM ducklake.direct_insert_stats() WHERE count > 0 ORDER BY pattern, reason;

DROP TABLE itc_num, itc_arr, itc_dt;
CALL ducklake.set_option('data_inlining_row_limit', 0);
