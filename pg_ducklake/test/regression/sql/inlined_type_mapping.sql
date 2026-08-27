-- Inlined-data column types and typmods across all three writers: VALUES,
-- UNNEST and COPY FROM STDIN.  All three share the same conversion and reader
-- code, so a defect common to them would pass an inline-vs-inline comparison;
-- the non-inlined parquet table below is the independent reference.

CALL ducklake.set_option('data_inlining_row_limit', 1000);

-- ------------------------------------------------------------------
-- numeric: VALUES, UNNEST and COPY must all agree
-- ------------------------------------------------------------------
CREATE TABLE itm_values (n numeric) USING ducklake;
CREATE TABLE itm_unnest (n numeric) USING ducklake;
CREATE TABLE itm_copy (n numeric) USING ducklake;

SELECT ducklake.reset_direct_insert_stats();

INSERT INTO itm_values VALUES (5), (1.2), (100), (0.5), (1.234), (1.23456);

PREPARE itm_u (numeric[]) AS INSERT INTO itm_unnest SELECT UNNEST($1);
EXECUTE itm_u(ARRAY[5, 1.2, 100, 0.5, 1.234, 1.23456]::numeric[]);
DEALLOCATE itm_u;

COPY itm_copy FROM STDIN;
5
1.2
100
0.5
1.234
1.23456
\.

-- COPY FROM STDIN does not bump these counters.
SELECT pattern, reason, count FROM ducklake.direct_insert_stats()
WHERE count > 0 ORDER BY pattern, reason;

SELECT n FROM itm_values ORDER BY n;
SELECT n FROM itm_unnest ORDER BY n;
SELECT n FROM itm_copy ORDER BY n;

-- Both counts must be 0.
SELECT count(*) AS values_vs_unnest_diffs
FROM (SELECT n FROM itm_values EXCEPT ALL SELECT n FROM itm_unnest) d;
SELECT count(*) AS copy_vs_unnest_diffs
FROM (SELECT n FROM itm_copy EXCEPT ALL SELECT n FROM itm_unnest) d;

-- ------------------------------------------------------------------
-- Out of range for decimal(18,3): must raise, not store silently
-- ------------------------------------------------------------------
INSERT INTO itm_values VALUES (12345678901234567890123.456789);

COPY itm_copy FROM STDIN;
12345678901234567890123.456789
\.

-- Neither failed statement stored a row.
SELECT count(*) AS values_rows FROM itm_values;
SELECT count(*) AS copy_rows FROM itm_copy;

-- ------------------------------------------------------------------
-- A declared numeric(p,s) rounds to its own scale
-- ------------------------------------------------------------------
CREATE TABLE itm_declared (n numeric(10,2)) USING ducklake;
INSERT INTO itm_declared VALUES (1.239), (5);
COPY itm_declared FROM STDIN;
2.351
\.
SELECT n FROM itm_declared ORDER BY n;

-- ------------------------------------------------------------------
-- LIST is not storable by the fast path: it declines and the normal
-- inline path handles the statement
-- ------------------------------------------------------------------
CREATE TABLE itm_list (a int, b int[]) USING ducklake;

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO itm_list VALUES (1, ARRAY[1,2]), (2, ARRAY[3,4]);
SELECT pattern, reason, count FROM ducklake.direct_insert_stats()
WHERE count > 0 ORDER BY pattern, reason;

SELECT a, b FROM itm_list ORDER BY a;

-- COPY has no fallback path, so it must refuse rather than store a value the
-- reader cannot decode.  The failure is at setup, so psql reports the
-- terminator as an unknown command.
COPY itm_list FROM STDIN;
\.
SELECT a, b FROM itm_list ORDER BY a;

-- ------------------------------------------------------------------
-- Non-inlined reference: all three writers above share the inlined
-- reader, so a defect common to them cannot show up in the
-- comparisons above.  This table stores parquet instead.
-- ------------------------------------------------------------------
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE itm_parquet (n numeric) USING ducklake;
INSERT INTO itm_parquet VALUES (5), (1.2), (100), (0.5), (1.234), (1.23456);

-- Must be 0, or the comparisons below are comparing inlined against inlined.
SELECT count(*) AS reference_inlined_tables
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table t ON t.table_id = idt.table_id
WHERE t.table_name = 'itm_parquet' AND t.end_snapshot IS NULL;

SELECT n FROM itm_parquet ORDER BY n;

-- All three must agree with it; every count must be 0.
SELECT count(*) AS values_vs_parquet_diffs
FROM (SELECT n FROM itm_values EXCEPT ALL SELECT n FROM itm_parquet) d;
SELECT count(*) AS unnest_vs_parquet_diffs
FROM (SELECT n FROM itm_unnest EXCEPT ALL SELECT n FROM itm_parquet) d;
SELECT count(*) AS copy_vs_parquet_diffs
FROM (SELECT n FROM itm_copy EXCEPT ALL SELECT n FROM itm_parquet) d;

-- ------------------------------------------------------------------
-- Temporal columns reach the inlined table through PG output functions,
-- which follow DateStyle, while DuckDB reads only ISO.
-- ------------------------------------------------------------------
-- The parquet section above turned inlining off; these cases need the inlined
-- writers, which are the ones that convert through output functions.
CALL ducklake.set_option('data_inlining_row_limit', 1000);
SET DateStyle = 'Postgres, MDY';

CREATE TABLE itm_dt (a int, d date, ts timestamp, n numeric) USING ducklake;
INSERT INTO itm_dt VALUES (0, '2024-01-01', '2024-01-01 00:00:00', 1);
INSERT INTO itm_dt VALUES (1, '2024-01-02', '2024-01-02 03:04:05', 1.2345);
COPY itm_dt FROM STDIN;
2	2024-01-02	2024-01-02 03:04:05	1.2345
\.

-- Readable at all, and the two writers agree, only if both stored ISO.
SELECT a, d, ts, n FROM itm_dt ORDER BY a;

-- UNNEST converts through output functions too.
CREATE TABLE itm_dt_unnest (d date) USING ducklake;
PREPARE itm_du (date[]) AS INSERT INTO itm_dt_unnest SELECT UNNEST($1);
EXECUTE itm_du(ARRAY['2024-01-02', '2024-01-03']::date[]);
DEALLOCATE itm_du;
SELECT d FROM itm_dt_unnest ORDER BY d;

-- A failed insert must not leave the session in ISO.
INSERT INTO itm_dt VALUES (3, '2024-01-02', '2024-01-02 03:04:05', 12345678901234567890123.456789);
SHOW DateStyle;
SELECT '2024-01-02'::date AS datestyle_intact;
RESET DateStyle;

-- ------------------------------------------------------------------
-- timestamptz reaches the inlined table only through COPY: the fast path
-- declines it.  The two writers store one instant as different text -- COPY
-- local time with an offset, DuckDB UTC -- so compare in UTC.
-- ------------------------------------------------------------------
SET TimeZone = 'America/Los_Angeles';

CREATE TABLE itm_tz_values (a int, t timestamptz) USING ducklake;
CREATE TABLE itm_tz_copy (a int, t timestamptz) USING ducklake;

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO itm_tz_values VALUES (1, '2024-01-02 03:04:05-08'), (2, '2024-06-02 03:04:05-07');

SELECT pattern, reason, count FROM ducklake.direct_insert_stats()
WHERE count > 0 ORDER BY pattern, reason;

COPY itm_tz_copy FROM STDIN;
1	2024-01-02 03:04:05-08
2	2024-06-02 03:04:05-07
\.

-- Half-hour offset under a non-ISO DateStyle: the reader accepts only ISO,
-- and the offset has to survive.
SET TimeZone = 'Asia/Kolkata';
SET DateStyle = 'Postgres, MDY';
COPY itm_tz_copy FROM STDIN;
3	2024-01-02 03:04:05+05:30
\.
RESET DateStyle;

-- Non-inlined reference, for the same reason as the numeric one above.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE itm_tz_parquet (a int, t timestamptz) USING ducklake;
SET TimeZone = 'America/Los_Angeles';
INSERT INTO itm_tz_parquet VALUES (1, '2024-01-02 03:04:05-08'), (2, '2024-06-02 03:04:05-07');

-- Must be 0, or the comparisons below are comparing inlined against inlined.
SELECT count(*) AS tz_reference_inlined_tables
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table t ON t.table_id = idt.table_id
WHERE t.table_name = 'itm_tz_parquet' AND t.end_snapshot IS NULL;

SET TimeZone = 'UTC';
SELECT a, t FROM itm_tz_values ORDER BY a;
SELECT a, t FROM itm_tz_copy ORDER BY a;
SELECT a, t FROM itm_tz_parquet ORDER BY a;

-- Both counts must be 0.
SELECT count(*) AS tz_values_vs_parquet_diffs
FROM (SELECT a, t FROM itm_tz_values EXCEPT ALL SELECT a, t FROM itm_tz_parquet) d;
SELECT count(*) AS tz_copy_vs_parquet_diffs
FROM (SELECT a, t FROM itm_tz_copy WHERE a < 3 EXCEPT ALL SELECT a, t FROM itm_tz_parquet) d;
RESET TimeZone;

-- ------------------------------------------------------------------
-- char(n): bpchar is a varchar source type, so the inline writers copy its
-- varlena verbatim -- PostgreSQL's blank padding included -- while the
-- standard DuckDB write path strips it (bpchartruelen).  The same char(5)
-- value therefore reads back padded out of an inlined table and trimmed out
-- of parquet.  That divergence predates this allowlist and is tracked
-- separately: the cases below record what happens today rather than assert
-- what should.
-- ------------------------------------------------------------------
CALL ducklake.set_option('data_inlining_row_limit', 1000);

CREATE TABLE itm_char_values (a int, c char(5)) USING ducklake;
CREATE TABLE itm_char_copy (a int, c char(5)) USING ducklake;

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO itm_char_values VALUES (1, 'ab'), (2, 'abcde'), (3, NULL);

COPY itm_char_copy FROM STDIN;
1	ab
2	abcde
3	\N
\.

SELECT pattern, reason, count FROM ducklake.direct_insert_stats()
WHERE count > 0 ORDER BY pattern, reason;

-- Both inline writers keep the padding: length 5 for every stored value.
SELECT a, '[' || c || ']' AS braced, length(c) AS len FROM itm_char_values ORDER BY a;
SELECT a, '[' || c || ']' AS braced, length(c) AS len FROM itm_char_copy ORDER BY a;

-- The padding is in the heap, not added by the reader.
SELECT idt.table_name AS char_inl
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table t ON t.table_id = idt.table_id
WHERE t.table_name = 'itm_char_values' AND t.end_snapshot IS NULL
ORDER BY idt.schema_version DESC LIMIT 1 \gset
SELECT a, '[' || convert_from(c, 'UTF8') || ']' AS stored
FROM ducklake.:char_inl ORDER BY a;

-- UNNEST cannot reach a char(n) column at all: the length coercion the target
-- typmod adds is not a shape the fast path matches, and the fallback has no
-- parameterized UNNEST.  Pre-existing, and not specific to bpchar -- any
-- declared typmod does it, numeric(12,3) included.
CREATE TABLE itm_char_unnest (c char(5)) USING ducklake;
PREPARE itm_cu (char(5)[]) AS INSERT INTO itm_char_unnest SELECT UNNEST($1);
EXECUTE itm_cu(ARRAY['ab', 'abcde']::char(5)[]);
DEALLOCATE itm_cu;
SELECT count(*) AS char_unnest_rows FROM itm_char_unnest;

-- Non-inlined reference: the same values through the standard write path.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE itm_char_parquet (a int, c char(5)) USING ducklake;
INSERT INTO itm_char_parquet VALUES (1, 'ab'), (2, 'abcde'), (3, NULL);

-- Must be 0, or this is comparing inlined against inlined.
SELECT count(*) AS char_reference_inlined_tables
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table t ON t.table_id = idt.table_id
WHERE t.table_name = 'itm_char_parquet' AND t.end_snapshot IS NULL;

-- Trimmed, where both inlined tables above are padded.
SELECT a, '[' || c || ']' AS braced, length(c) AS len FROM itm_char_parquet ORDER BY a;

-- bpchar with no length modifier has no typmod and nothing to pad, so it
-- rides all three writers and agrees with parquet.
CALL ducklake.set_option('data_inlining_row_limit', 1000);
CREATE TABLE itm_bpchar (c bpchar) USING ducklake;

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO itm_bpchar VALUES ('ab'), ('abcde');

PREPARE itm_bu (bpchar[]) AS INSERT INTO itm_bpchar SELECT UNNEST($1);
EXECUTE itm_bu(ARRAY['cd', 'cdefg']::bpchar[]);
DEALLOCATE itm_bu;

COPY itm_bpchar FROM STDIN;
ef
efghi
\.

SELECT pattern, reason, count FROM ducklake.direct_insert_stats()
WHERE count > 0 ORDER BY pattern, reason;

SELECT '[' || c || ']' AS braced, length(c) AS len FROM itm_bpchar ORDER BY c;

-- ------------------------------------------------------------------
-- json: the allowlist keys it apart from varchar -- both parse to
-- LogicalTypeId::VARCHAR -- so it needs its own case, and a json column is
-- the only thing that reaches the entry.
-- ------------------------------------------------------------------
CREATE TABLE itm_json_values (a int, j json) USING ducklake;
CREATE TABLE itm_json_copy (a int, j json) USING ducklake;

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO itm_json_values VALUES (1, '{"a": 1}'), (2, '[1, 2, 3]'), (3, NULL);

COPY itm_json_copy FROM STDIN;
1	{"a": 1}
2	[1, 2, 3]
3	\N
\.

SELECT pattern, reason, count FROM ducklake.direct_insert_stats()
WHERE count > 0 ORDER BY pattern, reason;

SELECT a, j FROM itm_json_values ORDER BY a;
SELECT a, j FROM itm_json_copy ORDER BY a;

-- Stored as json, not as text that happens to look like it.
SELECT a, j->>'a' AS a_field FROM itm_json_values WHERE a = 1;

-- Inlined as bytea, like varchar.
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema = 'ducklake' AND table_name = (
  SELECT idt.table_name
  FROM ducklake.ducklake_inlined_data_tables idt
  JOIN ducklake.ducklake_table t ON t.table_id = idt.table_id
  WHERE t.table_name = 'itm_json_values' AND t.end_snapshot IS NULL
  ORDER BY idt.schema_version DESC LIMIT 1)
AND column_name NOT IN ('row_id', 'begin_snapshot', 'end_snapshot')
ORDER BY ordinal_position;

-- Non-inlined reference, for the same reason as the numeric one above.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE itm_json_parquet (a int, j json) USING ducklake;
INSERT INTO itm_json_parquet VALUES (1, '{"a": 1}'), (2, '[1, 2, 3]'), (3, NULL);

-- Must be 0, or this is comparing inlined against inlined.
SELECT count(*) AS json_reference_inlined_tables
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table t ON t.table_id = idt.table_id
WHERE t.table_name = 'itm_json_parquet' AND t.end_snapshot IS NULL;

SELECT a, j FROM itm_json_parquet ORDER BY a;

-- Both counts must be 0.  json has no equality operator in PostgreSQL, so
-- compare the text the reader produced.
SELECT count(*) AS json_values_vs_parquet_diffs
FROM (SELECT a, j::text FROM itm_json_values
      EXCEPT ALL SELECT a, j::text FROM itm_json_parquet) d;
SELECT count(*) AS json_copy_vs_parquet_diffs
FROM (SELECT a, j::text FROM itm_json_copy
      EXCEPT ALL SELECT a, j::text FROM itm_json_parquet) d;

-- ------------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------------
DROP TABLE itm_json_values;
DROP TABLE itm_json_copy;
DROP TABLE itm_json_parquet;
DROP TABLE itm_char_values;
DROP TABLE itm_char_copy;
DROP TABLE itm_char_unnest;
DROP TABLE itm_char_parquet;
DROP TABLE itm_bpchar;
DROP TABLE itm_tz_values;
DROP TABLE itm_tz_copy;
DROP TABLE itm_tz_parquet;
DROP TABLE itm_dt;
DROP TABLE itm_dt_unnest;
DROP TABLE itm_parquet;
DROP TABLE itm_values;
DROP TABLE itm_unnest;
DROP TABLE itm_copy;
DROP TABLE itm_declared;
DROP TABLE itm_list;
CALL ducklake.set_option('data_inlining_row_limit', 0);
SELECT ducklake.reset_direct_insert_stats();
