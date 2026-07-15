-- Regression for GitHub issue #223:
-- ducklake.flush_inlined_data() on a table whose partition uses a non-identity
-- transform (month(), year(), day(), hour(), bucket()) failed with
--   IO Error: SPI execution failed: function month(timestamp with time zone) does not exist
--
-- The flush's per-file "which inlined rows were already deleted" query embeds
-- the DuckDB-dialect partition expression (month(col), murmur3_32(col)) and
-- pg_ducklake ships it to PostgreSQL via SPI. The fix provides those functions
-- as UDFs in the ducklake schema (resolved via forced search_path) with
-- DuckDB-identical semantics, so the filter agrees with the partition values
-- the DuckDB writer baked into the data files.
--
-- Caveat: the timestamptz transforms depend on the session TimeZone in both
-- engines; DuckDB's TimeZone is synced from PostgreSQL once, when the embedded
-- instance is created (a later SET timezone only affects the PG side).

CALL ducklake.set_option('data_inlining_row_limit', 100);

-- ------------------------------------------------------------------
-- 1. month() transform on TIMESTAMPTZ (the exact issue case),
--    including a NULL partition value (filter uses IS NULL)
-- ------------------------------------------------------------------
CREATE TABLE flush_month (id int, ts timestamptz, val int) USING ducklake;
CALL ducklake.set_partition('flush_month'::regclass, 'month(ts)');

-- Small batch -> inlined (not written to Parquet yet)
INSERT INTO flush_month VALUES
    (1, '2024-01-15 00:00:00+00', 10),
    (2, '2024-01-20 00:00:00+00', 20),
    (3, '2024-06-10 00:00:00+00', 30),
    (4, '2024-06-25 00:00:00+00', 40),
    (5, NULL, 50),
    (6, NULL, 60);

-- Delete + update inlined rows before flush: this leaves inlined versions with
-- end_snapshot set, which is exactly what the fixed query enumerates.
DELETE FROM flush_month WHERE id = 2;
UPDATE flush_month SET val = 99 WHERE id = 3;
DELETE FROM flush_month WHERE id = 5;   -- row in the NULL partition

-- Pre-fix: ERROR (function month(timestamp with time zone) does not exist)
SELECT * FROM ducklake.flush_inlined_data('flush_month'::regclass);

-- Deleted rows 2 and 5 gone, updated row 3 reflects new value, live rows intact.
SELECT id, val FROM flush_month ORDER BY id;

DROP TABLE flush_month;

-- ------------------------------------------------------------------
-- 2. bucket(N, col) on integer -- PostgreSQL has no murmur3_32() at all
-- ------------------------------------------------------------------
CREATE TABLE flush_bucket (id int, val int) USING ducklake;
CALL ducklake.set_partition('flush_bucket'::regclass, 'bucket(4, id)');

INSERT INTO flush_bucket VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
DELETE FROM flush_bucket WHERE id = 4;

SELECT * FROM ducklake.flush_inlined_data('flush_bucket'::regclass);
SELECT id, val FROM flush_bucket ORDER BY id;

DROP TABLE flush_bucket;

-- ------------------------------------------------------------------
-- 3. bucket(N, col) on text -- a hash-encoding mismatch here would make the
--    delete filter match nothing and silently resurrect the deleted row
-- ------------------------------------------------------------------
CREATE TABLE flush_btext (k text, val int) USING ducklake;
CALL ducklake.set_partition('flush_btext'::regclass, 'bucket(3, k)');

INSERT INTO flush_btext VALUES
    ('apple', 1), ('banana', 2), ('cherry', 3), ('', 4), (chr(233) || 't' || chr(233), 5);
DELETE FROM flush_btext WHERE k = 'banana';
DELETE FROM flush_btext WHERE k = chr(233) || 't' || chr(233);

SELECT * FROM ducklake.flush_inlined_data('flush_btext'::regclass);
SELECT k, val FROM flush_btext ORDER BY val;

DROP TABLE flush_btext;

-- ------------------------------------------------------------------
-- 4. identity partition on text: the inlined table stores text as BYTEA, so
--    the filter must recover the string (convert_from), not its hex form
-- ------------------------------------------------------------------
CREATE TABLE flush_idtext (k text, val int) USING ducklake;
CALL ducklake.set_partition('flush_idtext'::regclass, 'k');

INSERT INTO flush_idtext VALUES ('red', 1), ('blue', 2), ('red', 3);
DELETE FROM flush_idtext WHERE val = 3;

SELECT * FROM ducklake.flush_inlined_data('flush_idtext'::regclass);
SELECT k, val FROM flush_idtext ORDER BY val;

DROP TABLE flush_idtext;

-- ------------------------------------------------------------------
-- 5. bucket on numeric. DECIMAL(18,3) hashes its unscaled bigint; without an
--    exact numeric overload PG would silently pick the double one. The wide
--    DECIMAL(30,10) hashes its fixed-scale string form instead.
-- ------------------------------------------------------------------
CREATE TABLE flush_num (d numeric(18,3), val int) USING ducklake;
CALL ducklake.set_partition('flush_num'::regclass, 'bucket(4, d)');
INSERT INTO flush_num VALUES (12.5, 1), (0.001, 2), (-7.25, 3);
DELETE FROM flush_num WHERE val = 2;
SELECT * FROM ducklake.flush_inlined_data('flush_num'::regclass);
SELECT d, val FROM flush_num ORDER BY val;
DROP TABLE flush_num;

CREATE TABLE flush_widenum (d numeric(30,10), val int) USING ducklake;
CALL ducklake.set_partition('flush_widenum'::regclass, 'bucket(4, d)');
INSERT INTO flush_widenum VALUES (12.5, 1), (98765432109876543210.5, 2), (-0.0000000001, 3);
DELETE FROM flush_widenum WHERE val = 2;
SELECT * FROM ducklake.flush_inlined_data('flush_widenum'::regclass);
SELECT d, val FROM flush_widenum ORDER BY val;
DROP TABLE flush_widenum;

-- ------------------------------------------------------------------
-- 6. bucket on uuid (hashes the lowercase hyphenated string form)
-- ------------------------------------------------------------------
CREATE TABLE flush_uuid (u uuid, val int) USING ducklake;
CALL ducklake.set_partition('flush_uuid'::regclass, 'bucket(4, u)');
INSERT INTO flush_uuid VALUES
    ('f79c3e09-677c-4bbd-a479-3f349cb785e7', 1),
    ('00000000-0000-0000-0000-000000000000', 2),
    ('ffffffff-ffff-ffff-ffff-ffffffffffff', 3);
DELETE FROM flush_uuid WHERE val = 2;
SELECT * FROM ducklake.flush_inlined_data('flush_uuid'::regclass);
SELECT u, val FROM flush_uuid ORDER BY val;
DROP TABLE flush_uuid;

-- ------------------------------------------------------------------
-- 7. bucket on bytea (raw bytes; the column is passed through unconverted).
--    Identity partition on bytea is not covered here: DuckLake itself fails
--    to build the partition value from non-UTF8 bytes ("Invalid unicode ...
--    in value construction"), independent of the metadata backend.
-- ------------------------------------------------------------------
CREATE TABLE flush_bbytea (b bytea, val int) USING ducklake;
CALL ducklake.set_partition('flush_bbytea'::regclass, 'bucket(4, b)');
INSERT INTO flush_bbytea VALUES ('\x0102ff', 1), ('\x616263', 2), ('\x', 3);
DELETE FROM flush_bbytea WHERE val = 2;
SELECT * FROM ducklake.flush_inlined_data('flush_bbytea'::regclass);
SELECT b, val FROM flush_bbytea ORDER BY val;
DROP TABLE flush_bbytea;

-- ------------------------------------------------------------------
-- 8. hour() on time (the only transform DuckDB accepts on TIME;
--    year/month/day fail to bind there)
-- ------------------------------------------------------------------
CREATE TABLE flush_time (t time, val int) USING ducklake;
CALL ducklake.set_partition('flush_time'::regclass, 'hour(t)');
INSERT INTO flush_time VALUES ('22:31:08', 1), ('05:00:00', 2), ('22:59:59', 3);
DELETE FROM flush_time WHERE val = 2;
SELECT * FROM ducklake.flush_inlined_data('flush_time'::regclass);
SELECT t, val FROM flush_time ORDER BY val;
DROP TABLE flush_time;

CALL ducklake.set_option('data_inlining_row_limit', 0);

-- ------------------------------------------------------------------
-- 9. Equivalence: the PG UDFs must return exactly what DuckDB returns for the
--    same expression. The DuckDB side is evaluated via ducklake.query() and
--    captured with \gset; a false anywhere below is a dialect divergence.
-- ------------------------------------------------------------------

-- 9a. year/month/day/hour on TIMESTAMP (signature = year.month.day.hour)
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS ts1
    FROM (SELECT TIMESTAMP '2024-01-01 00:00:00' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS ts2
    FROM (SELECT TIMESTAMP '2023-12-31 23:59:59.999999' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS ts3
    FROM (SELECT TIMESTAMP '2024-02-29 12:34:56' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS ts4
    FROM (SELECT TIMESTAMP '1970-01-01 00:00:00' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS ts5
    FROM (SELECT TIMESTAMP '1969-12-31 23:59:59.999999' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS ts6
    FROM (SELECT TIMESTAMP '0001-01-01 00:00:00' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS ts7
    FROM (SELECT TIMESTAMP '9999-12-31 23:59:59' AS t) $$) \gset duck_

WITH v(name, duck, t) AS (VALUES
    ('ts1', :'duck_ts1', TIMESTAMP '2024-01-01 00:00:00'),
    ('ts2', :'duck_ts2', TIMESTAMP '2023-12-31 23:59:59.999999'),
    ('ts3', :'duck_ts3', TIMESTAMP '2024-02-29 12:34:56'),
    ('ts4', :'duck_ts4', TIMESTAMP '1970-01-01 00:00:00'),
    ('ts5', :'duck_ts5', TIMESTAMP '1969-12-31 23:59:59.999999'),
    ('ts6', :'duck_ts6', TIMESTAMP '0001-01-01 00:00:00'),
    ('ts7', :'duck_ts7', TIMESTAMP '9999-12-31 23:59:59'))
SELECT name, duck,
       duck = concat(ducklake.year(t), '.', ducklake.month(t), '.', ducklake.day(t), '.', ducklake.hour(t)) AS match
FROM v ORDER BY name;

-- 9b. year/month/day/hour on TIMESTAMPTZ (session TimeZone dependent; both
--     engines run with the same synced TimeZone here)
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS tz1
    FROM (SELECT TIMESTAMPTZ '2024-06-30 23:30:00+00' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS tz2
    FROM (SELECT TIMESTAMPTZ '2024-07-01 06:59:59+00' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS tz3
    FROM (SELECT TIMESTAMPTZ '2024-07-01 07:00:00+00' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS tz4
    FROM (SELECT TIMESTAMPTZ '2024-01-01 07:59:59+00' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS tz5
    FROM (SELECT TIMESTAMPTZ '1970-01-01 00:00:00+00' AS t) $$) \gset duck_

WITH v(name, duck, t) AS (VALUES
    ('tz1', :'duck_tz1', TIMESTAMPTZ '2024-06-30 23:30:00+00'),
    ('tz2', :'duck_tz2', TIMESTAMPTZ '2024-07-01 06:59:59+00'),
    ('tz3', :'duck_tz3', TIMESTAMPTZ '2024-07-01 07:00:00+00'),
    ('tz4', :'duck_tz4', TIMESTAMPTZ '2024-01-01 07:59:59+00'),
    ('tz5', :'duck_tz5', TIMESTAMPTZ '1970-01-01 00:00:00+00'))
SELECT name, duck,
       duck = concat(ducklake.year(t), '.', ducklake.month(t), '.', ducklake.day(t), '.', ducklake.hour(t)) AS match
FROM v ORDER BY name;

-- 9c. year/month/day/hour on DATE
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS d1
    FROM (SELECT DATE '2024-01-01' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS d2
    FROM (SELECT DATE '2024-02-29' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS d3
    FROM (SELECT DATE '1969-12-31' AS t) $$) \gset duck_
SELECT * FROM ducklake.query($$ SELECT
    concat(year(t), '.', month(t), '.', day(t), '.', hour(t)) AS d4
    FROM (SELECT DATE '9999-12-31' AS t) $$) \gset duck_

WITH v(name, duck, t) AS (VALUES
    ('d1', :'duck_d1', DATE '2024-01-01'),
    ('d2', :'duck_d2', DATE '2024-02-29'),
    ('d3', :'duck_d3', DATE '1969-12-31'),
    ('d4', :'duck_d4', DATE '9999-12-31'))
SELECT name, duck,
       duck = concat(ducklake.year(t), '.', ducklake.month(t), '.', ducklake.day(t), '.', ducklake.hour(t)) AS match
FROM v ORDER BY name;

-- 9d. murmur3_32 integer family. 34 hashes as a sign-extended 8-byte long for
--     every integer width (2017239379 is the Iceberg spec reference vector).
SELECT * FROM ducklake.query($$ SELECT
    murmur3_32(CAST(34 AS BIGINT)) AS h_i8_34,
    murmur3_32(CAST(0 AS BIGINT)) AS h_i8_0,
    murmur3_32(CAST(-1 AS BIGINT)) AS h_i8_neg1,
    murmur3_32(CAST(-34 AS BIGINT)) AS h_i8_neg34,
    murmur3_32(CAST(9223372036854775807 AS BIGINT)) AS h_i8_max,
    murmur3_32(CAST(-9223372036854775808 AS BIGINT)) AS h_i8_min,
    murmur3_32(CAST(34 AS INTEGER)) AS h_i4_34,
    murmur3_32(CAST(-2147483648 AS INTEGER)) AS h_i4_min,
    murmur3_32(CAST(2147483647 AS INTEGER)) AS h_i4_max,
    murmur3_32(CAST(34 AS SMALLINT)) AS h_i2_34,
    murmur3_32(CAST(-1 AS SMALLINT)) AS h_i2_neg1,
    murmur3_32(true) AS h_bool_t,
    murmur3_32(false) AS h_bool_f $$) \gset duck_

SELECT :'duck_h_i8_34'::int  = ducklake.murmur3_32(34::bigint) AS i8_34,
       :'duck_h_i8_34'::int  = 2017239379 AS i8_34_iceberg_vector,
       :'duck_h_i8_0'::int   = ducklake.murmur3_32(0::bigint) AS i8_0,
       :'duck_h_i8_neg1'::int = ducklake.murmur3_32((-1)::bigint) AS i8_neg1,
       :'duck_h_i8_neg34'::int = ducklake.murmur3_32((-34)::bigint) AS i8_neg34,
       :'duck_h_i8_max'::int = ducklake.murmur3_32(9223372036854775807::bigint) AS i8_max,
       :'duck_h_i8_min'::int = ducklake.murmur3_32((-9223372036854775808)::bigint) AS i8_min,
       :'duck_h_i4_34'::int  = ducklake.murmur3_32(34::integer) AS i4_34,
       :'duck_h_i4_34'::int  = ducklake.murmur3_32(34::bigint) AS i4_equals_i8,
       :'duck_h_i4_min'::int = ducklake.murmur3_32((-2147483648)::integer) AS i4_min,
       :'duck_h_i4_max'::int = ducklake.murmur3_32(2147483647::integer) AS i4_max,
       :'duck_h_i2_34'::int  = ducklake.murmur3_32(34::smallint) AS i2_34,
       :'duck_h_i2_neg1'::int = ducklake.murmur3_32((-1)::smallint) AS i2_neg1,
       :'duck_h_bool_t'::int = ducklake.murmur3_32(true) AS bool_t,
       :'duck_h_bool_f'::int = ducklake.murmur3_32(false) AS bool_f;

-- 9e. murmur3_32 text ('iceberg' = 1210000089 in the Iceberg spec; empty
--     string, a 4-byte block, block+tail, and non-ASCII UTF-8)
SELECT * FROM ducklake.query($$ SELECT
    murmur3_32('iceberg') AS h_iceberg,
    murmur3_32('') AS h_empty,
    murmur3_32('abcd') AS h_abcd,
    murmur3_32('abcde') AS h_abcde,
    murmur3_32(chr(233) || 't' || chr(233)) AS h_unicode $$) \gset duck_

SELECT :'duck_h_iceberg'::int = ducklake.murmur3_32('iceberg') AS str_iceberg,
       :'duck_h_iceberg'::int = 1210000089 AS str_iceberg_vector,
       :'duck_h_empty'::int   = ducklake.murmur3_32('') AS str_empty,
       :'duck_h_abcd'::int    = ducklake.murmur3_32('abcd') AS str_abcd,
       :'duck_h_abcde'::int   = ducklake.murmur3_32('abcde') AS str_abcde,
       :'duck_h_unicode'::int = ducklake.murmur3_32(chr(233) || 't' || chr(233)) AS str_unicode;

-- 9f. murmur3_32 float/double (widened to double, -0.0 normalized to +0.0)
SELECT * FROM ducklake.query($$ SELECT
    murmur3_32(CAST(1.0 AS FLOAT)) AS h_f4_1,
    murmur3_32(CAST(0.0 AS FLOAT)) AS h_f4_0,
    murmur3_32(CAST(-0.0 AS FLOAT)) AS h_f4_neg0,
    murmur3_32(CAST(1.0 AS DOUBLE)) AS h_f8_1,
    murmur3_32(CAST(-1.5 AS DOUBLE)) AS h_f8_neg15 $$) \gset duck_

SELECT :'duck_h_f4_1'::int    = ducklake.murmur3_32(1.0::real) AS f4_1,
       :'duck_h_f4_0'::int    = ducklake.murmur3_32(0.0::real) AS f4_0,
       :'duck_h_f4_neg0'::int = ducklake.murmur3_32('-0.0'::real) AS f4_neg0,
       :'duck_h_f4_neg0'::int = ducklake.murmur3_32(0.0::real) AS f4_neg0_normalized,
       :'duck_h_f4_1'::int    = ducklake.murmur3_32(1.0::double precision) AS f4_equals_f8,
       :'duck_h_f8_1'::int    = ducklake.murmur3_32(1.0::double precision) AS f8_1,
       :'duck_h_f8_neg15'::int = ducklake.murmur3_32((-1.5)::double precision) AS f8_neg15;

-- 9g. murmur3_32 date/time/timestamp/timestamptz (Iceberg vectors:
--     date 2017-11-16 = -653330422, time 22:31:08 = -662762989,
--     timestamp 2017-11-16T22:31:08 = -2047944441)
SELECT * FROM ducklake.query($$ SELECT
    murmur3_32(DATE '2017-11-16') AS h_d_iceberg,
    murmur3_32(DATE '1970-01-01') AS h_d_epoch,
    murmur3_32(DATE '1969-12-31') AS h_d_preepoch,
    murmur3_32(TIME '22:31:08') AS h_t_iceberg,
    murmur3_32(TIMESTAMP '2017-11-16 22:31:08') AS h_ts_iceberg,
    murmur3_32(TIMESTAMP '1970-01-01 00:00:00') AS h_ts_epoch,
    murmur3_32(TIMESTAMPTZ '2017-11-16 22:31:08+00') AS h_tstz_utc,
    murmur3_32(TIMESTAMPTZ '2017-11-16 14:31:08-08') AS h_tstz_offset $$) \gset duck_

SELECT :'duck_h_d_iceberg'::int  = ducklake.murmur3_32(DATE '2017-11-16') AS d_iceberg,
       :'duck_h_d_iceberg'::int  = -653330422 AS d_iceberg_vector,
       :'duck_h_d_epoch'::int    = ducklake.murmur3_32(DATE '1970-01-01') AS d_epoch,
       :'duck_h_d_preepoch'::int = ducklake.murmur3_32(DATE '1969-12-31') AS d_preepoch,
       :'duck_h_t_iceberg'::int  = ducklake.murmur3_32(TIME '22:31:08') AS t_iceberg,
       :'duck_h_t_iceberg'::int  = -662762989 AS t_iceberg_vector,
       :'duck_h_ts_iceberg'::int = ducklake.murmur3_32(TIMESTAMP '2017-11-16 22:31:08') AS ts_iceberg,
       :'duck_h_ts_iceberg'::int = -2047944441 AS ts_iceberg_vector,
       :'duck_h_ts_epoch'::int   = ducklake.murmur3_32(TIMESTAMP '1970-01-01 00:00:00') AS ts_epoch,
       :'duck_h_tstz_utc'::int   = ducklake.murmur3_32(TIMESTAMPTZ '2017-11-16 22:31:08+00') AS tstz_utc,
       :'duck_h_tstz_offset'::int = ducklake.murmur3_32(TIMESTAMPTZ '2017-11-16 22:31:08+00') AS tstz_same_instant;

-- 9h. murmur3_32 numeric/uuid/bytea. DECIMAL(18,3) 12.5 must hash like its
--     unscaled long 12500; DECIMAL(30,10) hashes its fixed-scale string form;
--     UUID hashes its lowercase hyphenated string; BYTEA hashes raw bytes.
--     hour(TIME) is the one temporal transform DuckDB accepts on TIME.
SELECT * FROM ducklake.query($$ SELECT
    murmur3_32(CAST(12.5 AS DECIMAL(18,3))) AS h_dec,
    murmur3_32(CAST(-7.25 AS DECIMAL(18,3))) AS h_dec_neg,
    murmur3_32(CAST(0.001 AS DECIMAL(18,3))) AS h_dec_small,
    murmur3_32(CAST(0 AS DECIMAL(10,0))) AS h_dec_zero,
    murmur3_32(CAST(12.5 AS DECIMAL(30,10))) AS h_widedec,
    murmur3_32(UUID 'f79c3e09-677c-4bbd-a479-3f349cb785e7') AS h_uuid,
    murmur3_32(UUID '00000000-0000-0000-0000-000000000000') AS h_uuid_zero,
    murmur3_32(BLOB '\x01\x02\xFF') AS h_blob,
    murmur3_32(BLOB '') AS h_blob_empty,
    hour(TIME '22:31:08') AS h_hour_time $$) \gset duck_

SELECT :'duck_h_dec'::int       = ducklake.murmur3_32(12.5::numeric(18,3)) AS dec,
       :'duck_h_dec'::int       = ducklake.murmur3_32(12500::bigint) AS dec_is_unscaled_long,
       :'duck_h_dec_neg'::int   = ducklake.murmur3_32((-7.25)::numeric(18,3)) AS dec_neg,
       :'duck_h_dec_small'::int = ducklake.murmur3_32(0.001::numeric(18,3)) AS dec_small,
       :'duck_h_dec_zero'::int  = ducklake.murmur3_32(0::numeric(10,0)) AS dec_zero,
       :'duck_h_widedec'::int   = ducklake.murmur3_32((12.5::numeric(30,10))::text) AS widedec_is_string,
       :'duck_h_uuid'::int      = ducklake.murmur3_32('f79c3e09-677c-4bbd-a479-3f349cb785e7'::uuid) AS uuid,
       :'duck_h_uuid_zero'::int = ducklake.murmur3_32('00000000-0000-0000-0000-000000000000'::uuid) AS uuid_zero,
       :'duck_h_blob'::int      = ducklake.murmur3_32('\x0102ff'::bytea) AS blob,
       :'duck_h_blob_empty'::int = ducklake.murmur3_32(''::bytea) AS blob_empty,
       :'duck_h_hour_time'::bigint = ducklake.hour('22:31:08'::time) AS hour_time;

-- 9i. NULL handling: both engines yield NULL (the flush filter then uses
--     "expr IS NULL" for NULL partition values, which matches in both)
SELECT * FROM ducklake.query($$ SELECT
    month(NULL::TIMESTAMPTZ) IS NULL AS duck_month_null,
    murmur3_32(NULL::INTEGER) IS NULL AS duck_murmur_null $$);
SELECT ducklake.month(NULL::timestamptz) IS NULL AS pg_month_null,
       ducklake.murmur3_32(NULL::integer) IS NULL AS pg_murmur_null;

-- Full bucket expression parity, exactly as generated by DuckLake
SELECT * FROM ducklake.query($$ SELECT
    (murmur3_32(CAST(42 AS INTEGER)) & 2147483647) % 4 AS bucket42 $$) \gset duck_
SELECT :'duck_bucket42'::int = (ducklake.murmur3_32(CAST(42 AS INTEGER)) & 2147483647) % 4 AS bucket_expr_match;
