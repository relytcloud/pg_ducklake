-- Every inline writer buffers MAX_BUFFERED_TUPLES = 1000 rows at a time and
-- resets the memory context holding a batch's coerced Datums when it moves to
-- the next one.  No other test in the suite inserts more than a couple of
-- hundred rows in one statement, so no other test reaches the second batch.
--
-- numeric is the type that makes the reset observable: a plain numeric column
-- inlines into numeric(18,3), so the writer coerces every value to that
-- typmod and stores a Datum it allocated itself rather than a pointer into
-- the source tuple.  The values carry four decimals so the coercion has to
-- round (x.1235 -> x.124) instead of passing the input through.  A declared
-- numeric(p,s) would defeat the point: the coercion node the target typmod
-- adds is a shape the UNNEST fast path does not match.
--
-- The statements are generated rather than written out: a 1200-row VALUES
-- list or a 1200-element array literal is one unreadable line in both the
-- input and the expected output.  Readback is by aggregate plus the rows
-- either side of the boundary, for the same reason.

-- Above the row count of every insert below, or the fast path declines them
-- for overflowing the inlining limit in one shot and nothing is tested.
CALL ducklake.set_option('data_inlining_row_limit', 5000);

-- ------------------------------------------------------------------
-- VALUES
-- ------------------------------------------------------------------
CREATE TABLE ibb_values (i int, n numeric) USING ducklake;

SELECT ducklake.reset_direct_insert_stats();
DO $$
DECLARE stmt text;
BEGIN
  SELECT 'INSERT INTO ibb_values VALUES ' ||
         string_agg(format('(%s, %s.1235)', i, i), ', ' ORDER BY i)
    INTO stmt
    FROM generate_series(1, 1200) i;
  EXECUTE stmt;
END $$;

-- matched_values, or the rows never reached the batching writer.
SELECT pattern, reason, count FROM ducklake.direct_insert_stats()
WHERE count > 0 ORDER BY pattern, reason;

-- Must be 1: a flush would have moved the rows out of the inlined table and
-- past the writer under test.
SELECT count(*) AS values_inlined_tables
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table t ON t.table_id = idt.table_id
WHERE t.table_name = 'ibb_values' AND t.end_snapshot IS NULL;

SELECT count(*), sum(n), min(n), max(n) FROM ibb_values;
SELECT i, n FROM ibb_values WHERE i BETWEEN 999 AND 1002 ORDER BY i;

-- ------------------------------------------------------------------
-- UNNEST
-- ------------------------------------------------------------------
CREATE TABLE ibb_unnest (n numeric) USING ducklake;

SELECT ducklake.reset_direct_insert_stats();
DO $$
DECLARE arr numeric[];
BEGIN
  SELECT array_agg((i || '.1235')::numeric) INTO arr FROM generate_series(1, 1200) i;
  EXECUTE 'INSERT INTO ibb_unnest SELECT UNNEST($1)' USING arr;
END $$;

-- matched_unnest, or the rows never reached the batching writer.
SELECT pattern, reason, count FROM ducklake.direct_insert_stats()
WHERE count > 0 ORDER BY pattern, reason;

SELECT count(*), sum(n), min(n), max(n) FROM ibb_unnest;
SELECT n FROM ibb_unnest WHERE n >= 999 AND n < 1003 ORDER BY n;

-- COPY FROM STDIN is deliberately absent.  Its per-batch reset predates this
-- file and is unchanged, and crossing the boundary through it needs either
-- 1200 literal data lines or psql streaming from a shell command -- a
-- dependency no other test in this suite carries.  Add it here if COPY's
-- context handling is ever changed.

-- Both writers must agree; the count must be 0.
SELECT count(*) AS values_vs_unnest_diffs
FROM (SELECT n FROM ibb_values EXCEPT ALL SELECT n FROM ibb_unnest) d;

-- ------------------------------------------------------------------
-- Non-inlined reference: both writers above share the inlined reader, so
-- a defect common to them cannot show up in the comparisons above.
-- ------------------------------------------------------------------
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE ibb_parquet (n numeric) USING ducklake;
INSERT INTO ibb_parquet SELECT (i || '.1235')::numeric FROM generate_series(1, 1200) i;

-- Must be 0, or the comparisons below are comparing inlined against inlined.
SELECT count(*) AS reference_inlined_tables
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table t ON t.table_id = idt.table_id
WHERE t.table_name = 'ibb_parquet' AND t.end_snapshot IS NULL;

SELECT count(*), sum(n), min(n), max(n) FROM ibb_parquet;

-- Every count must be 0.
SELECT count(*) AS values_vs_parquet_diffs
FROM (SELECT n FROM ibb_values EXCEPT ALL SELECT n FROM ibb_parquet) d;
SELECT count(*) AS unnest_vs_parquet_diffs
FROM (SELECT n FROM ibb_unnest EXCEPT ALL SELECT n FROM ibb_parquet) d;

-- ------------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------------
DROP TABLE ibb_values;
DROP TABLE ibb_unnest;
DROP TABLE ibb_parquet;
SELECT ducklake.reset_direct_insert_stats();
