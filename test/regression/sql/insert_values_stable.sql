-- Test direct-insert VALUES path with STABLE coercions / functions.
-- Before the deferred-evaluation refactor, eval_const_expressions
-- only folded IMMUTABLE function calls, so any STABLE coercion
-- (timestamptz->timestamp, current_date+1, etc.) would fall back to
-- the standard DuckDB executor.  After the refactor, cell expressions
-- are evaluated at executor start instead of demanding a planning-
-- time Const.

SET TIMEZONE = 'UTC';
CALL ducklake.set_option('data_inlining_row_limit', 100);

CREATE TABLE ivs (ts timestamp, d date, n int) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('ivs'::regclass);

SELECT ducklake.reset_direct_insert_stats();

-- ============================================================
-- Test 1: timestamptz literal cast to timestamp (the issue's repro)
-- ============================================================
EXPLAIN INSERT INTO ivs VALUES ('2026-04-27T06:37:14+00:00'::timestamptz, NULL, NULL);
INSERT INTO ivs VALUES ('2026-04-27T06:37:14+00:00'::timestamptz, NULL, 1);

-- ============================================================
-- Test 2: STABLE FuncExpr (current_date) -- single-row
-- ============================================================
EXPLAIN INSERT INTO ivs VALUES (NULL, current_date, 2);
INSERT INTO ivs VALUES (NULL, current_date, 2);

-- ============================================================
-- Test 3: STABLE FuncExpr in arithmetic (current_date + 1)
-- ============================================================
INSERT INTO ivs VALUES (NULL, current_date + 1, 3);

-- ============================================================
-- Test 4: Multi-row VALUES, mixed Const + STABLE, full coverage
-- ============================================================
EXPLAIN INSERT INTO ivs VALUES
    ('2030-01-01 00:00:00'::timestamp, current_date, 4),
    (NULL, current_date + 7, 5);
INSERT INTO ivs VALUES
    ('2030-01-01 00:00:00'::timestamp, current_date, 4),
    (NULL, current_date + 7, 5);

-- Each EXPLAIN and each INSERT runs through the planner hook and
-- bumps the counter once per statement (EXPLAIN does not execute,
-- but the plan is still produced and counted).
SELECT pattern, reason, count
    FROM ducklake.direct_insert_stats() WHERE count > 0
    ORDER BY pattern, reason;

-- Verify rows landed correctly (insensitive to today's date)
SELECT n,
       ts AT TIME ZONE 'UTC' = TIMESTAMP '2026-04-27 06:37:14' AS ts_match_1,
       ts = TIMESTAMP '2030-01-01 00:00:00' AS ts_match_4,
       d = CURRENT_DATE AS d_today,
       d = CURRENT_DATE + 1 AS d_plus1,
       d = CURRENT_DATE + 7 AS d_plus7
FROM ivs WHERE n IS NOT NULL ORDER BY n;

-- ============================================================
-- Test 5: VOLATILE function inside VALUES is rejected.  random()
-- is VOLATILE; the planner falls back to the pg_duckdb path, which
-- is observable as an unmatched counter bump.  We use EXPLAIN so
-- the test does not depend on the fall-back path executing
-- successfully (and on it not interacting with subsequent tests'
-- ducklake metadata state).
-- ============================================================
SELECT ducklake.reset_direct_insert_stats();
EXPLAIN INSERT INTO ivs VALUES (NULL, NULL, (random() * 100)::int);
SELECT pattern, reason, count
    FROM ducklake.direct_insert_stats() WHERE count > 0
    ORDER BY pattern, reason;

-- ============================================================
-- Test 6: INSERT ... SELECT FROM <table> with constant target list
-- must NOT direct-insert (FROM clause means N rows, not 1).  Before
-- the guard added in this PR, the deferred-eval walker accepted the
-- constant target list and would have inserted exactly one row.
-- Same EXPLAIN-only verification.
-- ============================================================
CREATE TABLE ivs_src (i int);

SELECT ducklake.reset_direct_insert_stats();
EXPLAIN INSERT INTO ivs SELECT NULL::timestamp, NULL::date, 200 FROM ivs_src;
SELECT pattern, reason, count
    FROM ducklake.direct_insert_stats() WHERE count > 0
    ORDER BY pattern, reason;

-- Cleanup
DROP TABLE ivs_src;
DROP TABLE ivs;
RESET TIMEZONE;
-- Restore the row_limit to what previous tests in the schedule left
-- behind so the next test (copy_from, which expects inlining) works.
CALL ducklake.set_option('data_inlining_row_limit', 1000);
