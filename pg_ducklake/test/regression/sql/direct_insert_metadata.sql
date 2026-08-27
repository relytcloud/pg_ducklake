-- Direct-insert (fast path) metadata commit protocol.
-- The fast path hand-writes the DuckLake commit; these tests pin the parts of
-- the protocol other DuckLake machinery relies on.

CALL ducklake.set_option('data_inlining_row_limit', 100);

-- ============================================================
-- 1. next_file_id must advance on a fast-path commit
-- ============================================================
-- DuckLake keys its table-stats cache on (next_file_id, schema_version,
-- table_id) and bumps next_file_id on inlined-only commits to signal a data
-- change. A fast-path commit that carries it forward leaves stale cached
-- stats (next_row_id, record_count, min/max) in every open DuckDB instance.

CREATE TABLE dim_t (id int, val text) USING ducklake;
INSERT INTO dim_t VALUES (0, 'seed');  -- normal path: creates the inlined data table

SELECT next_file_id AS file_id_before
FROM ducklake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1 \gset

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO dim_t VALUES (1, 'fast');
-- fast path handled the insert above
SELECT pattern, reason, count FROM ducklake.direct_insert_stats() WHERE count > 0;

SELECT next_file_id > :file_id_before AS file_id_advanced
FROM ducklake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1;

-- ============================================================
-- 2. row ids must stay unique when fast-path and normal-path
--    inserts interleave
-- ============================================================
-- Load this backend's DuckDB table-stats cache, then advance next_row_id
-- through the fast path (PG metadata only). The normal-path insert below
-- must not seed its row ids from the stale cache entry.
SELECT count(*) FROM dim_t WHERE id >= 0;
INSERT INTO dim_t VALUES (2, 'fast2');
BEGIN;  -- transaction block: fast path disengages, normal DuckLake path
INSERT INTO dim_t VALUES (3, 'slow');
COMMIT;

SELECT it.table_name AS dim_inl
FROM ducklake.ducklake_inlined_data_tables it
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'dim_t' AND t.end_snapshot IS NULL
ORDER BY it.schema_version DESC LIMIT 1 \gset

SELECT row_id, count(*) AS dup_count
FROM ducklake.:dim_inl GROUP BY row_id HAVING count(*) > 1;

SELECT count(*) AS inlined_rows, count(DISTINCT row_id) AS distinct_row_ids
FROM ducklake.:dim_inl;

-- deleting one row must not take an unrelated row with it
DELETE FROM dim_t WHERE id = 3;
SELECT * FROM dim_t ORDER BY id;

-- ============================================================
-- 3. global column stats must not claim ranges that exclude
--    fast-path rows
-- ============================================================
-- The fast path maintains min/max by widening them to cover the rows it
-- writes, so the recorded range never excludes a committed row. Stale claims
-- are trusted by DuckLake readers of this catalog (global stats feed DuckDB
-- optimizer statistics, which fold provably-false filters) and are
-- perpetuated by later stats merges.

CREATE TABLE stats_t (id int) USING ducklake;
INSERT INTO stats_t SELECT i FROM generate_series(1, 200) i;  -- normal path, real stats

-- cache the (accurate, soon stale) stats in this backend
SELECT count(*) FROM stats_t WHERE id = 150;

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO stats_t VALUES (1000);  -- outside the recorded min/max
INSERT INTO stats_t VALUES (NULL);  -- would violate contains_null = false
SELECT pattern, reason, count FROM ducklake.direct_insert_stats() WHERE count > 0;

-- A stale contains_null = false is not a lost optimization: the optimizer
-- folds IS NULL to false and the row disappears from the result.
SELECT s.min_value, s.max_value, s.contains_null
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL
WHERE t.table_name = 'stats_t';

SELECT count(*) FROM stats_t WHERE id = 1000;
SELECT count(*) FROM stats_t WHERE id IS NULL;
SELECT count(*) FROM stats_t;

-- ============================================================
-- 4. contains_null is maintained for every column, not just
--    the ones whose type has a maintainable min/max
-- ============================================================
-- Seeded from parquet so contains_null starts known-false. An inline-only
-- table would leave it unknown instead, which section 6 covers.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE nullstats (id int, v int, f double precision, w int, bs bytea) USING ducklake;
INSERT INTO nullstats SELECT i, i, i::float8, i, '\x01'::bytea FROM generate_series(1, 200) i;
CALL ducklake.set_option('data_inlining_row_limit', 100);
INSERT INTO nullstats VALUES (901, 7, 1.5, 7, '\x02');  -- normal path: creates the inlined data table

-- f (DOUBLE) and bs (BLOB) are excluded from min/max by StatsEligible, so they
-- pin that null-ness is not gated by that exclusion. BLOB additionally is the
-- only fast-path-reachable type whose persisted row can carry extra_stats.
SELECT ducklake.reset_direct_insert_stats();
INSERT INTO nullstats VALUES (902, NULL, NULL, 8, NULL), (903, NULL, NULL, 9, NULL);
SELECT pattern, reason, count FROM ducklake.direct_insert_stats() WHERE count > 0;

-- v's bounds must stay at [1,200]: an all-NULL batch moves no bound.
SELECT s.column_id, s.contains_null, s.min_value, s.max_value
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL
WHERE t.table_name = 'nullstats' ORDER BY s.column_id;

SELECT count(*) FROM nullstats WHERE v IS NULL;
SELECT count(*) FROM nullstats WHERE f IS NULL;

-- A batch with no NULLs must not reset it: the flip is one-way.
INSERT INTO nullstats VALUES (904, 8, 2.5, 10, '\x03');
SELECT s.column_id, s.contains_null
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL
WHERE t.table_name = 'nullstats' ORDER BY s.column_id;
SELECT count(*) FROM nullstats WHERE v IS NULL;

-- Seed extra_stats so the COPY assertion proves invalidation rather than only
-- observing the default NULL.
UPDATE ducklake.ducklake_table_column_stats s SET extra_stats = 'seeded'
FROM ducklake.ducklake_table t, ducklake.ducklake_column c
WHERE t.table_id = s.table_id AND c.table_id = s.table_id
  AND c.column_id = s.column_id AND t.table_name = 'nullstats'
  AND t.end_snapshot IS NULL AND c.end_snapshot IS NULL
  AND c.column_name = 'bs';
SELECT s.extra_stats = 'seeded' AS extra_stats_seeded
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
JOIN ducklake.ducklake_column c USING (table_id, column_id)
WHERE t.table_name = 'nullstats' AND t.end_snapshot IS NULL
  AND c.column_name = 'bs' AND c.end_snapshot IS NULL;

-- w's first NULL, so column 4 must flip here and nowhere earlier -- otherwise
-- this asserts nothing about the COPY writer.
COPY nullstats (id, v, f, w, bs) FROM STDIN WITH (FORMAT csv, NULL '');
905,,3.5,,\x04
\.
SELECT s.column_id, s.contains_null
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL
WHERE t.table_name = 'nullstats' ORDER BY s.column_id;
SELECT count(*) FROM nullstats WHERE v IS NULL;
SELECT count(*) FROM nullstats WHERE w IS NULL;
SELECT count(*) FROM nullstats;
SELECT s.extra_stats IS NULL AS extra_stats_invalidated
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
JOIN ducklake.ducklake_column c USING (table_id, column_id)
WHERE t.table_name = 'nullstats' AND t.end_snapshot IS NULL
  AND c.column_name = 'bs' AND c.end_snapshot IS NULL;

-- ============================================================
-- 5. the UNNEST writer maintains it too
-- ============================================================
-- Its own null branch, so a fix applied to the VALUES and COPY writers can miss
-- it. Needs a dedicated table: an UNNEST naming a column subset does not reach
-- the fast path at all.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE nullstats_un (id int, v int) USING ducklake;
INSERT INTO nullstats_un SELECT i, i FROM generate_series(1, 200) i;
CALL ducklake.set_option('data_inlining_row_limit', 100);
INSERT INTO nullstats_un VALUES (901, 7);

PREPARE nullstats_un_ins (int[], int[]) AS
  INSERT INTO nullstats_un (id, v) SELECT UNNEST($1), UNNEST($2);
SELECT ducklake.reset_direct_insert_stats();
EXECUTE nullstats_un_ins(ARRAY[902, 903], ARRAY[NULL, NULL]::int[]);
SELECT pattern, reason, count FROM ducklake.direct_insert_stats() WHERE count > 0;

SELECT s.column_id, s.contains_null, s.min_value, s.max_value
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL
WHERE t.table_name = 'nullstats_un' ORDER BY s.column_id;
SELECT count(*) FROM nullstats_un WHERE v IS NULL;
SELECT count(*) FROM nullstats_un;
DEALLOCATE nullstats_un_ins;

-- ============================================================
-- 6. the first native write initializes table and column stats
-- ============================================================
CALL ducklake.set_option('data_inlining_row_limit', 1000);
CREATE TABLE nullstats_inl (id int, v int) USING ducklake;
SELECT count(*) AS table_stats_before
FROM ducklake.ducklake_table_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'nullstats_inl' AND t.end_snapshot IS NULL;
INSERT INTO nullstats_inl VALUES (1, 1);
SELECT s.record_count, s.next_row_id
FROM ducklake.ducklake_table_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'nullstats_inl' AND t.end_snapshot IS NULL;
SELECT s.column_id, s.contains_null, s.min_value, s.max_value
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'nullstats_inl' AND t.end_snapshot IS NULL
ORDER BY s.column_id;

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO nullstats_inl VALUES (2, NULL), (3, 3);
SELECT pattern, reason, count FROM ducklake.direct_insert_stats() WHERE count > 0;

SELECT s.column_id, s.contains_null
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL
WHERE t.table_name = 'nullstats_inl' ORDER BY s.column_id;
SELECT count(*) FROM nullstats_inl WHERE v IS NULL;

SELECT * FROM ducklake.flush_inlined_data('nullstats_inl'::regclass);
SELECT count(*) FROM nullstats_inl WHERE v IS NULL;

-- ============================================================
-- 7. temporal bounds use DuckLake-canonical values
-- ============================================================
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE temporalstats (d date, ts timestamp) USING ducklake;
INSERT INTO temporalstats
SELECT (DATE '2020-01-01' + i * INTERVAL '1 day')::date,
       TIMESTAMP '2020-01-01' + i * INTERVAL '1 day'
FROM generate_series(1, 200) i;
CALL ducklake.set_option('data_inlining_row_limit', 100);
SELECT count(*) FROM ducklake.ensure_inlined_data_table('temporalstats'::regclass);
SELECT ducklake.reset_direct_insert_stats();
INSERT INTO temporalstats VALUES (DATE '2030-01-01', TIMESTAMP '2030-01-01');
SELECT pattern, reason, count FROM ducklake.direct_insert_stats() WHERE count > 0;
SELECT s.column_id,
       CASE s.column_id
         WHEN 1 THEN s.max_value::date >= DATE '2030-01-01'
         WHEN 2 THEN s.max_value::timestamp >= TIMESTAMP '2030-01-01'
       END AS safe
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL
WHERE t.table_name = 'temporalstats' ORDER BY s.column_id;

-- ============================================================
-- 8. FLOAT/DOUBLE bounds must cover direct-inserted rows
-- ============================================================
-- A parquet write records contains_nan=false, which makes DuckLake expose
-- FLOAT/DOUBLE min/max to the optimizer. Leaving those bounds stale after a
-- direct insert can therefore prune the new rows once the inlined-scan stats
-- guard is removed.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE floatstats (f real, d double precision) USING ducklake;
INSERT INTO floatstats SELECT i::real, i::double precision FROM generate_series(1, 200) i;
CALL ducklake.set_option('data_inlining_row_limit', 100);
-- Create the inlined table without a native inline write, which would degrade
-- contains_nan to unknown before the direct-insert path can observe it.
SELECT count(*) FROM ducklake.ensure_inlined_data_table('floatstats'::regclass);

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO floatstats VALUES (1000, 1000);
SELECT pattern, reason, count FROM ducklake.direct_insert_stats() WHERE count > 0;

-- Either widen the bound or make NaN knowledge unknown so DuckLake does not
-- expose FLOAT/DOUBLE optimizer stats. A known-false contains_nan plus a stale
-- max is unsafe.
SELECT s.column_id,
       s.contains_nan IS DISTINCT FROM false OR s.max_value::double precision >= 1000 AS safe
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL
WHERE t.table_name = 'floatstats' ORDER BY s.column_id;

-- Infinity must widen still-active finite bounds before any NaN disables them.
INSERT INTO floatstats VALUES ('Infinity', '-Infinity');
SELECT s.column_id, s.contains_nan,
       CASE s.column_id
         WHEN 1 THEN s.max_value::real = 'Infinity'::real
         WHEN 2 THEN s.min_value::double precision = '-Infinity'::double precision
       END AS infinity_bound
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'floatstats' AND t.end_snapshot IS NULL
ORDER BY s.column_id;

-- A NaN-only batch flips the claim without discarding infinity bounds.
INSERT INTO floatstats VALUES ('NaN', 'NaN');
SELECT s.column_id, s.contains_nan,
       s.max_value::double precision = 'Infinity'::double precision OR
       s.min_value::double precision = '-Infinity'::double precision AS infinity_retained
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'floatstats' AND t.end_snapshot IS NULL
ORDER BY s.column_id;

-- Exercise NaN and infinity in one native batch independently of the prior
-- NaN-only transition.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE floatstats_mixed (f real, d double precision) USING ducklake;
INSERT INTO floatstats_mixed
SELECT i::real, i::double precision FROM generate_series(1, 200) i;
CALL ducklake.set_option('data_inlining_row_limit', 100);
SELECT count(*) FROM ducklake.ensure_inlined_data_table('floatstats_mixed'::regclass);
INSERT INTO floatstats_mixed VALUES ('NaN', 'NaN'), ('Infinity', '-Infinity');
SELECT s.column_id, s.contains_nan,
       CASE s.column_id
         WHEN 1 THEN s.max_value::real = 'Infinity'::real
         WHEN 2 THEN s.min_value::double precision = '-Infinity'::double precision
       END AS infinity_bound
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'floatstats_mixed' AND t.end_snapshot IS NULL
ORDER BY s.column_id;

-- A missing persisted side stays unknown, while the known side can widen.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE one_sided_stats (id int) USING ducklake;
INSERT INTO one_sided_stats SELECT i FROM generate_series(1, 200) AS i;
CALL ducklake.set_option('data_inlining_row_limit', 100);
SELECT count(*) FROM ducklake.ensure_inlined_data_table('one_sided_stats'::regclass);
UPDATE ducklake.ducklake_table_column_stats s SET min_value = NULL
FROM ducklake.ducklake_table t
WHERE t.table_id = s.table_id AND t.table_name = 'one_sided_stats'
  AND t.end_snapshot IS NULL;
INSERT INTO one_sided_stats VALUES (1000);
SELECT s.min_value, s.max_value
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'one_sided_stats' AND t.end_snapshot IS NULL;

-- Exercise an actual type transition and merge the INTEGER-era bounds as
-- BIGINT before checking invalidation of a malformed stale bound.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE type_change_stats (v integer) USING ducklake;
INSERT INTO type_change_stats SELECT i FROM generate_series(1, 200) AS i;
ALTER TABLE type_change_stats ALTER COLUMN v TYPE bigint;
CALL ducklake.set_option('data_inlining_row_limit', 100);
SELECT count(*) FROM ducklake.ensure_inlined_data_table('type_change_stats'::regclass);
INSERT INTO type_change_stats VALUES (1000);
SELECT s.min_value, s.max_value
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'type_change_stats' AND t.end_snapshot IS NULL;
UPDATE ducklake.ducklake_table_column_stats s
SET min_value = 'stale-integer-bound'
FROM ducklake.ducklake_table t
WHERE t.table_id = s.table_id AND t.table_name = 'type_change_stats'
  AND t.end_snapshot IS NULL;
INSERT INTO type_change_stats VALUES (1001);
SELECT s.min_value IS NULL AS min_invalidated,
       s.max_value IS NULL AS max_invalidated
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'type_change_stats' AND t.end_snapshot IS NULL;

-- 9. A batch already covered by persisted stats must not rewrite the column
-- stats row. A widened bound must update it.
CREATE TABLE unchanged_stats (id integer) USING ducklake;
SET ducklake.enable_direct_insert = false;
INSERT INTO unchanged_stats VALUES (1), (100);
SET ducklake.enable_direct_insert = true;
SELECT s.ctid::text AS stats_tid
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'unchanged_stats' AND t.end_snapshot IS NULL
\gset
INSERT INTO unchanged_stats VALUES (50);
SELECT s.ctid::text = :'stats_tid' AS stats_unchanged
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'unchanged_stats' AND t.end_snapshot IS NULL;
INSERT INTO unchanged_stats VALUES (200);
SELECT s.ctid::text <> :'stats_tid' AS stats_widened
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'unchanged_stats' AND t.end_snapshot IS NULL;

-- ============================================================
-- 10. bounds must describe the stored value, not the supplied one
-- ============================================================
-- numeric is the only inlined type whose typmod coercion rewrites the datum,
-- so it is the only type that can catch a bound observed before the coercion.
-- Both values round outward: 9.8765 stores as 9.877, -9.8765 as -9.877.
CALL ducklake.set_option('data_inlining_row_limit', 1000);
CREATE TABLE scalestats (id int, n numeric(18,3)) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('scalestats'::regclass);

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO scalestats VALUES (1, 1.2345), (2, 9.8765), (3, -9.8765);
SELECT pattern, reason, count FROM ducklake.direct_insert_stats() WHERE count > 0;

SELECT id, n FROM scalestats ORDER BY id;
SELECT s.min_value, s.max_value,
       s.min_value::numeric <= (SELECT min(n) FROM scalestats) AS min_covers,
       s.max_value::numeric >= (SELECT max(n) FROM scalestats) AS max_covers
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL
WHERE t.table_name = 'scalestats' AND s.column_id = 2;

-- UNNEST is absent: it cannot reach a column with a declared typmod.
CREATE TABLE scalestats_copy (id int, n numeric(18,3)) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('scalestats_copy'::regclass);
COPY scalestats_copy FROM STDIN;
1	1.2345
2	9.8765
3	-9.8765
\.
SELECT id, n FROM scalestats_copy ORDER BY id;
SELECT s.min_value, s.max_value,
       s.min_value::numeric <= (SELECT min(n) FROM scalestats_copy) AS min_covers,
       s.max_value::numeric >= (SELECT max(n) FROM scalestats_copy) AS max_covers
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t ON t.table_id = s.table_id AND t.end_snapshot IS NULL
WHERE t.table_name = 'scalestats_copy' AND s.column_id = 2;

-- A bound that excludes a stored value surfaces as a missing row, not an error.
SELECT * FROM ducklake.flush_inlined_data('scalestats'::regclass);
SELECT id, n FROM scalestats WHERE n IN (9.877, -9.877) ORDER BY id;

-- Cleanup
DROP TABLE dim_t;
DROP TABLE stats_t;
DROP TABLE nullstats;
DROP TABLE nullstats_un;
DROP TABLE nullstats_inl;
DROP TABLE temporalstats;
DROP TABLE floatstats;
DROP TABLE floatstats_mixed;
DROP TABLE one_sided_stats;
DROP TABLE type_change_stats;
DROP TABLE unchanged_stats;
DROP TABLE scalestats;
DROP TABLE scalestats_copy;
CALL ducklake.set_option('data_inlining_row_limit', 0);
