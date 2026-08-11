-- Upstream: test/sql/stats/filter_pushdown.test
-- Skip: PostgreSQL EXPLAIN cannot expose DuckDB's per-file read counters, so physical file-pruning cardinalities are unobservable.
-- Min/max-backed filters must still preserve results for numeric, date, decimal, and text predicates.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_filter_stats (v integer, i integer, d date, k decimal(9,3), s text) USING ducklake;
INSERT INTO upstream_filter_stats
SELECT i % 1000, i, date '2000-01-01' + ((i % 100)::integer), i / 10.0, lpad(i::text, 6, '0')
FROM generate_series(0, 999) AS g(i);
INSERT INTO upstream_filter_stats
SELECT i % 1000, i, date '2010-01-01' + ((i % 100)::integer), i / 10.0, lpad(i::text, 6, '0')
FROM generate_series(100000, 100999) AS g(i);
SELECT * FROM upstream_filter_stats WHERE i = 527;
SELECT count(*) FROM upstream_filter_stats WHERE i > 100998;
SELECT count(*) FROM upstream_filter_stats WHERE d = date '2000-01-23';
SELECT * FROM upstream_filter_stats WHERE k = 25.300;
SELECT count(*) FROM upstream_filter_stats WHERE s >= '100900';
SELECT count(*) FROM upstream_filter_stats WHERE i IN (500, 600, 700);
DROP TABLE upstream_filter_stats;
