-- Upstream: test/sql/stats/topn_file_pruning.test
-- Skip: PostgreSQL EXPLAIN cannot expose DuckDB's dynamic-filter file-read counts, including the NULLS FIRST non-optimization case.
-- Top-N result semantics are retained below.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_topn_stats (ts timestamp, user_id text) USING ducklake;
INSERT INTO upstream_topn_stats SELECT timestamp '2026-01-01' + i * interval '1 second', 'a' FROM generate_series(0, 99) AS g(i);
INSERT INTO upstream_topn_stats SELECT timestamp '2026-01-02' + i * interval '1 second', 'b' FROM generate_series(0, 49) AS g(i);
INSERT INTO upstream_topn_stats SELECT timestamp '2026-01-03' + i * interval '1 second', 'c' FROM generate_series(0, 19) AS g(i);
INSERT INTO upstream_topn_stats SELECT timestamp '2026-01-04' + i * interval '1 second', 'd' FROM generate_series(0, 9) AS g(i);
SELECT * FROM upstream_topn_stats ORDER BY ts DESC LIMIT 2;
SELECT * FROM upstream_topn_stats ORDER BY ts ASC LIMIT 2;
SELECT * FROM upstream_topn_stats WHERE ts > timestamp '2026-01-02' ORDER BY ts DESC LIMIT 5;
SELECT * FROM upstream_topn_stats ORDER BY ts DESC, user_id ASC LIMIT 1;
DROP TABLE upstream_topn_stats;
