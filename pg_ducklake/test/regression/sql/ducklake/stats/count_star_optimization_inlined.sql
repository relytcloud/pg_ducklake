-- Upstream: test/sql/stats/count_star_optimization_inlined.test
-- Counts combine inlined data across schema evolution, rollback, and truncate.
CALL ducklake.set_option('data_inlining_row_limit', 100);
CREATE TABLE upstream_count_inline USING ducklake AS
SELECT i FROM generate_series(0, 49) AS g(i);
SELECT count(*) FROM upstream_count_inline;

INSERT INTO upstream_count_inline SELECT i FROM generate_series(50, 79) AS g(i);
SELECT count(*) FROM upstream_count_inline;
ALTER TABLE upstream_count_inline ADD COLUMN j integer DEFAULT 0;
SELECT count(*) FROM upstream_count_inline;
INSERT INTO upstream_count_inline(i, j)
SELECT i, i * 10 FROM generate_series(80, 89) AS g(i);
SELECT count(*) FROM upstream_count_inline;
ALTER TABLE upstream_count_inline ADD COLUMN k text DEFAULT 'x';
SELECT count(*) FROM upstream_count_inline;
INSERT INTO upstream_count_inline(i, j, k)
SELECT i, i * 10, 'val' || i::text FROM generate_series(90, 99) AS g(i);
SELECT count(*) FROM upstream_count_inline;

DELETE FROM upstream_count_inline WHERE i % 10 = 0;
SELECT count(*) FROM upstream_count_inline;

BEGIN;
INSERT INTO upstream_count_inline(i) SELECT i FROM generate_series(100, 119) AS g(i);
SELECT count(*) FROM upstream_count_inline;
ROLLBACK;
SELECT count(*) FROM upstream_count_inline;

BEGIN;
DELETE FROM upstream_count_inline WHERE i % 10 = 1;
SELECT count(*) FROM upstream_count_inline;
ROLLBACK;
SELECT count(*) FROM upstream_count_inline;

TRUNCATE upstream_count_inline;
SELECT count(*) FROM upstream_count_inline;
INSERT INTO upstream_count_inline(i) SELECT i FROM generate_series(0, 49) AS g(i);
SELECT count(*) FROM upstream_count_inline;
DROP TABLE upstream_count_inline;
