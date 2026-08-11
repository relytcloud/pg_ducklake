-- Upstream: test/sql/stats/count_star_optimization_basic.test
-- Metadata row counts remain correct across inserts, deletes, and transaction rollback.
CREATE TABLE upstream_count_basic (i integer) USING ducklake;
INSERT INTO upstream_count_basic SELECT i FROM generate_series(0, 999) AS g(i);
SELECT count(*) FROM upstream_count_basic;
INSERT INTO upstream_count_basic SELECT i FROM generate_series(1000, 1499) AS g(i);
DELETE FROM upstream_count_basic WHERE i < 100 OR i >= 1400;
SELECT count(*) FROM upstream_count_basic;
BEGIN;
INSERT INTO upstream_count_basic SELECT i FROM generate_series(1500, 1699) AS g(i);
SELECT count(*) FROM upstream_count_basic;
ROLLBACK;
SELECT count(*) FROM upstream_count_basic;
BEGIN;
DELETE FROM upstream_count_basic WHERE i >= 1200;
SELECT count(*) FROM upstream_count_basic;
ROLLBACK;
SELECT count(*) FROM upstream_count_basic;
SELECT count(*) FROM upstream_count_basic WHERE i >= 100 AND i < 300;
BEGIN;
TRUNCATE upstream_count_basic;
SELECT count(*) FROM upstream_count_basic;
ROLLBACK;
SELECT count(*) FROM upstream_count_basic;
TRUNCATE upstream_count_basic;
INSERT INTO upstream_count_basic VALUES (1), (2), (3), (4), (5);
SELECT count(*) FROM upstream_count_basic;
DROP TABLE upstream_count_basic;
