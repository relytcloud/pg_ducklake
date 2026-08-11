-- Upstream: test/sql/data_inlining/inlined_data_multi_table_fresh_connection.test
CALL ducklake.set_option('data_inlining_row_limit', 1000);
CREATE TABLE upstream_inline_fresh_b (j integer) USING ducklake;
INSERT INTO upstream_inline_fresh_b VALUES (10), (20), (30);
CREATE TABLE upstream_inline_fresh_a (i integer) USING ducklake;
INSERT INTO upstream_inline_fresh_a VALUES (1), (2), (3);
BEGIN;
ALTER TABLE upstream_inline_fresh_b ADD COLUMN y integer;
ALTER TABLE upstream_inline_fresh_a ADD COLUMN x integer;
COMMIT;
ALTER TABLE upstream_inline_fresh_a ADD COLUMN z1 integer;
ALTER TABLE upstream_inline_fresh_a ADD COLUMN z2 integer;
SELECT sum(i) FROM upstream_inline_fresh_a;
SELECT sum(j) FROM upstream_inline_fresh_b;
CALL ducklake.recycle_ddb();
SELECT sum(i) FROM upstream_inline_fresh_a;
SELECT sum(j) FROM upstream_inline_fresh_b;
DROP TABLE upstream_inline_fresh_a, upstream_inline_fresh_b;
CALL ducklake.set_option('data_inlining_row_limit', 0);
