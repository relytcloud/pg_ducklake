-- Upstream: test/sql/data_inlining/data_inlining_constraints.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_constraint (i integer, j integer NOT NULL) USING ducklake;
INSERT INTO upstream_inline_constraint VALUES (42, NULL);
SELECT count(*) FROM upstream_inline_constraint;
DROP TABLE upstream_inline_constraint;
CALL ducklake.set_option('data_inlining_row_limit', 0);
