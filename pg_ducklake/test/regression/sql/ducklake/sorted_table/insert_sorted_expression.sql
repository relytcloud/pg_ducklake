-- Upstream: test/sql/sorted_table/insert_sorted_expression.test
-- Skip: Requires implementation: pg_ducklake does not yet support expression sort keys.
-- INSERT accepts expression-based sort keys.
CREATE TABLE upstream_insert_sort_expression (a integer, b integer, name text) USING ducklake;
CALL ducklake.set_sort('upstream_insert_sort_expression'::regclass, 'length(name) ASC', '(a * b) DESC');
INSERT INTO upstream_insert_sort_expression VALUES (3, 4, 'ab'), (2, 5, 'abc'), (1, 1, 'ab'), (7, 1, 'a');
SELECT * FROM ducklake.get_sort('upstream_insert_sort_expression'::regclass);
SELECT a, b, name FROM upstream_insert_sort_expression ORDER BY length(name), a * b DESC, a;
DROP TABLE upstream_insert_sort_expression;
