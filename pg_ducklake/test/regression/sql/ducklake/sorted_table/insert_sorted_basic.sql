-- Upstream: test/sql/sorted_table/insert_sorted_basic.test
-- Skip: Physical inserted-file order is not exposed by the PostgreSQL API.
-- INSERT preserves rows for single-column, multi-column, and NULL-aware sort metadata.
CREATE TABLE upstream_insert_sort_basic (a integer, b text) USING ducklake;
CALL ducklake.set_sort('upstream_insert_sort_basic'::regclass, 'a ASC NULLS LAST', 'b DESC NULLS FIRST');
INSERT INTO upstream_insert_sort_basic VALUES (2, 'x'), (1, 'b'), (1, 'a'), (NULL, 'z');
SELECT * FROM ducklake.get_sort('upstream_insert_sort_basic'::regclass);
SELECT a, b FROM upstream_insert_sort_basic ORDER BY a NULLS LAST, b DESC NULLS FIRST;
DROP TABLE upstream_insert_sort_basic;
