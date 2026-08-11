-- Upstream: test/sql/insert/insert_column_list.test
-- Column lists, omitted columns, and DEFAULT must retain PostgreSQL semantics.

CREATE TABLE upstream_insert_columns (i integer, j text) USING ducklake;

INSERT INTO upstream_insert_columns (j, i) VALUES ('hello', 84);
INSERT INTO upstream_insert_columns (j) VALUES ('world');
INSERT INTO upstream_insert_columns (i) VALUES (100);
INSERT INTO upstream_insert_columns DEFAULT VALUES;
INSERT INTO upstream_insert_columns VALUES (1000, DEFAULT), (DEFAULT, 'xxx');

SELECT * FROM upstream_insert_columns ORDER BY i NULLS LAST, j NULLS FIRST;

DROP TABLE upstream_insert_columns;
