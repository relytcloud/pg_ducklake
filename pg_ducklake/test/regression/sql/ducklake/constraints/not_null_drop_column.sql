-- Upstream: test/sql/constraints/not_null_drop_column.test
CREATE TABLE upstream_not_null_drop (i integer NOT NULL, j integer, k integer NOT NULL) USING ducklake;
INSERT INTO upstream_not_null_drop VALUES (42, NULL, 3);
INSERT INTO upstream_not_null_drop VALUES (NULL, 84, 3);
ALTER TABLE upstream_not_null_drop DROP COLUMN j;
INSERT INTO upstream_not_null_drop VALUES (42, NULL);
ALTER TABLE upstream_not_null_drop DROP COLUMN k;
INSERT INTO upstream_not_null_drop VALUES (NULL);
SELECT * FROM upstream_not_null_drop ORDER BY i;
DROP TABLE upstream_not_null_drop;
