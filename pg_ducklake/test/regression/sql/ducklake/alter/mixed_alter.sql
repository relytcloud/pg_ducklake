-- Upstream: test/sql/alter/mixed_alter.test
CREATE TABLE upstream_mixed_alter (col1 integer, col2 integer, col3 integer) USING ducklake;
INSERT INTO upstream_mixed_alter VALUES (1, 2, 3);
ALTER TABLE upstream_mixed_alter DROP COLUMN col2;
INSERT INTO upstream_mixed_alter (col1, col3) VALUES (10, 20);
ALTER TABLE upstream_mixed_alter ADD COLUMN col2 text;
INSERT INTO upstream_mixed_alter (col1, col3, col2) VALUES (100, 300, 'hello world');
SELECT col1, col2, col3 FROM upstream_mixed_alter ORDER BY col1;
DROP TABLE upstream_mixed_alter;
