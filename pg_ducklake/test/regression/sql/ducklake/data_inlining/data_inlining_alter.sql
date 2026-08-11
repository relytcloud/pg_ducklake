-- Upstream: test/sql/data_inlining/data_inlining_alter.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_alter USING ducklake AS SELECT 1::integer i, 2::integer j;
ALTER TABLE upstream_inline_alter ADD COLUMN k integer;
INSERT INTO upstream_inline_alter VALUES (10, 20, 30);
SELECT * FROM upstream_inline_alter ORDER BY j;
ALTER TABLE upstream_inline_alter DROP COLUMN i;
ALTER TABLE upstream_inline_alter ALTER COLUMN j TYPE bigint;
INSERT INTO upstream_inline_alter VALUES (1000000000000, 0);
SELECT * FROM upstream_inline_alter ORDER BY j;
DROP TABLE upstream_inline_alter;
CALL ducklake.set_option('data_inlining_row_limit', 0);
