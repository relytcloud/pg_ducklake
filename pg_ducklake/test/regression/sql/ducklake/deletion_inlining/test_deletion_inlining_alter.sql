-- Upstream: test/sql/deletion_inlining/test_deletion_inlining_alter.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_delete_alter USING ducklake AS SELECT g AS i, g * 2 AS j FROM generate_series(0, 49) g;
DELETE FROM upstream_delete_alter WHERE i < 5;
ALTER TABLE upstream_delete_alter ADD COLUMN k integer;
INSERT INTO upstream_delete_alter VALUES (100,200,300);
DELETE FROM upstream_delete_alter WHERE i = 100;
SELECT count(*), sum(j), sum(k) FROM upstream_delete_alter;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_alter'::regclass);
ALTER TABLE upstream_delete_alter DROP COLUMN k;
DELETE FROM upstream_delete_alter WHERE i >= 45;
ALTER TABLE upstream_delete_alter ALTER COLUMN j TYPE bigint;
INSERT INTO upstream_delete_alter VALUES (1000,2000000000000);
DELETE FROM upstream_delete_alter WHERE i = 1000;
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_alter'::regclass);
SELECT count(*), sum(j) FROM upstream_delete_alter;
DROP TABLE upstream_delete_alter;
CALL ducklake.set_option('data_inlining_row_limit', 0);
