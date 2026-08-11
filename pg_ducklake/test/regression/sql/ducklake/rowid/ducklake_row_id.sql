-- Upstream: test/sql/rowid/ducklake_row_id.test

CREATE TABLE upstream_row_id (i integer) USING ducklake;
SELECT ducklake.rowid() FROM upstream_row_id;
INSERT INTO upstream_row_id SELECT g FROM generate_series(0, 2) AS g;
INSERT INTO upstream_row_id SELECT g FROM generate_series(5, 6) AS g;
INSERT INTO upstream_row_id SELECT g FROM generate_series(10, 14) AS g;
SELECT ducklake.rowid(), i FROM upstream_row_id ORDER BY ducklake.rowid();
DELETE FROM upstream_row_id WHERE i % 2 = 1;
SELECT ducklake.rowid(), i FROM upstream_row_id ORDER BY ducklake.rowid();
UPDATE upstream_row_id SET i = i + 1000 WHERE i < 3 OR i > 10;
SELECT ducklake.rowid(), i FROM upstream_row_id ORDER BY ducklake.rowid();
BEGIN;
UPDATE upstream_row_id SET i = i + 2000;
SELECT ducklake.rowid(), i FROM upstream_row_id ORDER BY ducklake.rowid();
ROLLBACK;
SELECT ducklake.rowid(), i FROM upstream_row_id ORDER BY ducklake.rowid();
DROP TABLE upstream_row_id;
