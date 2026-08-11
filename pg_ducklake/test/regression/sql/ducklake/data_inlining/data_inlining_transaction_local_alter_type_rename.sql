-- Upstream: test/sql/data_inlining/data_inlining_transaction_local_alter_type_rename.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_tx_rename (i integer, j text) USING ducklake;
BEGIN;
INSERT INTO upstream_inline_tx_rename VALUES (1, 'hello'), (2, 'world');
ALTER TABLE upstream_inline_tx_rename RENAME COLUMN j TO name;
SELECT * FROM upstream_inline_tx_rename ORDER BY i;
COMMIT;
SELECT i, name FROM upstream_inline_tx_rename ORDER BY i;
CREATE TABLE upstream_inline_tx_type (i integer, v integer) USING ducklake;
BEGIN;
INSERT INTO upstream_inline_tx_type VALUES (1, 100), (2, 200);
ALTER TABLE upstream_inline_tx_type ALTER COLUMN v TYPE bigint;
SELECT * FROM upstream_inline_tx_type ORDER BY i;
COMMIT;
SELECT * FROM upstream_inline_tx_type ORDER BY i;
SELECT format_type(atttypid, atttypmod)
FROM pg_attribute WHERE attrelid = 'upstream_inline_tx_type'::regclass AND attname = 'v';
CREATE TABLE upstream_inline_tx_double (i integer, v real) USING ducklake;
BEGIN;
INSERT INTO upstream_inline_tx_double VALUES (1, 1.5);
ALTER TABLE upstream_inline_tx_double ALTER COLUMN v TYPE double precision;
SELECT i, v FROM upstream_inline_tx_double ORDER BY i;
INSERT INTO upstream_inline_tx_double VALUES (2, 2.5);
SELECT i, v FROM upstream_inline_tx_double ORDER BY i;
COMMIT;
SELECT i, v FROM upstream_inline_tx_double ORDER BY i;
SELECT format_type(atttypid, atttypmod)
FROM pg_attribute WHERE attrelid = 'upstream_inline_tx_double'::regclass AND attname = 'v';
DROP TABLE upstream_inline_tx_rename, upstream_inline_tx_type, upstream_inline_tx_double;
CALL ducklake.set_option('data_inlining_row_limit', 0);
