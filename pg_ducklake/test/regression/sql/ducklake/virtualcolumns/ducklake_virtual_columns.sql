-- Upstream: test/sql/virtualcolumns/ducklake_virtual_columns.test

CREATE TABLE upstream_virtual_columns (i integer) USING ducklake;
INSERT INTO upstream_virtual_columns VALUES (1), (2), (3);
SELECT * FROM ducklake.flush_inlined_data('upstream_virtual_columns'::regclass);
SELECT ducklake.file_row_number(), i FROM upstream_virtual_columns ORDER BY i;
SELECT i FROM upstream_virtual_columns WHERE ducklake.file_row_number() = 1;
SELECT count(DISTINCT ducklake.filename()) FROM upstream_virtual_columns;
SELECT count(DISTINCT ducklake.file_index()) FROM upstream_virtual_columns;
DROP TABLE upstream_virtual_columns;
