SELECT ducklake.create_metadata();

CREATE TABLE row_store_table AS
SELECT i AS id, 'hello pg_ducklake' AS msg
FROM generate_series(1, 10000) AS i;

CREATE TABLE col_store_table USING ducklake AS
SELECT *
FROM row_store_table;

SELECT * FROM col_store_table ORDER BY id DESC LIMIT 3;
