-- Example from README: Create a PostgreSQL row-store (heap) table.
CREATE TABLE row_store_table AS
SELECT i AS id, 'hello pg_ducklake' AS msg
FROM generate_series(1, 10000) AS i;

-- Create a DuckLake column-store table via ETL.
CREATE TABLE col_store_table USING ducklake AS
SELECT *
FROM row_store_table;

-- Run analytics against the converted table.
SELECT id, msg FROM col_store_table ORDER BY id LIMIT 10;

-- Verify access methods via pg_catalog
SELECT c.relname, a.amname
FROM pg_class c
JOIN pg_am a ON c.relam = a.oid
WHERE c.relname IN ('row_store_table', 'col_store_table')
ORDER BY c.relname;

DROP TABLE row_store_table;
DROP TABLE col_store_table;

-- Example from README: CREATE AS with read_csv - using titanic dataset
CREATE TABLE titanic USING ducklake AS
SELECT * FROM read_csv('https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv');

SELECT *
FROM titanic
ORDER BY "PassengerId"
LIMIT 10;

-- Verify access method for titanic table
SELECT c.relname, a.amname
FROM pg_class c
JOIN pg_am a ON c.relam = a.oid
WHERE c.relname = 'titanic';

DROP TABLE titanic;
