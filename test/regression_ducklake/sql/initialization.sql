-- Table AM 'ducklake' is created once the extension is installed

SELECT amname FROM pg_am WHERE amname = 'ducklake';

CREATE TABLE t (a int) USING ducklake;

SELECT relname
FROM pg_class
WHERE relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake');

DROP TABLE t;

DROP EXTENSION pg_duckdb;

SELECT oid FROM pg_namespace WHERE nspname = 'ducklake';

CREATE EXTENSION pg_duckdb;
