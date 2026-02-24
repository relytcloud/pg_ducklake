-- Table AM 'ducklake' is created once the extension is installed

SELECT extname, extowner, extnamespace, extrelocatable, extversion, extconfig, extcondition FROM pg_extension;

SELECT amname FROM pg_am WHERE amname = 'ducklake';

CREATE TABLE t (a int) USING ducklake;

SELECT relname
FROM pg_class
WHERE relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake');

DROP TABLE t;

DROP EXTENSION pg_ducklake;

SELECT oid FROM pg_namespace WHERE nspname = 'ducklake';

CREATE EXTENSION pg_ducklake;
