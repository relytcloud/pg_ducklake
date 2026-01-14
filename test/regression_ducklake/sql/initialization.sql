-- Table AM 'ducklake' is created once the extension is installed, but tables cannot be created before initialization

SELECT amname FROM pg_am WHERE amname = 'ducklake';

CREATE TABLE t (a int) USING ducklake;

SELECT ducklake.create_metadata();

CREATE TABLE t (a int) USING ducklake;

SELECT relname
FROM pg_class
WHERE relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake');

SELECT ducklake.drop_metadata();

-- After drop_metadata(), all tables using ducklake AM are dropped, but the table AM itself is not dropped
SELECT amname FROM pg_am WHERE amname = 'ducklake';
SELECT relname
FROM pg_class
WHERE relam = (SELECT oid FROM pg_am WHERE amname = 'ducklake');
