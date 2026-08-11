-- Upstream: test/sql/settings/per_table_settings.test
-- Skip: file-level Parquet codec inspection and detach/reattach persistence are not exposed by the PostgreSQL API.
-- Exercise PostgreSQL's regclass/regnamespace scope overloads.

CREATE SCHEMA upstream_setting_schema;
CREATE TABLE upstream_setting_t1 (str text) USING ducklake;
CREATE TABLE upstream_setting_t2 (str text) USING ducklake;
CREATE TABLE upstream_setting_schema.t1 (str text) USING ducklake;
CREATE TABLE upstream_setting_schema.t2 (str text) USING ducklake;
CALL ducklake.set_option('parquet_compression', 'uncompressed');
CALL ducklake.set_option('parquet_compression', 'zstd', 'upstream_setting_t1'::regclass);
CALL ducklake.set_option('parquet_compression', 'lz4', 'upstream_setting_schema'::regnamespace);
CALL ducklake.set_option('parquet_compression', 'gzip', 'upstream_setting_schema.t1'::regclass);
SELECT option_name, value, scope, scope_entry
FROM ducklake.options()
WHERE option_name = 'parquet_compression'
  AND (scope = 'GLOBAL' OR scope_entry LIKE '%upstream_setting%')
ORDER BY scope, scope_entry NULLS FIRST;
INSERT INTO upstream_setting_t1 SELECT 'hello world ' || g FROM generate_series(1,100) AS g;
INSERT INTO upstream_setting_t2 SELECT 'hello world ' || g FROM generate_series(1,100) AS g;
INSERT INTO upstream_setting_schema.t1 SELECT 'hello world ' || g FROM generate_series(1,100) AS g;
INSERT INTO upstream_setting_schema.t2 SELECT 'hello world ' || g FROM generate_series(1,100) AS g;
SELECT count(*) FROM upstream_setting_t1;
SELECT count(*) FROM upstream_setting_t2;
SELECT count(*) FROM upstream_setting_schema.t1;
SELECT count(*) FROM upstream_setting_schema.t2;
DROP SCHEMA upstream_setting_schema CASCADE;
DROP TABLE upstream_setting_t1, upstream_setting_t2;
CALL ducklake.set_option('parquet_compression', 'snappy');
