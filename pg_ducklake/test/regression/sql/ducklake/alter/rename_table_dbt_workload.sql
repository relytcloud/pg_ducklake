-- Upstream: test/sql/alter/rename_table_dbt_workload.test
CREATE TABLE upstream_dbt_table USING ducklake AS SELECT g AS i FROM generate_series(1, 42) AS g;
BEGIN;
CREATE TABLE upstream_dbt_table_tmp USING ducklake AS SELECT g AS i FROM generate_series(1, 84) AS g;
ALTER TABLE upstream_dbt_table RENAME TO upstream_dbt_table_backup;
ALTER TABLE upstream_dbt_table_tmp RENAME TO upstream_dbt_table;
SELECT count(*) FROM upstream_dbt_table;
SELECT count(*) FROM upstream_dbt_table_backup;
SELECT to_regclass('upstream_dbt_table_tmp') IS NULL AS temporary_name_gone;
SELECT relname FROM pg_class
WHERE relnamespace = 'public'::regnamespace
  AND relname IN ('upstream_dbt_table', 'upstream_dbt_table_backup', 'upstream_dbt_table_tmp')
ORDER BY relname;
COMMIT;
SELECT count(*) FROM upstream_dbt_table;
SELECT count(*) FROM upstream_dbt_table_backup;
SELECT to_regclass('upstream_dbt_table_tmp') IS NULL AS temporary_name_gone;
SELECT relname FROM pg_class
WHERE relnamespace = 'public'::regnamespace
  AND relname IN ('upstream_dbt_table', 'upstream_dbt_table_backup', 'upstream_dbt_table_tmp')
ORDER BY relname;
DROP TABLE upstream_dbt_table, upstream_dbt_table_backup;
