-- Upstream: test/sql/default/all_types_column_default_stats.test
CALL ducklake.set_option('data_inlining_row_limit', 100);
SET TIME ZONE 'UTC';
SET DATESTYLE TO 'ISO, YMD';
CREATE TABLE upstream_default_stats_types (
  c_bool boolean, c_smallint smallint, c_int integer, c_bigint bigint,
  c_real real, c_double double precision, c_numeric numeric(12,4),
  c_date date, c_time time, c_timetz timetz, c_ts timestamp,
  c_tstz timestamptz, c_varchar text, c_bytea bytea, c_uuid uuid
) USING ducklake;
INSERT INTO upstream_default_stats_types VALUES
(false, 1, 10, 100, 1.5, 1.5, 1.1000, DATE '2020-01-01', TIME '01:00',
 TIMETZ '01:00+00', TIMESTAMP '2020-01-01 01:00', TIMESTAMPTZ '2020-01-01 01:00+00',
 'a', '\x61', '11111111-1111-1111-1111-111111111111'),
(true, 3, 30, 300, 3.5, 3.5, 3.3000, DATE '2022-03-03', TIME '03:00',
 TIMETZ '03:00+00', TIMESTAMP '2022-03-03 03:00', TIMESTAMPTZ '2022-03-03 03:00+00',
 'c', '\x63', '33333333-3333-3333-3333-333333333333');
BEGIN;
ALTER TABLE upstream_default_stats_types ADD COLUMN n_bool boolean DEFAULT true;
ALTER TABLE upstream_default_stats_types ADD COLUMN n_smallint smallint DEFAULT 700;
ALTER TABLE upstream_default_stats_types ADD COLUMN n_int integer DEFAULT -5;
ALTER TABLE upstream_default_stats_types ADD COLUMN n_bigint bigint DEFAULT 1234567890123;
ALTER TABLE upstream_default_stats_types ADD COLUMN n_real real DEFAULT 3.14;
ALTER TABLE upstream_default_stats_types ADD COLUMN n_double double precision DEFAULT 2.71828;
ALTER TABLE upstream_default_stats_types ADD COLUMN n_numeric numeric(12,4) DEFAULT 100.5;
ALTER TABLE upstream_default_stats_types ADD COLUMN n_date date DEFAULT '2024-06-15';
ALTER TABLE upstream_default_stats_types ADD COLUMN n_time time DEFAULT '12:34:56';
ALTER TABLE upstream_default_stats_types ADD COLUMN n_timetz timetz DEFAULT '12:34:56+02';
ALTER TABLE upstream_default_stats_types ADD COLUMN n_ts timestamp DEFAULT '2024-06-15 12:34:56';
ALTER TABLE upstream_default_stats_types ADD COLUMN n_tstz timestamptz DEFAULT '2024-06-15 12:34:56+00';
ALTER TABLE upstream_default_stats_types ADD COLUMN n_varchar text DEFAULT 'hello';
ALTER TABLE upstream_default_stats_types ADD COLUMN n_bytea bytea DEFAULT '\x616263';
ALTER TABLE upstream_default_stats_types ADD COLUMN n_uuid uuid DEFAULT '12345678-1234-1234-1234-123456789012';
COMMIT;
SELECT c.column_name, s.contains_null, s.min_value, s.max_value
FROM ducklake.ducklake_table_column_stats s
JOIN ducklake.ducklake_table t USING (table_id)
JOIN ducklake.ducklake_column c USING (table_id, column_id)
WHERE t.table_name = 'upstream_default_stats_types'
  AND t.end_snapshot IS NULL AND c.end_snapshot IS NULL
ORDER BY c.column_order;
DROP TABLE upstream_default_stats_types;
SET TIME ZONE DEFAULT;
SET DATESTYLE TO DEFAULT;
CALL ducklake.set_option('data_inlining_row_limit', 0);
