-- Test CREATE TABLE ... USING ducklake WITH (ducklake.*) options.

-- Disable data inlining so inserts write parquet files immediately.
CALL ducklake.set_option('data_inlining_row_limit', 0);

-- 1. ducklake.table_path basic: data files land at the per-table path.
CREATE TABLE cwo_t1 (id int, val text) USING ducklake
  WITH (ducklake.table_path = '/tmp/cwo_t1/');
INSERT INTO cwo_t1 VALUES (1, 'a'), (2, 'b');
SELECT * FROM cwo_t1 ORDER BY id;
SELECT * FROM duckdb.query($$
  SELECT bool_and(starts_with(data_file, '/tmp/cwo_t1/')) AS path_ok
  FROM ducklake_list_files('pgducklake', 'cwo_t1', schema => 'public')
$$);

-- 2. No leak: a table created right after without WITH must NOT land
--    under /tmp/cwo_t1/.
CREATE TABLE cwo_t2 (id int) USING ducklake;
INSERT INTO cwo_t2 VALUES (1);
SELECT * FROM duckdb.query($$
  SELECT bool_and(NOT starts_with(data_file, '/tmp/cwo_t1/')) AS no_leak
  FROM ducklake_list_files('pgducklake', 'cwo_t2', schema => 'public')
$$);

-- 3. CTAS path: WITH on the new table, files land at the requested path.
CREATE TABLE cwo_ctas USING ducklake
  WITH (ducklake.table_path = '/tmp/cwo_ctas/')
  AS SELECT i AS id, ('row_' || i) AS val FROM generate_series(1, 3) t(i);
SELECT count(*) FROM cwo_ctas;
SELECT * FROM duckdb.query($$
  SELECT bool_and(starts_with(data_file, '/tmp/cwo_ctas/')) AS path_ok
  FROM ducklake_list_files('pgducklake', 'cwo_ctas', schema => 'public')
$$);

-- 4. Error: unknown ducklake.* option.
CREATE TABLE cwo_bad_opt (id int) USING ducklake WITH (ducklake.bogus = 'x');

-- 5. Error: empty table_path.
CREATE TABLE cwo_empty (id int) USING ducklake WITH (ducklake.table_path = '');

-- 6. Error: duplicate option.
CREATE TABLE cwo_dup (id int) USING ducklake
  WITH (ducklake.table_path = '/tmp/a/', ducklake.table_path = '/tmp/b/');

-- 7. PG core rejects the ducklake.* namespace on non-ducklake tables.
CREATE TABLE cwo_not_ducklake (id int) WITH (ducklake.table_path = '/tmp/x/');

-- Cleanup
DROP TABLE cwo_t1, cwo_t2, cwo_ctas;
