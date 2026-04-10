-- Test DECIMAL/NUMERIC precision is preserved through DuckDB (#171)

-- Wide precision: DECIMAL(31,3) -- exceeds DuckDB's default DECIMAL(18,3)
CREATE TABLE t_decimal_wide (usd decimal(31, 3)) USING ducklake;

-- Insert a large value that overflows DECIMAL(18,3) but fits DECIMAL(31,3)
INSERT INTO t_decimal_wide VALUES (1.7820000000000002e+16);

-- Insert normal values
INSERT INTO t_decimal_wide VALUES (12345.678);
INSERT INTO t_decimal_wide VALUES (0.001);
INSERT INTO t_decimal_wide VALUES (-99999999999999.999);

SELECT * FROM t_decimal_wide ORDER BY usd;

-- Max precision: DECIMAL(38,10)
CREATE TABLE t_decimal_max (val decimal(38, 10)) USING ducklake;
INSERT INTO t_decimal_max VALUES (12345678901234567890.1234567890);
INSERT INTO t_decimal_max VALUES (-12345678901234567890.1234567890);
SELECT * FROM t_decimal_max ORDER BY val;

-- Verify PG column types are preserved
\d+ t_decimal_wide
\d+ t_decimal_max

-- Verify DuckDB-side column types in ducklake metadata
SELECT c.column_name, c.column_type
FROM ducklake.ducklake_column c
JOIN ducklake.ducklake_table t ON c.table_id = t.table_id
WHERE t.table_name = 't_decimal_wide' AND t.end_snapshot IS NULL
  AND c.end_snapshot IS NULL
ORDER BY c.column_order;

SELECT c.column_name, c.column_type
FROM ducklake.ducklake_column c
JOIN ducklake.ducklake_table t ON c.table_id = t.table_id
WHERE t.table_name = 't_decimal_max' AND t.end_snapshot IS NULL
  AND c.end_snapshot IS NULL
ORDER BY c.column_order;

-- Cleanup
DROP TABLE t_decimal_wide;
DROP TABLE t_decimal_max;
