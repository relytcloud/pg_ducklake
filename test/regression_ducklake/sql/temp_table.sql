SELECT ducklake.create_metadata();

CREATE TEMP TABLE tt (a int, b int) using ducklake;

SELECT ducklake.drop_metadata();
