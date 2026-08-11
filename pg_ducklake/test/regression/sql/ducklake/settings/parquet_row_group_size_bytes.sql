-- Upstream: test/sql/settings/parquet_row_group_size_bytes.test
-- Skip: PostgreSQL exposes no Parquet row-group metadata API, so the upstream >10 physical row-group invariant is unobservable.

CALL ducklake.set_option('parquet_row_group_size_bytes', '10KB');
SELECT option_name, value, scope
FROM ducklake.options()
WHERE option_name = 'parquet_row_group_size_bytes';
-- Leave a valid, non-disruptive value instead of passing unsupported unitless bytes.
CALL ducklake.set_option('parquet_row_group_size_bytes', '128MB');
