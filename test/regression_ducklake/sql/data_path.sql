-- Create ducklake relative to PG data directory.
SELECT ducklake.create_metadata();

SELECT
    starts_with(data_path, data_directory) as is_prefix,
    replace(data_path, data_directory, '') as relative_data_path
FROM
(SELECT value AS data_path FROM ducklake.ducklake_metadata WHERE key = 'data_path') metadata,
(SELECT setting AS data_directory FROM pg_settings WHERE name = 'data_directory') settings;

SELECT path, path_is_relative FROM ducklake.ducklake_schema WHERE schema_id = 0;

SELECT ducklake.drop_metadata(true);

-- Create ducklake at S3.
SELECT ducklake.create_metadata('s3://ducklake-bucket/');

SELECT
    starts_with(data_path, data_directory) as is_prefix,
    replace(data_path, data_directory, '') as relative_data_path
FROM
(SELECT value AS data_path FROM ducklake.ducklake_metadata WHERE key = 'data_path') metadata,
(SELECT setting AS data_directory FROM pg_settings WHERE name = 'data_directory') settings;

SELECT path, path_is_relative FROM ducklake.ducklake_schema WHERE schema_id = 0;

SELECT ducklake.drop_metadata(true);

-- Create ducklake at local directory.
SELECT ducklake.create_metadata('/tmp/ducklake/');

SELECT
    starts_with(data_path, data_directory) as is_prefix,
    replace(data_path, data_directory, '') as relative_data_path
FROM
(SELECT value AS data_path FROM ducklake.ducklake_metadata WHERE key = 'data_path') metadata,
(SELECT setting AS data_directory FROM pg_settings WHERE name = 'data_directory') settings;

SELECT path, path_is_relative FROM ducklake.ducklake_schema WHERE schema_id = 0;

SELECT ducklake.drop_metadata(true);
