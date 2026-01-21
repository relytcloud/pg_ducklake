# Secrets Management for DuckLake

DuckLake tables in `pg_ducklake` can store data in cloud storage (AWS S3, Azure Blob Storage) or local filesystems. To access cloud storage, you need to configure secrets that provide the necessary credentials.

Secrets management in `pg_ducklake` aligns with upstream `pg_duckdb`. For comprehensive documentation on all secret types and advanced configurations, see the [secrets documentation](../secrets.md).

## Quick Start

### AWS S3 / Compatible Storage

The most common use case is storing DuckLake tables in S3-compatible storage:

```sql
-- Create an S3 secret with scope matching your bucket/prefix
SELECT duckdb.create_simple_secret(
    type   := 'S3',
    key_id := 'your_access_key_id',
    secret := 'your_secret_access_key',
    scope  := 's3://my-bucket/prefix'
);

-- Configure DuckLake to use S3 for table storage
SET ducklake.default_table_path = 's3://my-bucket/prefix/ducklake';

-- Create a DuckLake table (data will be stored in S3)
CREATE TABLE ducklake_tbl (
    id INT,
    name TEXT,
    age INT
) USING ducklake;

-- Insert data (automatically written to S3)
INSERT INTO ducklake_tbl VALUES (1, 'Alice', 25), (2, 'Bob', 30);

-- Query the table
SELECT * FROM ducklake_tbl;

-- Check table metadata and storage location
SELECT table_name, path, path_is_relative 
FROM ducklake.ducklake_table 
WHERE table_name = 'ducklake_tbl';
```

### Azure Blob Storage

```sql
SELECT duckdb.create_azure_secret(
    '<connection_string>',
    scope := 'az://my-container/prefix'
);

SET ducklake.default_table_path = 'az://my-container/prefix/ducklake';
```

## Understanding Scope

The `scope` parameter in secrets defines the URL prefix where the secret applies. When creating DuckLake tables:

- **With scope**: If your secret has `scope := 's3://my-bucket/prefix'`, any DuckLake table path starting with `s3://my-bucket/prefix/` will use this secret automatically.
- **Without scope**: If you omit the scope, the secret becomes the default secret and will be used for any S3 operations that don't match a more specific scoped secret.

**Best Practice**: Set the scope to match your `ducklake.default_table_path` prefix for clarity and security.
