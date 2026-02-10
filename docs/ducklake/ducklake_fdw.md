# DuckLake Foreign Data Wrapper

The DuckLake Foreign Data Wrapper (FDW) provides read-only access to DuckLake tables from PostgreSQL.

The FDW supports two data source types:
- **PostgreSQL-backed DuckLake**: Access DuckLake tables that use the same PostgreSQL instance as their catalog service.
- **Frozen DuckLake**: Access frozen DuckLake snapshots hosted over HTTP.

## Quick Start

```sql
-- 1. Create the foreign server
CREATE SERVER ducklake_server
    FOREIGN DATA WRAPPER ducklake_fdw;

-- 2. Create a foreign table (column list must be empty)
CREATE FOREIGN TABLE my_foreign_table ()
    SERVER ducklake_server
    OPTIONS (schema_name 'public', table_name 'my_ducklake_table');

-- 3. Query the foreign table
SELECT * FROM my_foreign_table WHERE id > 100;
```

## Server Options

```sql
CREATE SERVER ducklake_server
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (
        dbname 'my_database',      -- Optional: defaults to current database
        metadata_schema 'ducklake' -- Optional: defaults to 'ducklake'
    );
```

| Option | Required | Default | Description |
| :--- | :--- | :--- | :--- |
| `dbname` | No | Current DB | The PostgreSQL database containing the DuckLake tables |
| `metadata_schema` | No | `ducklake` | The schema where DuckLake metadata tables reside |
| `frozen_url` | No | - | HTTP URL of a frozen DuckLake file (mutually exclusive with `dbname` and `metadata_schema`) |

`frozen_url` cannot be combined with `dbname` or `metadata_schema`. A server is either PostgreSQL-backed or frozen (HTTP-backed).

User mapping is unnecessary and not allowed. Since the FDW accesses DuckLake tables on the local PostgreSQL instance, it always uses the current session's credentials to preserve PostgreSQL permission checks.

## Foreign Table Options

```sql
CREATE FOREIGN TABLE my_ducklake_table ()
    SERVER ducklake_server
    OPTIONS (
        schema_name 'public',  -- Required: schema name in DuckLake
        table_name 'users'     -- Required: table name in DuckLake
    );
```

| Option | Required | Description |
| :--- | :--- | :--- |
| `schema_name` | Yes | The schema name of the table in DuckLake |
| `table_name` | Yes | The table name in DuckLake |

**Important:** Column definitions are automatically inferred from DuckLake metadata. You must specify an empty column list `()`. Specifying columns manually will result in an error:

```sql
-- This will fail
CREATE FOREIGN TABLE my_table (id INT, name TEXT)
    SERVER ducklake_server
    OPTIONS (schema_name 'public', table_name 'users');
-- ERROR: cannot specify column definitions for DuckLake foreign table
```

## Cross-Database Queries

Query DuckLake tables from other databases on the same PostgreSQL instance:

```sql
-- From database 'analytics', query tables in 'warehouse' database
CREATE SERVER warehouse_server
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (dbname 'warehouse');

CREATE FOREIGN TABLE warehouse_sales ()
    SERVER warehouse_server
    OPTIONS (schema_name 'public', table_name 'sales');

SELECT * FROM warehouse_sales WHERE region = 'West';
```

## Frozen DuckLake

Frozen DuckLake files are read-only snapshots published as a single file over HTTP. Use the `frozen_url` server option to point to one:

```sql
-- 1. Create a server pointing to a frozen DuckLake URL
CREATE SERVER frozen_space
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (frozen_url 'https://example.com/path/to/data.ducklake');

-- 2. Create a foreign table (columns are auto-inferred)
CREATE FOREIGN TABLE astronauts ()
    SERVER frozen_space
    OPTIONS (schema_name 'main', table_name 'astronauts');

-- 3. Query it
SELECT * FROM astronauts ORDER BY astronaut_id LIMIT 5;
```

Frozen DuckLake tables are always read-only. INSERT, UPDATE, and DELETE operations are not supported.

## Schema Changes

If the underlying DuckLake table schema changes (columns added or removed), recreate the foreign table to pick up the new schema:

```sql
DROP FOREIGN TABLE my_foreign_table;
CREATE FOREIGN TABLE my_foreign_table ()
    SERVER ducklake_server
    OPTIONS (schema_name 'public', table_name 'my_ducklake_table');
```

## Troubleshooting

### Table Not Found

```
ERROR: Cannot create foreign table: DuckLake table "public.my_table" in database "mydb" is not accessible.
```

Verify that:
1. The `schema_name` and `table_name` options are correct
2. The `metadata_schema` option points to the correct schema (default: `ducklake`)
3. You have permission to access the table

Check if the table exists:

```sql
\c target_database
SELECT t.table_name, s.schema_name
FROM ducklake.ducklake_table t
JOIN ducklake.ducklake_schema s USING (schema_id)
WHERE s.schema_name = 'public'
  AND t.table_name = 'my_table'
  AND t.end_snapshot IS NULL;
```

### Permission Errors

Ensure your PostgreSQL user has:
- `USAGE` privilege on the foreign server
- Access to the target database specified in the `dbname` option
- Read permissions on the DuckLake metadata schema
