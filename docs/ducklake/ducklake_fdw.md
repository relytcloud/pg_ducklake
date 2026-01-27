# DuckLake FDW: User Guide

The DuckLake Foreign Data Wrapper (FDW) allows PostgreSQL to query DuckLake tables directly. These tables are managed by DuckDB but accessible seamlessly through standard SQL queries in PostgreSQL.

**Note**: The DuckLake FDW only supports DuckLake instances that use the same PostgreSQL instance as their catalog service.

## Creating a Foreign Server

Create a foreign server using the `ducklake_fdw` wrapper:

```sql
CREATE SERVER ducklake_server
FOREIGN DATA WRAPPER ducklake_fdw
OPTIONS (
    dbname 'my_database',      -- Optional: Target database name (defaults to current DB)
    metadata_schema 'ducklake' -- Optional: DuckLake metadata schema (defaults to 'ducklake')
);
```

### Server Options

| Option | Required | Default | Description |
| :--- | :--- | :--- | :--- |
| `dbname` | No | Current DB | The PostgreSQL database name to use for the DuckDB attachment. |
| `metadata_schema` | No | `ducklake` | The schema where DuckLake metadata tables reside. |

## Creating Foreign Tables

When creating a foreign table, leave the column list empty. The column definitions are automatically inferred from the DuckLake table schema:

```sql
CREATE FOREIGN TABLE my_ducklake_table ()
SERVER ducklake_server
OPTIONS (
    schema_name 'public',     -- Required: Source schema in DuckLake
    table_name 'users'        -- Required: Source table in DuckLake
);
```

### Table Options

| Option | Required | Description |
| :--- | :--- | :--- |
| `schema_name` | **Yes** | The schema name of the table in DuckLake. |
| `table_name` | **Yes** | The table name in DuckLake. |

## Usage

Once created, you can query the foreign table like any regular PostgreSQL table:

```sql
SELECT * FROM my_ducklake_table WHERE id > 100;
```

The FDW supports SELECT queries, joins, and aggregates, with queries pushed down to DuckDB when possible. Foreign tables are read-only by design—INSERT, UPDATE, and DELETE operations are not supported, and attempting to modify a foreign table will result in an error: `ERROR: cannot update foreign table "my_ducklake_table"`.
