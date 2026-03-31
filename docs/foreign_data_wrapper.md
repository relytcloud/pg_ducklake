# Foreign Data Wrapper

`ducklake_fdw` provides access to DuckLake tables stored in a remote
PostgreSQL catalog or a frozen `.ducklake` snapshot file. Foreign tables
backed by a remote catalog support full DML (INSERT/UPDATE/DELETE);
frozen snapshots are read-only. Queries on foreign tables are routed
through DuckDB -- the FDW handles catalog registration, not scan
execution.

## Connection modes

### Remote PostgreSQL catalog

Connect to a DuckLake catalog hosted in another PostgreSQL database.

**Simple (same host, peer auth):**

```sql
CREATE SERVER remote_catalog
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (dbname 'analytics_db', metadata_schema 'ducklake');
```

**Full connection string (remote host):**

```sql
CREATE SERVER remote_catalog
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (connection_string 'host=db.example.com port=5432 dbname=analytics_db user=reader',
             metadata_schema 'ducklake');
```

The `connection_string` value is passed directly to DuckDB's PostgreSQL
scanner. It accepts any libpq connection string format.

### Frozen snapshot

Connect to a static `.ducklake` file, either local or hosted over HTTP/HTTPS.

```sql
CREATE SERVER frozen_snapshot
    FOREIGN DATA WRAPPER ducklake_fdw
    OPTIONS (frozen_url 'https://bucket.s3.amazonaws.com/data.ducklake');
```

### Server option reference

| Option | Context | Description |
|--------|---------|-------------|
| `dbname` | server | Database name (simple local connection) |
| `connection_string` | server | Full libpq connection string (remote connections) |
| `metadata_schema` | server | Schema holding DuckLake metadata (default: `ducklake`) |
| `frozen_url` | server | Path or URL to a frozen `.ducklake` file |
| `updatable` | server, table | Allow INSERT/UPDATE/DELETE (default: `true`; `false` for frozen) |
| `schema_name` | table | Remote DuckLake schema (default: `public`) |
| `table_name` | table | Remote table name (default: foreign table name) |

**Mutual exclusivity:** `dbname`, `connection_string`, and `frozen_url`
are mutually exclusive. `metadata_schema` can be used with `dbname` or
`connection_string` but not with `frozen_url`. `frozen_url` and
`updatable 'true'` are mutually exclusive.

## Creating foreign tables

### Single table

Create a foreign table for a single remote DuckLake table. Omit the
column list to auto-infer columns from the remote schema:

```sql
CREATE FOREIGN TABLE local_orders ()
    SERVER remote_catalog
    OPTIONS (schema_name 'public', table_name 'orders');
```

You can also specify columns explicitly:

```sql
CREATE FOREIGN TABLE local_orders (id int, amount float8)
    SERVER remote_catalog
    OPTIONS (table_name 'orders');
```

### Bulk import

Use `IMPORT FOREIGN SCHEMA` to import all tables from a remote DuckLake
schema at once:

```sql
CREATE SCHEMA analytics;

IMPORT FOREIGN SCHEMA public
    FROM SERVER remote_catalog
    INTO analytics;

-- All remote tables are now available as foreign tables:
SELECT * FROM analytics.orders;
SELECT * FROM analytics.customers;
```

Filter which tables to import with `LIMIT TO` or `EXCEPT`:

```sql
-- Import only specific tables
IMPORT FOREIGN SCHEMA public LIMIT TO (orders, customers)
    FROM SERVER remote_catalog
    INTO analytics;

-- Import all except specific tables
IMPORT FOREIGN SCHEMA public EXCEPT (internal_logs)
    FROM SERVER remote_catalog
    INTO analytics;
```

`IMPORT FOREIGN SCHEMA` works with both remote PostgreSQL catalogs and
frozen snapshots.

## Querying and modifying foreign tables

Foreign tables backed by a remote PostgreSQL catalog support full SQL
including SELECT, INSERT, UPDATE, and DELETE -- queries and writes are
routed to DuckDB for execution:

```sql
SELECT * FROM analytics.orders WHERE amount > 100 ORDER BY created_at;

INSERT INTO analytics.orders (id, amount) VALUES (42, 199.99);

UPDATE analytics.orders SET amount = 249.99 WHERE id = 42;

DELETE FROM analytics.orders WHERE id = 42;
```

Foreign tables backed by a **frozen snapshot** (`frozen_url`) are
read-only. INSERT, UPDATE, and DELETE raise an error.

## Handling remote schema changes

If the remote DuckLake schema changes (columns added, removed, or
renamed), the local foreign table definitions become stale. Follow the
standard PostgreSQL pattern -- drop and re-import:

```sql
DROP SCHEMA analytics CASCADE;
CREATE SCHEMA analytics;
IMPORT FOREIGN SCHEMA public FROM SERVER remote_catalog INTO analytics;
```
