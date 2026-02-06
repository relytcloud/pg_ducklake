# DuckLake Time Travel

Query DuckLake tables as they existed at a specific point in time to view history, compare changes, or recover from mistakes.

## At a Glance

- Query any past timestamp
- Compare historical vs current results
- Set a global timestamp or per-table snapshots
- Join multiple tables at a consistent point in time
- No special query syntax; you just set a timestamp

## Basic Usage

### Setting a Global Timestamp

Set a global timestamp to time-travel all DuckLake tables in the session:

```sql
-- Set the timestamp
SET ducklake.as_of_timestamp = '2025-01-01 12:00:00';

-- Query normally - results reflect that timestamp
SELECT * FROM orders WHERE customer_id = 123;
SELECT COUNT(*) FROM products;

-- Reset to query current data
RESET ducklake.as_of_timestamp;
```

### Per-Table Snapshots

For different timestamps per table:

```sql
-- Set timestamps for specific tables
SELECT ducklake.set_table_snapshot('orders', '2025-01-01 10:00:00');
SELECT ducklake.set_table_snapshot('customers', '2025-01-15 14:30:00');

-- Query normally - each table uses its own timestamp
SELECT * FROM orders;      -- shows data from Jan 1
SELECT * FROM customers;   -- shows data from Jan 15

-- Clear all per-table timestamps
SELECT ducklake.clear_table_snapshots();
```

**Notes:**
- Per-table snapshots override the global `ducklake.as_of_timestamp`.
- Use schema-qualified names (e.g., `'public.orders'`) to avoid affecting same-named tables in other schemas.
- Per-table snapshots are session-scoped and persist until cleared.

## Timestamp Formats

Common timestamp formats:

- Date only: `'2025-01-01'`
- Date and time: `'2025-01-01 12:30:45'`
- With microseconds: `'2025-01-01 12:30:45.123456'`
- ISO 8601: `'2025-01-01T12:30:45'`

**Tip:** Format validation is minimal; DuckDB parses the value at query time.

## Examples

### Compare Historical and Current

```sql
-- Query historical data
SET ducklake.as_of_timestamp = '2025-01-01';
SELECT COUNT(*), SUM(amount) FROM orders;
-- Result: 100 rows, $50,000

-- Query current data
RESET ducklake.as_of_timestamp;
SELECT COUNT(*), SUM(amount) FROM orders;
-- Result: 150 rows, $75,000
```

### Point-in-Time Recovery

```sql
-- Capture current timestamp before making changes
CREATE TEMP TABLE my_snapshot (ts TEXT);
INSERT INTO my_snapshot SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS.US');

-- Make some changes
UPDATE orders SET status = 'cancelled' WHERE order_id = 12345;

-- Query data before the change
DO $$
BEGIN
  PERFORM set_config('ducklake.as_of_timestamp', (SELECT ts FROM my_snapshot), false);
END $$;

SELECT * FROM orders WHERE order_id = 12345;
-- Shows: status = 'pending' (the old value)

RESET ducklake.as_of_timestamp;
```

### Join Tables at Different Times

```sql
-- Query orders from January, but customers from February
SELECT ducklake.set_table_snapshot('orders', '2025-01-01');
SELECT ducklake.set_table_snapshot('customers', '2025-02-01');

-- Orders from Jan 1, customers from Feb 1
SELECT o.order_id, o.amount, c.name, c.current_address
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;

SELECT ducklake.clear_table_snapshots();
```

## Configuration

### ducklake.as_of_timestamp

Global session setting for time travel queries.

- **Type:** Text (timestamp)
- **Default:** Empty (no time travel)
- **Scope:** Session or transaction (`SET LOCAL` for transaction-only)

```sql
-- Session-wide
SET ducklake.as_of_timestamp = '2025-01-01 12:00:00';

-- Transaction-only
BEGIN;
SET LOCAL ducklake.as_of_timestamp = '2025-01-01';
SELECT * FROM orders;
COMMIT;  -- timestamp resets after commit
```

## Functions

### ducklake.set_table_snapshot(table_name, timestamp)

Sets a time travel timestamp for a specific table.

**Parameters:**
- `table_name` (text): Table name, optionally schema-qualified (e.g., `'public.orders'`)
- `timestamp` (text): Timestamp in any accepted format (parsed by DuckDB)

**Example:**
```sql
SELECT ducklake.set_table_snapshot('orders', '2025-01-01 10:00:00');
```

**Error conditions:**
- Table does not exist
- Table is not a DuckLake table (not created with `USING ducklake`)

### ducklake.clear_table_snapshots()

Clears all per-table timestamps set via `ducklake.set_table_snapshot()`.

**Example:**
```sql
SELECT ducklake.clear_table_snapshots();
```

## Important Notes

### DuckLake Tables Only

Time travel only works with DuckLake tables (created with `USING ducklake`).

- If `ducklake.as_of_timestamp` is set and you query a non-DuckLake table, a `NOTICE` is shown and the table is queried normally.
- Calling `ducklake.set_table_snapshot()` on a non-DuckLake table raises an error.

### Snapshot Availability

- Timestamps must correspond to when data actually existed
- Querying before any data was written returns no rows
- Querying between snapshots returns the most recent snapshot before that timestamp

### Read-Only Queries

Time travel queries are read-only; DML is not supported while a time-travel timestamp is active.

## See Also

- [DuckLake Time Travel](https://ducklake.select/docs/stable/duckdb/usage/time_travel) - DuckLake reference documentation
