-- Test time-travel queries for DuckLake tables using GUC + UDF approach

-- Test 1: Basic time-travel with ducklake.as_of_timestamp GUC
CREATE TABLE tt_basic (id INT, value INT) USING ducklake;
INSERT INTO tt_basic VALUES (1, 100), (2, 200);
SELECT pg_sleep(0.2);
\! sleep 0.2

-- Store timestamp for time-travel
CREATE TEMP TABLE ts_storage (ts1 TIMESTAMP, ts2 TIMESTAMP);
INSERT INTO ts_storage SELECT now()::timestamp, NULL;

UPDATE tt_basic SET value = value + 1000;

-- Set GUC and query (should show original values)
DO $$ BEGIN PERFORM set_config('ducklake.as_of_timestamp', (SELECT to_char(ts1, 'YYYY-MM-DD HH24:MI:SS.US') FROM ts_storage), false); END $$;
SELECT * FROM tt_basic ORDER BY id;

-- Reset GUC and query current (should show updated values)
RESET ducklake.as_of_timestamp;
SELECT * FROM tt_basic ORDER BY id;

DROP TABLE tt_basic;

-- Test 2: Per-table snapshot with set_table_snapshot()
CREATE TABLE tt_sales (id INT, amount INT) USING ducklake;
INSERT INTO tt_sales VALUES (1, 100), (2, 200), (3, 300);
SELECT pg_sleep(0.2);
\! sleep 0.2

UPDATE ts_storage SET ts1 = now()::timestamp;

INSERT INTO tt_sales VALUES (4, 400), (5, 500);

-- Set per-table snapshot and query
SELECT ducklake.set_table_snapshot('tt_sales'::regclass, (SELECT ts1 FROM ts_storage));
SELECT COUNT(*), SUM(amount) FROM tt_sales;

-- Clear and query current
SELECT ducklake.clear_table_snapshots();
SELECT COUNT(*), SUM(amount) FROM tt_sales;

DROP TABLE tt_sales;

-- Test 3: Per-table snapshot takes priority over GUC
CREATE TABLE tt_priority (id INT, value TEXT) USING ducklake;
INSERT INTO tt_priority VALUES (1, 'v1');
SELECT pg_sleep(0.2);
\! sleep 0.2

UPDATE ts_storage SET ts1 = now()::timestamp;

UPDATE tt_priority SET value = 'v2';
SELECT pg_sleep(0.2);
\! sleep 0.2

-- Store second timestamp
UPDATE ts_storage SET ts2 = now()::timestamp;

UPDATE tt_priority SET value = 'v3';

-- Set GUC to ts2 (should show v2), but per-table to ts1 (should show v1)
DO $$ BEGIN PERFORM set_config('ducklake.as_of_timestamp', (SELECT to_char(ts2, 'YYYY-MM-DD HH24:MI:SS.US') FROM ts_storage), false); END $$;
SELECT ducklake.set_table_snapshot('tt_priority'::regclass, (SELECT ts1 FROM ts_storage));
SELECT * FROM tt_priority ORDER BY id;

-- Clear per-table, GUC still active (should show v2)
SELECT ducklake.clear_table_snapshots();
SELECT * FROM tt_priority ORDER BY id;

-- Reset GUC (should show v3)
RESET ducklake.as_of_timestamp;
SELECT * FROM tt_priority ORDER BY id;

DROP TABLE tt_priority;

-- Test 4: JOIN with global GUC
CREATE TABLE tt_orders (order_id INT, amount INT) USING ducklake;
CREATE TABLE tt_customers (customer_id INT, name TEXT) USING ducklake;

INSERT INTO tt_orders VALUES (1, 100), (2, 200);
INSERT INTO tt_customers VALUES (1, 'Alice'), (2, 'Bob');
SELECT pg_sleep(0.2);
\! sleep 0.2

UPDATE ts_storage SET ts1 = now()::timestamp;

UPDATE tt_orders SET amount = amount * 2;
UPDATE tt_customers SET name = 'Charlie' WHERE customer_id = 1;

-- Query both tables at historical timestamp via GUC
DO $$ BEGIN PERFORM set_config('ducklake.as_of_timestamp', (SELECT to_char(ts1, 'YYYY-MM-DD HH24:MI:SS.US') FROM ts_storage), false); END $$;
SELECT o.order_id, o.amount, c.name
FROM tt_orders o
JOIN tt_customers c ON o.order_id = c.customer_id
ORDER BY o.order_id;

-- Current state
RESET ducklake.as_of_timestamp;
SELECT o.order_id, o.amount, c.name
FROM tt_orders o
JOIN tt_customers c ON o.order_id = c.customer_id
ORDER BY o.order_id;

DROP TABLE tt_orders;
DROP TABLE tt_customers;

-- Test 5: Non-DuckLake table with GUC (should be ignored with NOTICE)
CREATE TABLE tt_heap (id INT);
INSERT INTO tt_heap VALUES (1);

SET ducklake.as_of_timestamp = '2025-01-01';
SELECT * FROM tt_heap;
RESET ducklake.as_of_timestamp;

DROP TABLE tt_heap;

-- Test 6: set_table_snapshot on non-DuckLake table (should error)
CREATE TABLE tt_not_ducklake (id INT);
SELECT ducklake.set_table_snapshot('tt_not_ducklake'::regclass, '2025-01-01'::timestamp);
DROP TABLE tt_not_ducklake;

-- Test 7: Invalid timestamp format in set_table_snapshot (should error)
CREATE TABLE tt_invalid (id INT) USING ducklake;
INSERT INTO tt_invalid VALUES (1);

SELECT ducklake.set_table_snapshot('tt_invalid'::regclass, 'invalid-timestamp'::timestamp);

DROP TABLE tt_invalid;

-- Test 8: Aggregation with GUC time-travel
CREATE TABLE tt_agg (id INT, amount INT) USING ducklake;
INSERT INTO tt_agg VALUES (1, 10), (2, 20), (3, 30);
SELECT pg_sleep(0.2);
\! sleep 0.2

UPDATE ts_storage SET ts1 = now()::timestamp;

INSERT INTO tt_agg VALUES (4, 40), (5, 50);

DO $$ BEGIN PERFORM set_config('ducklake.as_of_timestamp', (SELECT to_char(ts1, 'YYYY-MM-DD HH24:MI:SS.US') FROM ts_storage), false); END $$;
SELECT COUNT(*), SUM(amount), AVG(amount) FROM tt_agg;

RESET ducklake.as_of_timestamp;
SELECT COUNT(*), SUM(amount), AVG(amount) FROM tt_agg;

DROP TABLE tt_agg;

-- Cleanup
DROP TABLE ts_storage;
