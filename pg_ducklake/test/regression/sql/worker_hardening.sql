-- Failure-path behavior of the shared DuckDB worker: worker-side errors surface as
-- PG ERRORs, cancellation interrupts the running query, an Arrow-unsupported column
-- fails loudly, and a killed worker respawns on the next dispatch.
CREATE TABLE wh_t (a int, b text) USING ducklake;
INSERT INTO wh_t SELECT g, 'row_' || g FROM generate_series(1, 1000) g;

SET ducklake.use_shared_worker = on;

-- A worker-side execution error surfaces as a PG ERROR with DuckDB's message,
-- identical to the in-process error.
SET ducklake.use_shared_worker = off;
SELECT CAST(b AS int) FROM wh_t WHERE a = 1;
SET ducklake.use_shared_worker = on;
SELECT CAST(b AS int) FROM wh_t WHERE a = 1;

-- Cancellation: a statement timeout cancels a dispatched long-running aggregate
-- (the backend interrupts the worker session; the worker aborts the query).
SET statement_timeout = '200ms';
SELECT count(*) FROM wh_t x, wh_t y, wh_t z WHERE x.a + y.a + z.a > 0;
RESET statement_timeout;

-- The worker survived the cancelled query and still serves new dispatches.
SELECT count(*) FROM wh_t;

-- A heap column the Arrow scan transport does not support fails loudly, naming the
-- column, instead of silently falling back.
CREATE TABLE wh_uuid (a int, u uuid);
INSERT INTO wh_uuid VALUES (1, '00000000-0000-0000-0000-000000000001');
SELECT t.a, h.u FROM wh_t t JOIN wh_uuid h USING (a) ORDER BY t.a;

-- Kill the duckdb worker; the next dispatch respawns it on demand.
SELECT count(pg_terminate_backend(pid)) AS terminated
FROM pg_stat_activity WHERE backend_type = 'pg_ducklake duckdb worker';
SELECT pg_sleep(0.5);
SELECT count(*) FROM wh_t;
SELECT count(*) > 0 AS worker_running
FROM pg_stat_activity WHERE backend_type = 'pg_ducklake duckdb worker';

RESET ducklake.use_shared_worker;
DROP TABLE wh_t;
DROP TABLE wh_uuid;
