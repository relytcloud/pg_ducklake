-- Dispatch read-only heap-table queries to the shared DuckDB worker and
-- confirm identical results to in-process DuckDB execution. force_execution is on
-- globally (regression.conf), so each SELECT is offloaded to DuckDB; toggling
-- duckdb.use_shared_worker decides whether DuckDB runs in this backend or the worker.
CREATE TABLE sw_t(a int, b int, c text);
INSERT INTO sw_t SELECT g, g % 10, 'row_' || g FROM generate_series(1, 1000) g;

CREATE TABLE sw_u(b int, label text);
INSERT INTO sw_u VALUES (0, 'zero'), (1, 'one'), (2, 'two');

-- count
SET duckdb.use_shared_worker = off;
SELECT count(*) FROM sw_t;
SET duckdb.use_shared_worker = on;
SELECT count(*) FROM sw_t;

-- filter + ordered scan
SET duckdb.use_shared_worker = off;
SELECT a, b, c FROM sw_t WHERE a <= 5 ORDER BY a;
SET duckdb.use_shared_worker = on;
SELECT a, b, c FROM sw_t WHERE a <= 5 ORDER BY a;

-- grouped aggregation
SET duckdb.use_shared_worker = off;
SELECT b, count(*), sum(a) FROM sw_t GROUP BY b ORDER BY b;
SET duckdb.use_shared_worker = on;
SELECT b, count(*), sum(a) FROM sw_t GROUP BY b ORDER BY b;

-- join
SET duckdb.use_shared_worker = off;
SELECT u.label, count(*) FROM sw_t t JOIN sw_u u USING (b) GROUP BY u.label ORDER BY u.label;
SET duckdb.use_shared_worker = on;
SELECT u.label, count(*) FROM sw_t t JOIN sw_u u USING (b) GROUP BY u.label ORDER BY u.label;

-- bool / timestamp / timestamptz columns (Arrow scan transport coverage)
CREATE TABLE sw_types(a int, ok bool, ts timestamp, tstz timestamptz);
INSERT INTO sw_types VALUES
    (1, true, '2024-01-02 03:04:05', '2024-01-02 03:04:05+00'),
    (2, false, '1999-12-31 23:59:59', '1999-12-31 23:59:59+00'),
    (3, NULL, NULL, NULL);
SET timezone = 'UTC';
SET duckdb.use_shared_worker = off;
SELECT a, ok, ts, tstz FROM sw_types ORDER BY a;
SET duckdb.use_shared_worker = on;
SELECT a, ok, ts, tstz FROM sw_types ORDER BY a;
RESET timezone;

-- temp tables are backend-local: dispatched queries read them via the inversion
-- path (never the scan-producer pool)
CREATE TEMP TABLE sw_tmp(a int, d text);
INSERT INTO sw_tmp VALUES (1, 't1'), (2, 't2');
SET duckdb.use_shared_worker = off;
SELECT t.a, m.d FROM sw_t t JOIN sw_tmp m USING (a) ORDER BY t.a;
SET duckdb.use_shared_worker = on;
SELECT t.a, m.d FROM sw_t t JOIN sw_tmp m USING (a) ORDER BY t.a;

-- a worker-side execution error surfaces as a PG ERROR
SET duckdb.use_shared_worker = off;
SELECT CAST(c AS int) FROM sw_t WHERE a = 1;
SET duckdb.use_shared_worker = on;
SELECT CAST(c AS int) FROM sw_t WHERE a = 1;

-- an Arrow-unsupported heap column fails loudly, naming the column
CREATE TABLE sw_uuid(a int, u uuid);
INSERT INTO sw_uuid VALUES (1, '00000000-0000-0000-0000-000000000001');
SELECT a, u FROM sw_uuid;

-- cancellation: a statement timeout cancels a dispatched long-running aggregate
SET statement_timeout = '200ms';
SELECT count(*) FROM sw_t x, sw_t y, sw_t z WHERE x.a + y.a + z.a > 0;
RESET statement_timeout;

-- the worker survived the cancelled query and still serves new dispatches
SELECT count(*) FROM sw_t;

-- kill the duckdb worker; the next dispatch respawns it on demand
SELECT count(pg_terminate_backend(pid)) AS terminated
FROM pg_stat_activity WHERE backend_type = 'pg_duckdb duckdb worker';
SELECT pg_sleep(0.5);
SELECT count(*) FROM sw_t;

RESET duckdb.use_shared_worker;
DROP TABLE sw_t, sw_u, sw_types, sw_uuid;
