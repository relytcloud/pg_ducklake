-- Dispatch real read-only DuckLake queries to the shared DuckDB worker and confirm
-- identical results to in-process execution. Writes stay in-process; only the
-- read-only SELECTs are routed to the worker when ducklake.use_shared_worker is on.
-- Each query is run in-process (off) then dispatched (on); the outputs must match.
CREATE TABLE we_t (a int, b text) USING ducklake;
INSERT INTO we_t VALUES (1, 'x'), (2, 'y'), (3, 'z');
CREATE TABLE we_u (a int, c text) USING ducklake;
INSERT INTO we_u VALUES (1, 'p'), (2, 'q');

-- Scan + ORDER BY.
SET ducklake.use_shared_worker = off;
SELECT a, b FROM we_t ORDER BY a;
SET ducklake.use_shared_worker = on;
SELECT a, b FROM we_t ORDER BY a;

-- Aggregation.
SET ducklake.use_shared_worker = off;
SELECT count(*), sum(a) FROM we_t;
SET ducklake.use_shared_worker = on;
SELECT count(*), sum(a) FROM we_t;

-- Join.
SET ducklake.use_shared_worker = off;
SELECT t.a, t.b, u.c FROM we_t t JOIN we_u u USING (a) ORDER BY t.a;
SET ducklake.use_shared_worker = on;
SELECT t.a, t.b, u.c FROM we_t t JOIN we_u u USING (a) ORDER BY t.a;

-- Grouped aggregation.
SET ducklake.use_shared_worker = off;
SELECT b, count(*) FROM we_t GROUP BY b ORDER BY b;
SET ducklake.use_shared_worker = on;
SELECT b, count(*) FROM we_t GROUP BY b ORDER BY b;

-- The worker really served the dispatched query: the accepted-session count grew by
-- exactly one (the stats call itself is not a DuckDB query, so it never dispatches).
SET ducklake.use_shared_worker = on;
SELECT ducklake.worker_stats() AS stats_before \gset
SELECT count(*) FROM we_t;
SELECT ducklake.worker_stats() - :stats_before AS dispatched_delta;

-- Hybrid: a DuckLake table joined with a regular PostgreSQL heap table. The query
-- is offloaded because it references a DuckLake table; when dispatched, the worker
-- reads the heap table under the backend's shipped snapshot.
CREATE TABLE we_heap (a int, d text);
INSERT INTO we_heap VALUES (1, 'h1'), (2, 'h2'), (3, 'h3');
SET ducklake.use_shared_worker = off;
SELECT t.a, t.b, h.d FROM we_t t JOIN we_heap h USING (a) ORDER BY t.a;
SET ducklake.use_shared_worker = on;
SELECT t.a, t.b, h.d FROM we_t t JOIN we_heap h USING (a) ORDER BY t.a;

-- Hybrid with a temp table: backend-local, so the scan must run on the requesting
-- backend (inversion path), never on the scan-producer pool.
CREATE TEMP TABLE we_tmp (a int, d text);
INSERT INTO we_tmp VALUES (1, 't1'), (3, 't3');
SET ducklake.use_shared_worker = off;
SELECT t.a, t.b, m.d FROM we_t t JOIN we_tmp m USING (a) ORDER BY t.a;
SET ducklake.use_shared_worker = on;
SELECT t.a, t.b, m.d FROM we_t t JOIN we_tmp m USING (a) ORDER BY t.a;

-- Hybrid over bool/timestamp/timestamptz heap columns (Arrow scan transport coverage).
CREATE TABLE we_types (a int, ok bool, ts timestamp, tstz timestamptz);
INSERT INTO we_types VALUES
    (1, true, '2024-01-02 03:04:05', '2024-01-02 03:04:05+00'),
    (2, false, '1999-12-31 23:59:59', '1999-12-31 23:59:59+00'),
    (3, NULL, NULL, NULL);
SET timezone = 'UTC';
SET ducklake.use_shared_worker = off;
SELECT t.a, y.ok, y.ts, y.tstz FROM we_t t JOIN we_types y USING (a) ORDER BY t.a;
SET ducklake.use_shared_worker = on;
SELECT t.a, y.ok, y.ts, y.tstz FROM we_t t JOIN we_types y USING (a) ORDER BY t.a;
RESET timezone;

-- Autocommit write dispatched to the worker, then read back in-process to confirm
-- the worker's write committed and is visible. (Transactional/DDL writes are not
-- dispatched and stay in-process.)
SET ducklake.use_shared_worker = on;
INSERT INTO we_t VALUES (4, 'w');
SET ducklake.use_shared_worker = off;
SELECT a, b FROM we_t ORDER BY a;
SELECT count(*) FROM we_t;

-- UPDATE and DELETE dispatched to the worker, then read back.
SET ducklake.use_shared_worker = on;
UPDATE we_t SET b = 'W' WHERE a = 4;
DELETE FROM we_t WHERE a = 4;
SET ducklake.use_shared_worker = off;
SELECT a, b FROM we_t ORDER BY a;

-- Writes inside a transaction block or with RETURNING never dispatch (the
-- accepted-session count stays flat); the gate keeps them in-process, where the
-- transaction-block insert runs (and rolls back) and RETURNING is refused by DuckLake.
SET ducklake.use_shared_worker = on;
SELECT ducklake.worker_stats() AS stats_before \gset
BEGIN;
INSERT INTO we_t VALUES (5, 'txn');
ROLLBACK;
INSERT INTO we_t VALUES (6, 'ret') RETURNING a, b;
SELECT ducklake.worker_stats() - :stats_before AS gated_delta;
SET ducklake.use_shared_worker = off;
DELETE FROM we_t WHERE a = 6;
SELECT a, b FROM we_t ORDER BY a;

RESET ducklake.use_shared_worker;
