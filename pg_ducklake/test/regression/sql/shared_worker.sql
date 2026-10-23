-- Read-only SELECTs dispatched to the shared per-database duckdb worker.
CREATE TABLE sw_t (i int, d double precision, s text, b boolean, ts timestamp, dt date, n numeric(10, 2)) USING ducklake;
INSERT INTO sw_t
SELECT g, g * 1.5, 'row' || g, g % 2 = 0, '2024-01-01'::timestamp + g * interval '1 hour', ('2024-01-01'::date + g * interval '1 day')::date, g * 0.25
FROM generate_series(1, 1000) g;
INSERT INTO sw_t VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL);
-- Small inserts stay inlined in PostgreSQL; the worker reads them through its RPC.
CREATE TABLE sw_small (i int, s text) USING ducklake;
INSERT INTO sw_small VALUES (1, 'a'), (2, 'b'), (3, 'c');
CREATE TABLE sw_heap (i int, label text);
INSERT INTO sw_heap VALUES (1, 'one'), (2, 'two');

CREATE FUNCTION sw_total() RETURNS bigint LANGUAGE sql
AS $$ SELECT coalesce(total_sessions, 0) FROM ducklake.worker_stats() $$;

-- Parity: the same queries in-process and through the worker.
SET ducklake.use_shared_worker = off;
SELECT count(*), count(i), sum(i), min(s), max(ts), min(dt), sum(n), bool_and(b) FROM sw_t;
SELECT i, d, s, b, ts, dt, n FROM sw_t WHERE i IN (1, 2, 1000) OR i IS NULL ORDER BY i NULLS LAST;
SELECT b, count(*), sum(d) FROM sw_t GROUP BY b ORDER BY b NULLS LAST;
SELECT t.i, t.s, sm.s FROM sw_t t JOIN sw_small sm ON sm.i = t.i ORDER BY t.i;
SELECT * FROM sw_small ORDER BY i;

SET ducklake.use_shared_worker = on;
SELECT count(*), count(i), sum(i), min(s), max(ts), min(dt), sum(n), bool_and(b) FROM sw_t;
SELECT i, d, s, b, ts, dt, n FROM sw_t WHERE i IN (1, 2, 1000) OR i IS NULL ORDER BY i NULLS LAST;
SELECT b, count(*), sum(d) FROM sw_t GROUP BY b ORDER BY b NULLS LAST;
SELECT t.i, t.s, sm.s FROM sw_t t JOIN sw_small sm ON sm.i = t.i ORDER BY t.i;
SELECT * FROM sw_small ORDER BY i;
SELECT worker_pid IS NOT NULL AS has_worker, active_sessions FROM ducklake.worker_stats();

-- Dispatch gate: only read-only SELECTs over DuckLake tables, outside
-- transaction blocks, leave this backend.
SELECT sw_total() AS before \gset
SELECT count(*) FROM sw_t;
SELECT sw_total() - :before AS dispatched;

SELECT sw_total() AS before \gset
SELECT t.i, h.label FROM sw_t t JOIN sw_heap h ON h.i = t.i ORDER BY t.i;
SELECT sw_total() - :before AS dispatched;

CREATE TEMP TABLE sw_tmp (i int);
INSERT INTO sw_tmp VALUES (3);
SELECT sw_total() AS before \gset
SELECT t.i FROM sw_t t JOIN sw_tmp x ON x.i = t.i;
SELECT sw_total() - :before AS dispatched;

SELECT sw_total() AS before \gset
BEGIN;
SELECT count(*) FROM sw_t;
COMMIT;
SELECT sw_total() - :before AS dispatched;

SELECT sw_total() AS before \gset
INSERT INTO sw_small VALUES (4, 'd');
SELECT sw_total() - :before AS dispatched;

SELECT sw_total() AS before \gset
EXPLAIN (COSTS OFF) SELECT count(*) FROM sw_t;
SELECT sw_total() - :before AS dispatched;

-- Parameters travel with the query.
PREPARE sw_p(int, text) AS SELECT i, s FROM sw_t WHERE i > $1 AND s <> $2 ORDER BY i LIMIT 3;
SELECT sw_total() AS before \gset
EXECUTE sw_p(997, 'row999');
EXECUTE sw_p(997, NULL);
SELECT sw_total() - :before AS dispatched;

-- Dispatches nest: the loop's query holds a session while each inner query
-- opens another one on the same worker and arena.
SELECT sw_total() AS before \gset
DO $$
DECLARE
	r record;
	n bigint;
	total bigint := 0;
BEGIN
	FOR r IN SELECT i FROM sw_t WHERE i <= 3 ORDER BY i LOOP
		SELECT count(*) INTO n FROM sw_small WHERE i = r.i;
		total := total + n;
	END LOOP;
	RAISE NOTICE 'matched %', total;
END $$;
SELECT sw_total() - :before AS dispatched;

-- Session settings travel with the query (in-process DuckDB keeps the
-- TimeZone it was initialized with, so only the worker follows the SET).
SET TimeZone = 'Asia/Tokyo';
SELECT (ts::timestamptz)::text FROM sw_t WHERE i = 1;
SET ducklake.use_shared_worker = off;
SELECT (ts::timestamptz)::text FROM sw_t WHERE i = 1;
RESET TimeZone;

-- Errors keep their DuckDB type and message.
SELECT s::int FROM sw_t WHERE i = 1;
SET ducklake.use_shared_worker = on;
SELECT s::int FROM sw_t WHERE i = 1;

-- Cancellation reaches the worker; the worker survives it.
SET statement_timeout = '300ms';
SELECT count(*) FROM sw_t a, sw_t b, sw_t c, sw_t d WHERE a.i + b.i + c.i + d.i = -1;
RESET statement_timeout;
SELECT count(*) FROM sw_t;

-- A killed worker is respawned by the next dispatch.
SELECT pg_terminate_backend(worker_pid) FROM ducklake.worker_stats();
DO $$
BEGIN
	WHILE EXISTS (SELECT 1 FROM ducklake.worker_stats() WHERE worker_pid IS NOT NULL) LOOP
		PERFORM pg_sleep(0.05);
	END LOOP;
END $$;
SELECT count(*) FROM sw_t;
SELECT worker_pid IS NOT NULL AS has_worker FROM ducklake.worker_stats();

-- Result pages: regression.conf sets 64kB arena pages, so these rows span
-- several pages; a row larger than a page and a nested type travel serialized.
CREATE TABLE sw_wide (i int, s text, l int[]) USING ducklake;
INSERT INTO sw_wide SELECT g, repeat('x', 1000), ARRAY[g, g + 1] FROM generate_series(1, 500) g;
INSERT INTO sw_wide VALUES (0, repeat('y', 200000), ARRAY[0]);
SET ducklake.use_shared_worker = off;
SELECT count(*), sum(length(s)), min(i), max(i) FROM sw_wide;
SELECT i, length(s), left(s, 3) FROM sw_wide WHERE i IN (0, 1, 500) ORDER BY i;
SELECT i, l FROM sw_wide WHERE i <= 2 ORDER BY i;
SET ducklake.use_shared_worker = on;
SELECT count(*), sum(length(s)), min(i), max(i) FROM sw_wide;
SELECT i, length(s), left(s, 3) FROM sw_wide WHERE i IN (0, 1, 500) ORDER BY i;
SELECT i, l FROM sw_wide WHERE i <= 2 ORDER BY i;
SELECT count(*) FROM (SELECT s FROM sw_wide WHERE i > 0) x;

RESET ducklake.use_shared_worker;
DROP FUNCTION sw_total();
DROP TABLE sw_t, sw_small, sw_heap, sw_wide;
