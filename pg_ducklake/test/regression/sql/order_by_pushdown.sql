-- The ORDER BY / Top-N -> Postgres-scan pushdown optimizer lives in the shared
-- libpgduckdb kernel, so it is active in pg_ducklake too. A query that touches a
-- DuckLake table is offloaded to DuckDB as a whole; when that plan also scans a
-- regular PG heap table with a matching btree index, the Top-N over that heap
-- scan is pushed into Postgres (ORDER BY + LIMIT appear inside
-- PGDUCKDB_POSTGRES_SCAN instead of a DuckDB TOP_N node).

CREATE TABLE pgducklake_heap_idx (id int PRIMARY KEY, val text);
INSERT INTO pgducklake_heap_idx SELECT g, 'v' || g FROM generate_series(1, 20) g;

CREATE TABLE pgducklake_lake (x int) USING ducklake;
INSERT INTO pgducklake_lake VALUES (1), (2), (3);

-- Top-N over the indexed heap, joined to a DuckLake table (the DuckLake
-- reference forces the whole query through DuckDB). The heap scan shows the
-- pushed "Order By" + "Limit".
EXPLAIN SELECT h.id, h.val
FROM (SELECT id, val FROM pgducklake_heap_idx ORDER BY id LIMIT 3) h
JOIN pgducklake_lake ON h.id = pgducklake_lake.x;

SELECT h.id, h.val
FROM (SELECT id, val FROM pgducklake_heap_idx ORDER BY id LIMIT 3) h
JOIN pgducklake_lake ON h.id = pgducklake_lake.x
ORDER BY h.id;

-- Descending Top-N also pushes (btree backward scan).
EXPLAIN SELECT h.id
FROM (SELECT id FROM pgducklake_heap_idx ORDER BY id DESC LIMIT 3) h
JOIN pgducklake_lake ON h.id = pgducklake_lake.x;

DROP TABLE pgducklake_lake;
DROP TABLE pgducklake_heap_idx;
