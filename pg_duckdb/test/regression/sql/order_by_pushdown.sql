CREATE TABLE pgduckdb_order_pushdown (id INTEGER, payload TEXT);

INSERT INTO pgduckdb_order_pushdown VALUES (1, 'row-1'), (2, 'row-2'), (null, 'row-3');

CREATE INDEX ON pgduckdb_order_pushdown (id);

-- Table without index should not push down ORDER BY
CREATE TABLE pgduckdb_order_pushdown_no_idx (id INTEGER, payload TEXT);
INSERT INTO pgduckdb_order_pushdown_no_idx VALUES (1, 'plain-1'), (2, 'plain-2'), (null, 'plain-3');

-- Baseline: ORDER BY asc should be pushed to Postgres scan
EXPLAIN SELECT * FROM pgduckdb_order_pushdown ORDER BY id;
SELECT * FROM pgduckdb_order_pushdown ORDER BY id;

-- Descending order should also leverage the index
EXPLAIN SELECT * FROM pgduckdb_order_pushdown ORDER BY id DESC;
SELECT * FROM pgduckdb_order_pushdown ORDER BY id DESC;

-- ORDER BY with incompatible NULLS clause should stay in DuckDB
EXPLAIN SELECT * FROM pgduckdb_order_pushdown ORDER BY id NULLS FIRST;
SELECT * FROM pgduckdb_order_pushdown ORDER BY id NULLS FIRST;

-- Table without a supporting index should not push down ORDER BY
EXPLAIN SELECT * FROM pgduckdb_order_pushdown_no_idx ORDER BY id;
SELECT * FROM pgduckdb_order_pushdown_no_idx ORDER BY id;

-- Complex query: nested subquery with ORDER BY
EXPLAIN SELECT * FROM ( SELECT id, payload FROM pgduckdb_order_pushdown ) sub_query ORDER BY id;
SELECT * FROM ( SELECT id, payload FROM pgduckdb_order_pushdown ) sub_query ORDER BY id;

-- Complex query: join between indexed and non-indexed tables
EXPLAIN SELECT a.id, a.payload, b.payload FROM pgduckdb_order_pushdown a JOIN pgduckdb_order_pushdown_no_idx b ON a.id = b.id ORDER BY a.id;
SELECT a.id, a.payload, b.payload FROM pgduckdb_order_pushdown a JOIN pgduckdb_order_pushdown_no_idx b ON a.id = b.id ORDER BY a.id;

-- Top-N: ORDER BY + LIMIT should push both the order and the LIMIT into the scan
EXPLAIN SELECT * FROM pgduckdb_order_pushdown ORDER BY id LIMIT 2;
SELECT * FROM pgduckdb_order_pushdown ORDER BY id LIMIT 2;

-- Top-N with OFFSET
EXPLAIN SELECT * FROM pgduckdb_order_pushdown ORDER BY id LIMIT 1 OFFSET 1;
SELECT * FROM pgduckdb_order_pushdown ORDER BY id LIMIT 1 OFFSET 1;

-- Top-N descending
EXPLAIN SELECT * FROM pgduckdb_order_pushdown ORDER BY id DESC LIMIT 2;
SELECT * FROM pgduckdb_order_pushdown ORDER BY id DESC LIMIT 2;

-- Top-N with incompatible NULLS clause should stay in DuckDB
EXPLAIN SELECT * FROM pgduckdb_order_pushdown ORDER BY id NULLS FIRST LIMIT 2;
SELECT * FROM pgduckdb_order_pushdown ORDER BY id NULLS FIRST LIMIT 2;

-- Top-N on a table without a supporting index should stay in DuckDB
EXPLAIN SELECT * FROM pgduckdb_order_pushdown_no_idx ORDER BY id LIMIT 2;
SELECT * FROM pgduckdb_order_pushdown_no_idx ORDER BY id LIMIT 2;

DROP TABLE pgduckdb_order_pushdown;
DROP TABLE pgduckdb_order_pushdown_no_idx;
