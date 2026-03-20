-- Verify DuckLake works after duckdb.recycle_ddb() destroys and
-- recreates the DuckDB instance (GitHub issue #81).

CREATE TABLE t (a int, b text) USING ducklake;
INSERT INTO t VALUES (1, 'before');
SELECT * FROM t;

CALL duckdb.recycle_ddb();

-- The metadata manager factory is registered in a process-global static
-- map, so it survives the recycle. The catalog must be re-attached.
SELECT * FROM t;
INSERT INTO t VALUES (2, 'after');
SELECT * FROM t ORDER BY a;

DROP TABLE t;
