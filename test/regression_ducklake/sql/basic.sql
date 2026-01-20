CREATE TABLE t (a int, b int) using ducklake;

INSERT INTO t VALUES (1, 101), (2, 202);

SELECT * FROM t ORDER BY a;

SELECT * FROM t WHERE a = 1;

DROP TABLE t;
