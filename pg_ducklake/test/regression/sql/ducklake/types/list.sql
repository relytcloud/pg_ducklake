-- Upstream: test/sql/types/list.test
-- Skip: the PostgreSQL path does not expose DuckDB's stats(l[1]) min/max/null summary.
CREATE TABLE upstream_type_list (id integer, l integer[]) USING ducklake;
SELECT count(*) FROM upstream_type_list;
INSERT INTO upstream_type_list VALUES
  (1, ARRAY[1]), (2, ARRAY[NULL::integer]), (3, NULL), (4, ARRAY[3]);
SELECT id, l FROM upstream_type_list ORDER BY id;
SELECT id, l FROM upstream_type_list WHERE l[1] = 1 ORDER BY id;
SELECT id, l FROM upstream_type_list WHERE l[1] = 100 ORDER BY id;
INSERT INTO upstream_type_list VALUES (5, ARRAY[4, 5]), (6, ARRAY[6, 7]);
SELECT id, l FROM upstream_type_list ORDER BY id;
CALL ducklake.recycle_ddb();
SELECT id, l FROM upstream_type_list ORDER BY id;
DROP TABLE upstream_type_list;
