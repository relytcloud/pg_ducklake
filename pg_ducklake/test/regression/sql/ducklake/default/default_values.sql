-- Upstream: test/sql/default/default_values.test
BEGIN;
CREATE TABLE upstream_default_values (i integer DEFAULT 42, j integer) USING ducklake;
INSERT INTO upstream_default_values (j) VALUES (100);
COMMIT;
INSERT INTO upstream_default_values (j) VALUES (200);
SELECT * FROM upstream_default_values ORDER BY j;
CREATE TABLE upstream_default_special (
  i integer,
  s1 text DEFAULT '',
  -- DuckDB's serialized special default is SQL NULL, not the text 'NULL'.
  s2 text DEFAULT NULL
) USING ducklake;
INSERT INTO upstream_default_special (i) VALUES (100);
SELECT i, s1, s2
FROM upstream_default_special
WHERE s2 IS NULL
ORDER BY i;
DROP TABLE upstream_default_values, upstream_default_special;
