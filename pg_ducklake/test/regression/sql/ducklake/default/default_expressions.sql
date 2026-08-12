-- Upstream: test/sql/default/default_expressions.test
-- Skip: SET DEFAULT of a PostgreSQL string literal is recorded as a DuckLake expression instead of literal metadata.
CREATE TABLE upstream_default_expr_time (
  id integer,
  created_at timestamp DEFAULT now()
) USING ducklake;
INSERT INTO upstream_default_expr_time (id) VALUES (1);
SELECT id, created_at < now() AS default_precedes_read
FROM upstream_default_expr_time ORDER BY id;
CREATE TABLE upstream_default_expr_num (id integer, id_plus integer DEFAULT 1) USING ducklake;
INSERT INTO upstream_default_expr_num (id) VALUES (0);
ALTER TABLE upstream_default_expr_num ALTER id_plus SET DEFAULT round(pi());
INSERT INTO upstream_default_expr_num (id) VALUES (1);
SELECT * FROM upstream_default_expr_num ORDER BY id;
CREATE TABLE upstream_default_expr_literal (a integer, b text) USING ducklake;
ALTER TABLE upstream_default_expr_literal ALTER b SET DEFAULT 'random()';
INSERT INTO upstream_default_expr_literal (a) VALUES (1);
SELECT * FROM upstream_default_expr_literal ORDER BY a;
SELECT t.table_name, c.column_name, c.default_value,
       c.default_value_type, c.default_value_dialect,
       c.end_snapshot IS NULL AS active
FROM ducklake.ducklake_table t
JOIN ducklake.ducklake_column c USING (table_id)
WHERE t.table_name IN ('upstream_default_expr_time',
                       'upstream_default_expr_num',
                       'upstream_default_expr_literal')
ORDER BY t.table_name, c.column_order, c.begin_snapshot;
ALTER TABLE upstream_default_expr_literal ADD COLUMN j double precision DEFAULT random();
SELECT NOT EXISTS (
  SELECT 1 FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name = 'upstream_default_expr_literal'
    AND column_name = 'j'
) AS nonliteral_column_rejected;
DROP TABLE upstream_default_expr_time, upstream_default_expr_num, upstream_default_expr_literal;
