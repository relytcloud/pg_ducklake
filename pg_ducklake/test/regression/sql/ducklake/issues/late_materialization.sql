-- Upstream: test/sql/issues/late_materialization.test

CREATE TABLE upstream_late_materialization (id integer, value text) USING ducklake;
INSERT INTO upstream_late_materialization VALUES
    (1, 'hello'), (2, 'world'), (3, 'this'),
    (4, 'is'), (5, 'a'), (6, 'test');
SELECT * FROM upstream_late_materialization
WHERE id > 3 ORDER BY value DESC LIMIT 1;
DROP TABLE upstream_late_materialization;
