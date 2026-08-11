-- Upstream: test/sql/delete/delete_join.test
-- DELETE ... USING must delete each joined target row exactly once.

CREATE TABLE upstream_delete_join USING ducklake AS
SELECT i AS id FROM generate_series(0, 499) AS g(i);
CREATE TEMP TABLE upstream_deleted_rows AS
SELECT i AS delete_id FROM generate_series(0, 998, 2) AS g(i);

BEGIN;
INSERT INTO upstream_delete_join SELECT i FROM generate_series(500, 999) AS g(i);
DELETE FROM upstream_delete_join t USING upstream_deleted_rows d
WHERE t.id = d.delete_id;
COMMIT;

SELECT count(*) FROM upstream_delete_join;
SELECT count(*) FILTER (WHERE id % 2 = 0) FROM upstream_delete_join;

DROP TABLE upstream_delete_join;
DROP TABLE upstream_deleted_rows;
