-- Upstream: test/sql/update/update_rollback.test
-- Skip: rollback removal of replacement parquet files is not observable from PostgreSQL.
-- Rolling back an UPDATE must restore all original values.

CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_update_rollback USING ducklake AS
SELECT 1000 + i AS id, i % 10 AS val FROM generate_series(0, 999) AS g(i);

BEGIN;
UPDATE upstream_update_rollback SET id = id + 1;
SELECT count(*), sum(id), sum(val) FROM upstream_update_rollback;
ROLLBACK;

SELECT count(*), sum(id), sum(val) FROM upstream_update_rollback;

DROP TABLE upstream_update_rollback;
