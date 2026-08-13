-- Upstream: test/sql/reserved_names/reserved_names.test

CALL ducklake.set_option('data_inlining_row_limit', 100);
CREATE TABLE upstream_reserved_name (
    id integer,
    _ducklake_internal_snapshot_id integer
) USING ducklake;
SELECT to_regclass('upstream_reserved_name') IS NULL AS creation_rejected;
DROP TABLE IF EXISTS upstream_reserved_name;
CALL ducklake.set_option('data_inlining_row_limit', 0);
