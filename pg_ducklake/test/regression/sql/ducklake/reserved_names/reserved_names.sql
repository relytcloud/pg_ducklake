-- Upstream: test/sql/reserved_names/reserved_names.test

CREATE TABLE upstream_reserved_name (
    id integer,
    _ducklake_internal_snapshot_id integer
) USING ducklake;
SELECT to_regclass('upstream_reserved_name') IS NULL AS creation_rejected;
