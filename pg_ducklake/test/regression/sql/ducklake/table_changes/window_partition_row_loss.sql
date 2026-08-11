-- Upstream: test/sql/table_changes/window_partition_row_loss.test
-- Non-filtering windows over change functions must not drop rows.

CREATE TABLE upstream_change_window (id integer, val integer) USING ducklake;
SELECT max(snapshot_id) AS v0 FROM ducklake.ducklake_snapshot \gset

INSERT INTO upstream_change_window VALUES (1, 100), (2, 200);
UPDATE upstream_change_window SET val = 150 WHERE id = 1;
SELECT max(snapshot_id) AS vend FROM ducklake.ducklake_snapshot \gset

SELECT count(*)
FROM ducklake.table_insertions(
    'upstream_change_window'::regclass, :v0, :vend
);

SELECT count(*)
FROM ducklake.table_changes(
    'upstream_change_window'::regclass, :v0, :vend
);

SELECT count(*)
FROM (
    SELECT max(r['snapshot_id']::bigint) OVER (
        PARTITION BY r['id']::integer, r['snapshot_id']::bigint
    )
    FROM ducklake.table_changes(
        'upstream_change_window'::regclass, :v0, :vend
    ) AS r
) AS windowed;

SELECT * FROM ducklake.flush_inlined_data('upstream_change_window'::regclass);

SELECT count(*)
FROM (
    SELECT max(r['snapshot_id']::bigint) OVER (
        PARTITION BY r['id']::integer, r['snapshot_id']::bigint
    )
    FROM ducklake.table_changes(
        'upstream_change_window'::regclass, :v0, :vend
    ) AS r
) AS windowed;

DROP TABLE upstream_change_window;
