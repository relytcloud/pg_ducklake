-- Upstream: test/sql/partitioning/update_partition_function.test
-- UPDATE must preserve rows when changing identity and transformed partition keys.

SET datestyle TO ISO;

CREATE TABLE upstream_update_partition (
    p text,
    ts timestamp,
    v text
) USING ducklake;

CALL ducklake.set_partition(
    'upstream_update_partition'::regclass,
    'p',
    'day(ts)'
);

INSERT INTO upstream_update_partition VALUES
    ('p1', TIMESTAMP '2026-02-05', 'va'),
    ('p2', TIMESTAMP '2026-03-10', 'vb');

UPDATE upstream_update_partition SET p = 'p3' WHERE v = 'va';
SELECT * FROM upstream_update_partition ORDER BY p;

UPDATE upstream_update_partition SET v = 'vc' WHERE p = 'p2';
SELECT * FROM upstream_update_partition ORDER BY p;

UPDATE upstream_update_partition
SET ts = TIMESTAMP '2026-06-15'
WHERE p = 'p3';
SELECT * FROM upstream_update_partition ORDER BY p;

SELECT * FROM ducklake.get_partition('upstream_update_partition'::regclass);

DROP TABLE upstream_update_partition;
