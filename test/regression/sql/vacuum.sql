-- Test VACUUM on DuckLake tables (should be a no-op; maintenance is
-- handled by the ducklake background maintenance worker).

-- Ensure data inlining is disabled so we create actual files
CALL ducklake.set_option('data_inlining_row_limit', 0);

-- Scenario 1: VACUUM does not error and data survives
CREATE TABLE vacuum_noop (a int, b text) USING ducklake;

INSERT INTO vacuum_noop VALUES (1, 'one');
INSERT INTO vacuum_noop VALUES (2, 'two');
INSERT INTO vacuum_noop VALUES (3, 'three');

SELECT * FROM vacuum_noop ORDER BY a;

-- VACUUM should silently succeed (no-op)
VACUUM vacuum_noop;
VACUUM VERBOSE vacuum_noop;

-- Data is unchanged
SELECT * FROM vacuum_noop ORDER BY a;

DROP TABLE vacuum_noop;
