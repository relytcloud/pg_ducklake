-- Setup: create table and make multiple versions
CREATE TABLE tt (id int, name text) USING ducklake;
SELECT max(snapshot_id) AS v0 FROM ducklake.ducklake_snapshot \gset
INSERT INTO tt VALUES (1, 'Alice');
INSERT INTO tt VALUES (2, 'Bob');

-- 1. Basic version query (v0 = CREATE, v0+1 = INSERT Alice, v0+2 = INSERT Bob)
SELECT * FROM ducklake.time_travel('tt', :v0);
SELECT * FROM ducklake.time_travel('tt', :v0 + 1);
SELECT * FROM ducklake.time_travel('tt', :v0 + 2);

-- 2. Schema evolution: add column, query old version (still 2 columns)
ALTER TABLE tt ADD COLUMN age int;
INSERT INTO tt VALUES (3, 'Carol', 25);
SELECT * FROM ducklake.time_travel('tt', :v0 + 2);
-- Latest version has 3 columns; use count to avoid row-order sensitivity
SELECT max(snapshot_id) AS v_latest FROM ducklake.ducklake_snapshot \gset
SELECT count(*) FROM ducklake.time_travel('tt', :v_latest);

-- 3. Views over historical snapshots: verify schema via pg_catalog
CREATE VIEW tt_v1 AS SELECT * FROM ducklake.time_travel('tt', :v0 + 1);
CREATE VIEW tt_v2 AS SELECT * FROM ducklake.time_travel('tt', :v0 + 2);
CREATE VIEW tt_v_latest AS SELECT * FROM ducklake.time_travel('tt', :v_latest);
SELECT attname, format_type(atttypid, atttypmod)
FROM pg_attribute WHERE attrelid = 'tt_v1'::regclass AND attnum > 0
ORDER BY attnum;
SELECT attname, format_type(atttypid, atttypmod)
FROM pg_attribute WHERE attrelid = 'tt_v_latest'::regclass AND attnum > 0
ORDER BY attnum;
SELECT * FROM tt_v1;
SELECT * FROM tt_v2;

-- 4. Timestamp-based query (use count to avoid row-order sensitivity)
SELECT count(*) FROM ducklake.time_travel('tt', now());

-- 5. Schema + table overload
SELECT * FROM ducklake.time_travel('public', 'tt', :v0 + 2);
SELECT count(*) FROM ducklake.time_travel('public', 'tt', now());

-- 6. Regclass overload
SELECT * FROM ducklake.time_travel('tt'::regclass, :v0 + 2);
SELECT count(*) FROM ducklake.time_travel('tt'::regclass, now());

-- 7. Error: invalid version (direct query and via view)
SELECT * FROM ducklake.time_travel('tt', 999999);
CREATE VIEW tt_v_bad AS SELECT * FROM ducklake.time_travel('tt', 999999);
SELECT * FROM tt_v_bad;

-- Cleanup
DROP VIEW tt_v1, tt_v2, tt_v_latest;
DROP TABLE tt;
