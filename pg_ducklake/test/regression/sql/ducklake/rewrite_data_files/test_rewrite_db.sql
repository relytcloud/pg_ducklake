-- Upstream: test/sql/rewrite_data_files/test_rewrite_db.test
-- Catalog-wide rewrite processes delete files from multiple tables.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CALL ducklake.set_option('rewrite_delete_threshold', 0);
CREATE TABLE upstream_rewrite_db_0 (key integer) USING ducklake;
CREATE TABLE upstream_rewrite_db_1 (key integer) USING ducklake;
INSERT INTO upstream_rewrite_db_0 SELECT i FROM generate_series(0, 49) AS g(i);
INSERT INTO upstream_rewrite_db_0 SELECT i FROM generate_series(50, 99) AS g(i);
INSERT INTO upstream_rewrite_db_1 SELECT i FROM generate_series(0, 49) AS g(i);
INSERT INTO upstream_rewrite_db_1 SELECT i FROM generate_series(50, 99) AS g(i);
SELECT max(r['snapshot_id']::bigint) AS rewrite_db_full_snapshot
FROM ducklake.snapshots() AS r \gset
DELETE FROM upstream_rewrite_db_0 WHERE key < 25;
DELETE FROM upstream_rewrite_db_0 WHERE key >= 25 AND key < 50;
DELETE FROM upstream_rewrite_db_1 WHERE key >= 50 AND key < 75;
DELETE FROM upstream_rewrite_db_1 WHERE key >= 75;
SELECT * FROM ducklake.rewrite_data_files();
SELECT count(*), min(key), max(key) FROM upstream_rewrite_db_0;
SELECT count(*), min(key), max(key) FROM upstream_rewrite_db_1;
SELECT count(*) = 100 AS table0_history_ok
FROM ducklake.time_travel('upstream_rewrite_db_0'::regclass, :rewrite_db_full_snapshot);
SELECT count(*) = 100 AS table1_history_ok
FROM ducklake.time_travel('upstream_rewrite_db_1'::regclass, :rewrite_db_full_snapshot);
DROP TABLE upstream_rewrite_db_0;
DROP TABLE upstream_rewrite_db_1;
