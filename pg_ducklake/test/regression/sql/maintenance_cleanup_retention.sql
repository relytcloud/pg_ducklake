-- The background worker's cleanup must keep files that were unreferenced
-- for less than delete_older_than, so older snapshots can still read them.
CALL ducklake.set_option('data_inlining_row_limit', 0);

CREATE TABLE mcr (a int) USING ducklake;
INSERT INTO mcr VALUES (1);
INSERT INTO mcr VALUES (2);
INSERT INTO mcr VALUES (3);

SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('mcr'::regclass);
SELECT count(*) AS scheduled FROM ducklake.ducklake_files_scheduled_for_deletion \gset
SELECT :scheduled >= 3 AS files_scheduled;

ALTER SYSTEM SET ducklake.maintenance_naptime = 1;
ALTER SYSTEM SET ducklake.maintenance_cleanup_old_files = on;
ALTER SYSTEM SET ducklake.maintenance_enabled = on;
SELECT pg_reload_conf();

-- Let the launcher spawn a worker and run a full maintenance cycle.
SELECT pg_sleep(4);

SELECT count(*) = :scheduled AS files_kept FROM ducklake.ducklake_files_scheduled_for_deletion;

ALTER SYSTEM RESET ducklake.maintenance_enabled;
ALTER SYSTEM RESET ducklake.maintenance_cleanup_old_files;
ALTER SYSTEM RESET ducklake.maintenance_naptime;
SELECT pg_reload_conf();
SELECT pg_sleep(2);

SELECT * FROM mcr ORDER BY a;
DROP TABLE mcr;
SELECT count(*) >= :scheduled AS cleaned FROM ducklake.cleanup_old_files();
