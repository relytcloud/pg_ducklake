-- Upstream: test/sql/cleanup/cleanup_old_files.test
-- Rewritten and merged files scheduled for deletion are removed by catalog cleanup.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CALL ducklake.set_option('rewrite_delete_threshold', 0);
SELECT count(*) AS scheduled_base
FROM ducklake.ducklake_files_scheduled_for_deletion \gset
CREATE TABLE upstream_cleanup_old (x integer) USING ducklake;
INSERT INTO upstream_cleanup_old VALUES (1), (2), (3);
INSERT INTO upstream_cleanup_old VALUES (4), (5);
DELETE FROM upstream_cleanup_old WHERE x <= 2;
INSERT INTO upstream_cleanup_old VALUES (6), (7);
SELECT * FROM ducklake.rewrite_data_files('upstream_cleanup_old'::regclass);
SELECT * FROM ducklake.merge_adjacent_files('upstream_cleanup_old'::regclass);
SELECT count(*) > :scheduled_base AS files_were_scheduled
FROM ducklake.ducklake_files_scheduled_for_deletion;
SELECT count(*) > 0 AS expired_snapshots FROM ducklake.expire_snapshots();
SELECT count(*) > 0 AS cleaned_files FROM ducklake.cleanup_old_files();
SELECT count(*) = :scheduled_base AS scheduled_queue_restored
FROM ducklake.ducklake_files_scheduled_for_deletion;
SELECT * FROM upstream_cleanup_old ORDER BY x;
DROP TABLE upstream_cleanup_old;
