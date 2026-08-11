-- Upstream: test/sql/audit/test_base_audit.test
-- Map the supported author/message API; extra_info is not exposed by pg_ducklake.

CREATE TABLE upstream_audit (a integer, b text) USING ducklake;
BEGIN;
INSERT INTO upstream_audit VALUES (1, 'Pedro');
CALL ducklake.set_commit_message('Pedro', 'Inserting myself');
COMMIT;
SELECT max(r['snapshot_id']::bigint) AS saudit FROM ducklake.snapshots() AS r \gset
SELECT r['author']::text, r['commit_message']::text
FROM ducklake.snapshots() AS r
WHERE r['snapshot_id']::bigint = :saudit;

BEGIN;
INSERT INTO upstream_audit VALUES (2, 'Hannes');
CALL ducklake.set_commit_message('Pedro', 'rolled back');
ROLLBACK;
INSERT INTO upstream_audit VALUES (2, 'Hannes');
SELECT max(r['snapshot_id']::bigint) AS srollback FROM ducklake.snapshots() AS r \gset
SELECT r['author'] IS NULL AS no_author, r['commit_message'] IS NULL AS no_message
FROM ducklake.snapshots() AS r
WHERE r['snapshot_id']::bigint = :srollback;

BEGIN;
INSERT INTO upstream_audit VALUES (3, 'Teddy');
CALL ducklake.set_commit_message('Pedro', 'first message');
CALL ducklake.set_commit_message('Mark', 'Inserting Teddy');
COMMIT;
SELECT max(r['snapshot_id']::bigint) AS slast FROM ducklake.snapshots() AS r \gset
SELECT r['author']::text, r['commit_message']::text
FROM ducklake.snapshots() AS r
WHERE r['snapshot_id']::bigint = :slast;
DROP TABLE upstream_audit;
