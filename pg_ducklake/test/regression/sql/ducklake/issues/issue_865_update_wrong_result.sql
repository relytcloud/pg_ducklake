-- Upstream: test/sql/issues/issue_865_update_wrong_result.test
-- A file with file-backed and inlined deletions must not duplicate rows on UPDATE.

CREATE TABLE upstream_issue_865 USING ducklake AS
SELECT g AS id, 'original'::text AS val FROM generate_series(0, 99) AS g;
CALL ducklake.set_option('data_inlining_row_limit', 10, 'upstream_issue_865'::regclass);
DELETE FROM upstream_issue_865 WHERE id >= 80;
SELECT count(*) FROM upstream_issue_865;
DELETE FROM upstream_issue_865 WHERE id >= 75;
SELECT count(*) FROM upstream_issue_865;
UPDATE upstream_issue_865 SET val = 'updated' WHERE id < 20;
SELECT count(*) FROM upstream_issue_865;
SELECT count(*) FROM upstream_issue_865 WHERE val = 'updated';
SELECT count(*)
FROM (SELECT id FROM upstream_issue_865 GROUP BY id HAVING count(*) > 1) AS duplicates;
DROP TABLE upstream_issue_865;
