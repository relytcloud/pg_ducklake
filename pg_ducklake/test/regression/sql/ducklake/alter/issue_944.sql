-- Upstream: test/sql/alter/issue_944.test
CREATE TABLE upstream_issue_944 (row_id integer) USING ducklake;
CREATE TABLE upstream_issue_944 (a integer) USING ducklake;
ALTER TABLE upstream_issue_944 ADD COLUMN row_id integer;
ALTER TABLE upstream_issue_944 RENAME COLUMN a TO row_id;
DROP TABLE upstream_issue_944;
