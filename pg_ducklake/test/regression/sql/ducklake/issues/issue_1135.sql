-- Upstream: test/sql/issues/issue_1135.test
-- Filters on an added column with a default must include pre-alter rows.

CREATE TABLE upstream_issue_1135 (a integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_issue_1135'::regclass);
INSERT INTO upstream_issue_1135 SELECT g FROM generate_series(0, 9) AS g;
SELECT * FROM ducklake.flush_inlined_data('upstream_issue_1135'::regclass);
ALTER TABLE upstream_issue_1135 ADD COLUMN b integer DEFAULT 42;
SELECT count(*) FROM upstream_issue_1135 WHERE b = 42;
DROP TABLE upstream_issue_1135;
DELETE FROM ducklake.ducklake_metadata
WHERE key = 'data_inlining_row_limit' AND scope = 'table';
