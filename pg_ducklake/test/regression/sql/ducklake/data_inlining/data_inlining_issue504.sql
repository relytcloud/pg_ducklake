-- Upstream: test/sql/data_inlining/data_inlining_issue504.test
CALL ducklake.set_option('data_inlining_row_limit', 10000);
CREATE TABLE upstream_inline_issue504 (created_at timestamptz, version integer) USING ducklake;
INSERT INTO upstream_inline_issue504 VALUES ('2026-01-01 12:00:00+00', 1);
SELECT version FROM upstream_inline_issue504 ORDER BY created_at DESC LIMIT 1;
DROP TABLE upstream_inline_issue504;
CALL ducklake.set_option('data_inlining_row_limit', 0);
