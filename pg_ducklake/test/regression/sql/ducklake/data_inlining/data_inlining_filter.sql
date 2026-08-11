-- Upstream: test/sql/data_inlining/data_inlining_filter.test
CALL ducklake.set_option('data_inlining_row_limit', 10000);
CREATE TABLE upstream_inline_filter (
  id text NOT NULL, category text NOT NULL, created_at timestamp NOT NULL
) USING ducklake;
INSERT INTO upstream_inline_filter VALUES
 ('a_1','A','2026-01-01 00:00:01'), ('b_1','B','2026-01-01 00:00:02'),
 ('a_2','A','2026-01-01 00:00:03'), ('b_2','B','2026-01-01 00:00:04'),
 ('a_3','A','2026-01-01 00:00:05'), ('b_3','B','2026-01-01 00:00:06');
SELECT id FROM upstream_inline_filter WHERE category = 'A' ORDER BY created_at DESC LIMIT 3;
DROP TABLE upstream_inline_filter;
CALL ducklake.set_option('data_inlining_row_limit', 0);
