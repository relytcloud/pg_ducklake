-- Upstream: test/sql/data_inlining/inlining_issue_on_empty_inline.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_empty_seed (i integer) USING ducklake;
INSERT INTO upstream_inline_empty_seed VALUES (1), (2), (3);
SELECT * FROM ducklake.flush_inlined_data('upstream_inline_empty_seed'::regclass);
CREATE TABLE upstream_inline_empty (id integer) USING ducklake;
ALTER TABLE upstream_inline_empty ADD COLUMN k integer;
CALL ducklake.recycle_ddb();
SELECT count(*) FROM ducklake.flush_inlined_data();
SELECT count(*) FROM upstream_inline_empty;
DROP TABLE upstream_inline_empty_seed, upstream_inline_empty;
CALL ducklake.set_option('data_inlining_row_limit', 0);
