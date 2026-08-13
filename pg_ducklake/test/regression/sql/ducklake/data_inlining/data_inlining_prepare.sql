-- Upstream: test/sql/data_inlining/data_inlining_prepare.test
CALL ducklake.set_option('data_inlining_row_limit', 1000);
CREATE TABLE upstream_inline_prepare (id integer) USING ducklake;
INSERT INTO upstream_inline_prepare VALUES (1), (2);
PREPARE upstream_flush AS SELECT count(*) FROM ducklake.flush_inlined_data('upstream_inline_prepare'::regclass);
EXECUTE upstream_flush;
DEALLOCATE upstream_flush;
SELECT * FROM upstream_inline_prepare ORDER BY id;
DROP TABLE upstream_inline_prepare;
CALL ducklake.set_option('data_inlining_row_limit', 0);
