-- Upstream: test/sql/data_inlining/inlining_global_options.test
CALL ducklake.set_option('data_inlining_row_limit', 2);
CREATE SCHEMA upstream_inline_opts;
CREATE TABLE upstream_inline_opts_a (k text, v text) USING ducklake;
CREATE TABLE upstream_inline_opts_b (k text, v text) USING ducklake;
CREATE TABLE upstream_inline_opts.a (k text, v text) USING ducklake;
CREATE TABLE upstream_inline_opts.b (k text, v text) USING ducklake;
INSERT INTO upstream_inline_opts_a VALUES ('foo','bar');
INSERT INTO upstream_inline_opts_a VALUES ('baz','qux');
INSERT INTO upstream_inline_opts_b VALUES ('foo','bar');
INSERT INTO upstream_inline_opts_b VALUES ('baz','qux');
INSERT INTO upstream_inline_opts.a VALUES ('foo','bar');
INSERT INTO upstream_inline_opts.a VALUES ('baz','qux');
INSERT INTO upstream_inline_opts.b VALUES ('foo','bar');
INSERT INTO upstream_inline_opts.b VALUES ('baz','qux');
CALL ducklake.set_option('auto_compact', false, 'public'::regnamespace);
CALL ducklake.set_option('auto_compact', false, 'upstream_inline_opts'::regnamespace);
CALL ducklake.set_option('auto_compact', true, 'upstream_inline_opts_b'::regclass);
SELECT * FROM ducklake.flush_inlined_data();
SELECT count(*) AS files_a
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_opts_a' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
SELECT count(*) AS files_b
FROM ducklake.ducklake_data_file f JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_inline_opts_b' AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;
CALL ducklake.set_option('auto_compact', true, 'upstream_inline_opts'::regnamespace);
CALL ducklake.set_option('auto_compact', true, 'public'::regnamespace);
SELECT * FROM ducklake.flush_inlined_data();
SELECT k, v FROM upstream_inline_opts_a UNION ALL SELECT k, v FROM upstream_inline_opts_b
UNION ALL SELECT k, v FROM upstream_inline_opts.a UNION ALL SELECT k, v FROM upstream_inline_opts.b
ORDER BY k, v;
DROP TABLE upstream_inline_opts_a, upstream_inline_opts_b, upstream_inline_opts.a, upstream_inline_opts.b;
DROP SCHEMA upstream_inline_opts;
CALL ducklake.set_option('data_inlining_row_limit', 0);
