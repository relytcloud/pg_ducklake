-- Upstream: test/sql/deletion_inlining/test_deletion_inlining_compaction.test
CALL ducklake.set_option('data_inlining_row_limit', 2);
CREATE TABLE upstream_delete_compact USING ducklake AS SELECT g AS a FROM generate_series(0, 49) g;
SELECT max(snapshot_id) AS vorig FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_delete_compact WHERE a = 25;
SELECT max(snapshot_id) AS vdelete FROM ducklake.ducklake_snapshot \gset
INSERT INTO upstream_delete_compact VALUES (51),(52),(53);
INSERT INTO upstream_delete_compact VALUES (54),(55),(56);
SELECT * FROM ducklake.merge_adjacent_files('upstream_delete_compact'::regclass);
SELECT * FROM ducklake.flush_inlined_data('upstream_delete_compact'::regclass);
CALL ducklake.set_option('rewrite_delete_threshold', 0.0);
SELECT * FROM ducklake.rewrite_data_files('upstream_delete_compact'::regclass);
SELECT count(*), sum(a) FROM upstream_delete_compact;
SELECT count(*), sum(r['a']::integer)
FROM ducklake.time_travel('upstream_delete_compact'::regclass, :vorig) AS r;
SELECT count(*), sum(r['a']::integer)
FROM ducklake.time_travel('upstream_delete_compact'::regclass, :vdelete) AS r;
CALL ducklake.set_option('rewrite_delete_threshold', 0.2);
DROP TABLE upstream_delete_compact;
CALL ducklake.set_option('data_inlining_row_limit', 0);
