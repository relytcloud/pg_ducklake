-- Upstream: test/sql/compaction/compaction_full_file_delete.test
-- A fully deleted file must not reappear when adjacent live files are compacted.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_compact_full_delete (id integer) USING ducklake;
INSERT INTO upstream_compact_full_delete VALUES (1);
DELETE FROM upstream_compact_full_delete WHERE id = 1;
INSERT INTO upstream_compact_full_delete VALUES (2);
INSERT INTO upstream_compact_full_delete VALUES (3);
SELECT * FROM ducklake.merge_adjacent_files('upstream_compact_full_delete'::regclass);
SELECT * FROM upstream_compact_full_delete ORDER BY id;
DROP TABLE upstream_compact_full_delete;
