-- Upstream: test/sql/compaction/compaction_multiple_rename_column.test
-- Repeated renames must leave compaction bound to the current column identity.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_compact_rename (id integer, user_id integer) USING ducklake;
ALTER TABLE upstream_compact_rename RENAME COLUMN user_id TO temp;
ALTER TABLE upstream_compact_rename RENAME COLUMN temp TO user_id;
INSERT INTO upstream_compact_rename VALUES (1, 100);
INSERT INTO upstream_compact_rename VALUES (2, 200);
ALTER TABLE upstream_compact_rename RENAME COLUMN user_id TO distinct_id;
SELECT * FROM ducklake.merge_adjacent_files('upstream_compact_rename'::regclass);
SELECT id, distinct_id FROM upstream_compact_rename ORDER BY id;
DROP TABLE upstream_compact_rename;
