-- Upstream: test/sql/compaction/compaction_alter_table.test
-- Compaction must preserve rows across add/drop/re-add column schema evolution.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_compact_alter (id integer, i integer) USING ducklake;
INSERT INTO upstream_compact_alter VALUES (1, 10);
INSERT INTO upstream_compact_alter VALUES (2, 20);
ALTER TABLE upstream_compact_alter ADD COLUMN j integer;
INSERT INTO upstream_compact_alter VALUES (3, 30, 300);
INSERT INTO upstream_compact_alter VALUES (4, 40, 400);
ALTER TABLE upstream_compact_alter DROP COLUMN i;
INSERT INTO upstream_compact_alter VALUES (5, 500);
INSERT INTO upstream_compact_alter VALUES (6, 600);
ALTER TABLE upstream_compact_alter ADD COLUMN i text;
INSERT INTO upstream_compact_alter VALUES (7, 700, 'hello');
INSERT INTO upstream_compact_alter VALUES (8, 800, 'world');
SELECT * FROM ducklake.merge_adjacent_files('upstream_compact_alter'::regclass);
SELECT * FROM upstream_compact_alter ORDER BY id;
DROP TABLE upstream_compact_alter;
