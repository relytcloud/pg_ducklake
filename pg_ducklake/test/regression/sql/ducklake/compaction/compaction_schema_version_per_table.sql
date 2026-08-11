-- Upstream: test/sql/compaction/compaction_schema_version_per_table.test
-- Compaction uses each table's schema version, not an unrelated catalog version.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_compact_schema_a (a integer) USING ducklake;
INSERT INTO upstream_compact_schema_a VALUES (1), (2);
CREATE TABLE upstream_compact_schema_b (x integer) USING ducklake;
INSERT INTO upstream_compact_schema_b VALUES (100), (200);
ALTER TABLE upstream_compact_schema_b ADD COLUMN y text;
CREATE VIEW upstream_compact_schema_view AS SELECT * FROM upstream_compact_schema_b;
DROP VIEW upstream_compact_schema_view;
INSERT INTO upstream_compact_schema_a VALUES (3);
INSERT INTO upstream_compact_schema_a VALUES (4);
SELECT * FROM ducklake.merge_adjacent_files('upstream_compact_schema_a'::regclass);
SELECT count(*) = 1 AS table_a_has_one_live_file
FROM ducklake.list_files('upstream_compact_schema_a'::regclass);
SELECT * FROM upstream_compact_schema_a ORDER BY a;
SELECT * FROM upstream_compact_schema_b ORDER BY x;
DROP TABLE upstream_compact_schema_a;
DROP TABLE upstream_compact_schema_b;
