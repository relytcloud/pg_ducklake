-- Upstream: test/sql/compaction/small_insert_compaction.test
-- Catalog-wide compaction merges one table's small inserts without changing another table.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_small_compact (i integer) USING ducklake;
CREATE TABLE upstream_small_compact_other (i integer) USING ducklake;
INSERT INTO upstream_small_compact VALUES (1);
INSERT INTO upstream_small_compact_other VALUES (42);
INSERT INTO upstream_small_compact VALUES (2);
INSERT INTO upstream_small_compact VALUES (3);
INSERT INTO upstream_small_compact VALUES (4);
INSERT INTO upstream_small_compact VALUES (5);
SELECT max(r['snapshot_id']::bigint) AS before_compaction
FROM ducklake.snapshots() AS r \gset
SELECT * FROM ducklake.merge_adjacent_files();
SELECT count(*) = 1 AS target_one_file
FROM ducklake.list_files('upstream_small_compact'::regclass);
SELECT count(*) = 1 AS other_one_file
FROM ducklake.list_files('upstream_small_compact_other'::regclass);
SELECT count(*) = 5 AS historical_rows_preserved
FROM ducklake.time_travel('upstream_small_compact'::regclass, :before_compaction);
SELECT ducklake.rowid(), i FROM upstream_small_compact ORDER BY i;
SELECT * FROM upstream_small_compact_other;
DROP TABLE upstream_small_compact;
DROP TABLE upstream_small_compact_other;
