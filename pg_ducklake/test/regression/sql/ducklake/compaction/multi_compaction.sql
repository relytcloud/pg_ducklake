-- Upstream: test/sql/compaction/multi_compaction.test
-- Repeated catalog-wide compaction is idempotent and preserves row lineage.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_multi_compact (i integer) USING ducklake;
INSERT INTO upstream_multi_compact VALUES (1);
INSERT INTO upstream_multi_compact VALUES (2);
INSERT INTO upstream_multi_compact VALUES (3);
SELECT max(r['snapshot_id']::bigint) AS before_first_compaction
FROM ducklake.snapshots() AS r \gset
SELECT * FROM ducklake.merge_adjacent_files('upstream_multi_compact'::regclass);
INSERT INTO upstream_multi_compact VALUES (4);
INSERT INTO upstream_multi_compact VALUES (5);
SELECT * FROM ducklake.merge_adjacent_files('upstream_multi_compact'::regclass);
SELECT * FROM ducklake.merge_adjacent_files('upstream_multi_compact'::regclass);
SELECT count(*) = 1 AS one_live_file
FROM ducklake.list_files('upstream_multi_compact'::regclass);
SELECT ducklake.rowid(), i FROM upstream_multi_compact ORDER BY i;
SELECT count(*) AS original_rows
FROM ducklake.time_travel('upstream_multi_compact'::regclass, :before_first_compaction);
DROP TABLE upstream_multi_compact;
