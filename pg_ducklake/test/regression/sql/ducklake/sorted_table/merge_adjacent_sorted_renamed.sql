-- Upstream: test/sql/sorted_table/merge_adjacent_sorted_renamed.test
-- Skip: Physical sorted compaction order is not exposed by the PostgreSQL API.
-- Renaming sorted columns updates metadata used by compaction.
CREATE TABLE upstream_merge_sort_renamed (id bigint, key_one bigint, key_two text) USING ducklake;
CALL ducklake.set_sort('upstream_merge_sort_renamed'::regclass, 'key_one ASC NULLS LAST', 'key_two ASC NULLS LAST');
ALTER TABLE upstream_merge_sort_renamed RENAME COLUMN key_one TO first_key;
ALTER TABLE upstream_merge_sort_renamed RENAME COLUMN key_two TO second_key;
INSERT INTO upstream_merge_sort_renamed VALUES (2, 20, 'b'), (1, 10, 'a');
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_merge_sort_renamed'::regclass);
SELECT * FROM ducklake.get_sort('upstream_merge_sort_renamed'::regclass);
SELECT * FROM upstream_merge_sort_renamed ORDER BY id;
DROP TABLE upstream_merge_sort_renamed;
