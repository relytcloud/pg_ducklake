-- Upstream: test/sql/sorted_table/merge_adjacent_sorted_reset.test
-- Skip: Physical compaction order after reset is not exposed by the PostgreSQL API.
-- Resetting sorting before compaction clears the active sort order.
CREATE TABLE upstream_merge_sort_reset (id bigint, key_one bigint, key_two text) USING ducklake;
INSERT INTO upstream_merge_sort_reset VALUES (2, 20, 'b'), (1, 10, 'a');
CALL ducklake.set_sort('upstream_merge_sort_reset'::regclass, 'key_one ASC NULLS LAST', 'key_two ASC NULLS LAST');
CALL ducklake.reset_sort('upstream_merge_sort_reset'::regclass);
SELECT * FROM ducklake.get_sort('upstream_merge_sort_reset'::regclass);
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_merge_sort_reset'::regclass);
SELECT * FROM upstream_merge_sort_reset ORDER BY id;
DROP TABLE upstream_merge_sort_reset;
