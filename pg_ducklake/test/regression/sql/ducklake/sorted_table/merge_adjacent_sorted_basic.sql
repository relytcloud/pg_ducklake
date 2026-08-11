-- Upstream: test/sql/sorted_table/merge_adjacent_sorted_basic.test
-- Skip: Physical sorted compaction order is not exposed by the PostgreSQL API.
-- Adjacent files can be compacted using multi-column sort metadata.
CREATE TABLE upstream_merge_sort_basic (id bigint, key_one bigint, key_two text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_merge_sort_basic'::regclass);
INSERT INTO upstream_merge_sort_basic VALUES (3, 30, 'c'), (1, 10, 'a');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_merge_sort_basic'::regclass);
INSERT INTO upstream_merge_sort_basic VALUES (4, 40, 'd'), (2, 20, 'b');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_merge_sort_basic'::regclass);
CALL ducklake.set_sort('upstream_merge_sort_basic'::regclass, 'key_one ASC NULLS LAST', 'key_two ASC NULLS LAST');
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_merge_sort_basic'::regclass);
SELECT * FROM ducklake.get_sort('upstream_merge_sort_basic'::regclass);
SELECT * FROM upstream_merge_sort_basic ORDER BY id;
DROP TABLE upstream_merge_sort_basic;
