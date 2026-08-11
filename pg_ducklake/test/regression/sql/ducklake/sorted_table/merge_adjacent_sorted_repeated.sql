-- Upstream: test/sql/sorted_table/merge_adjacent_sorted_repeated.test
-- Skip: Physical sorted compaction order is not exposed by the PostgreSQL API.
-- Repeating an identical sort order does not create conflicting active metadata.
CREATE TABLE upstream_merge_sort_repeated (id bigint, key_one bigint, key_two text) USING ducklake;
INSERT INTO upstream_merge_sort_repeated VALUES (2, 20, 'b'), (1, 10, 'a');
CALL ducklake.set_sort('upstream_merge_sort_repeated'::regclass, 'key_one ASC NULLS LAST', 'key_two ASC NULLS LAST');
CALL ducklake.set_sort('upstream_merge_sort_repeated'::regclass, 'key_one ASC NULLS LAST', 'key_two ASC NULLS LAST');
SELECT * FROM ducklake.get_sort('upstream_merge_sort_repeated'::regclass);
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_merge_sort_repeated'::regclass);
SELECT * FROM upstream_merge_sort_repeated ORDER BY id;
DROP TABLE upstream_merge_sort_repeated;
