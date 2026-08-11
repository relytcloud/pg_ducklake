-- Upstream: test/sql/sorted_table/data_inlining_flush_sorted_renamed.test
-- Renaming sort columns updates expressions used by later flushes.
CREATE TABLE upstream_flush_sort_renamed (id integer, key_one integer, key_two text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_flush_sort_renamed'::regclass);
CALL ducklake.set_sort('upstream_flush_sort_renamed'::regclass, 'key_one ASC NULLS LAST', 'key_two ASC NULLS LAST');
ALTER TABLE upstream_flush_sort_renamed RENAME COLUMN key_one TO first_key;
ALTER TABLE upstream_flush_sort_renamed RENAME COLUMN key_two TO second_key;
INSERT INTO upstream_flush_sort_renamed VALUES (2, 20, 'b'), (1, 10, 'a');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_flush_sort_renamed'::regclass);
SELECT * FROM ducklake.get_sort('upstream_flush_sort_renamed'::regclass);
SELECT * FROM upstream_flush_sort_renamed ORDER BY id;
DROP TABLE upstream_flush_sort_renamed;
