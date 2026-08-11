-- Upstream: test/sql/partitioning/multi_key_merge.test
-- Merge adjacent files on a table partitioned by multiple keys.
CREATE TABLE upstream_multi_key_merge (sale_id integer, product_name text, country text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_multi_key_merge'::regclass);
CALL ducklake.set_partition('upstream_multi_key_merge'::regclass, 'product_name', 'country');
INSERT INTO upstream_multi_key_merge VALUES (1, 'Laptop', 'UK'), (2, 'Mouse', 'GR');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_multi_key_merge'::regclass);
INSERT INTO upstream_multi_key_merge VALUES (3, 'Monitor', 'ES'), (4, 'Laptop', 'UK');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_multi_key_merge'::regclass);
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_multi_key_merge'::regclass);
SELECT * FROM upstream_multi_key_merge ORDER BY sale_id;
DROP TABLE upstream_multi_key_merge;
