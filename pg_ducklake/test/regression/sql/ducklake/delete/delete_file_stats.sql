-- Upstream: test/sql/delete/delete_file_stats.test
-- Delete files must record nonempty file and footer size metadata.

CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_delete_file_stats USING ducklake AS
SELECT i AS id FROM generate_series(0, 1999) AS g(i);
INSERT INTO upstream_delete_file_stats
SELECT i FROM generate_series(15000, 15999) AS g(i);

DELETE FROM upstream_delete_file_stats WHERE id % 2 = 0;

SELECT count(*) > 0 AS has_delete_files,
       bool_and(file_size_bytes > 0) AS positive_file_sizes,
       bool_and(footer_size >= 0) AS valid_footers
FROM ducklake.ducklake_delete_file f
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_delete_file_stats'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL;

DROP TABLE upstream_delete_file_stats;
