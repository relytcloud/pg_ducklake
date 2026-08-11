-- Upstream: test/sql/stats/variant_stats_special_chars.test
-- Quotes in variant string values must not break stats serialization.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_variant_stats_chars (v ducklake.variant) USING ducklake;
INSERT INTO upstream_variant_stats_chars VALUES ('{"text":"hello"}');
INSERT INTO upstream_variant_stats_chars VALUES ('{"text":"it''s fine"}');
SELECT count(*) FROM upstream_variant_stats_chars;
SELECT fs.variant_path, fs.shredded_type, fs.min_value, fs.max_value
FROM ducklake.ducklake_file_variant_stats fs
JOIN ducklake.ducklake_data_file df USING (data_file_id)
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_variant_stats_chars' AND t.end_snapshot IS NULL
  AND df.end_snapshot IS NULL
ORDER BY df.file_order;
SELECT count(*) = 1 AS one_column_stat,
       bool_and(cs.extra_stats IS NOT NULL) AS serialized_extra_stats
FROM ducklake.ducklake_table_column_stats cs
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_variant_stats_chars' AND t.end_snapshot IS NULL;
DROP TABLE upstream_variant_stats_chars;
