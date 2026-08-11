-- Upstream: test/sql/partitioning/year_month_day.test
-- Skip: Physical year/month partition directory layout is not exposed by the PostgreSQL API.
-- Year and month transforms partition timestamp data without changing results.
SET datestyle TO ISO;
CREATE TABLE upstream_year_month_day (id integer, ts timestamp, val text) USING ducklake;
CALL ducklake.set_partition('upstream_year_month_day'::regclass, 'year(ts)', 'month(ts)');
INSERT INTO upstream_year_month_day VALUES
 (1, TIMESTAMP '2020-01-15', 'jan'), (2, TIMESTAMP '2020-02-15', 'feb'),
 (3, TIMESTAMP '2021-01-15', 'next');
SELECT * FROM ducklake.get_partition('upstream_year_month_day'::regclass);
SELECT fpv.partition_key_index, fpv.partition_value, count(*) AS files
FROM ducklake.ducklake_file_partition_value fpv
JOIN ducklake.ducklake_data_file f USING (data_file_id)
JOIN ducklake.ducklake_table t USING (table_id)
WHERE t.table_name = 'upstream_year_month_day'
  AND t.end_snapshot IS NULL AND f.end_snapshot IS NULL
GROUP BY fpv.partition_key_index, fpv.partition_value
ORDER BY fpv.partition_key_index, fpv.partition_value;
SELECT extract(year FROM ts)::integer AS year, count(*), min(val), max(val)
FROM upstream_year_month_day GROUP BY 1 ORDER BY 1;
DROP TABLE upstream_year_month_day;
