-- Upstream: test/sql/compaction/repro_merge_adjacent_zero_output.test
-- Skip: PostgreSQL does not materialize a Parquet file for a zero-row INSERT, so the four-empty-file invariant is unobservable.
-- The mapped smoke check still proves an empty logical table has no phantom rows.
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE upstream_compact_empty (id integer, payload text) USING ducklake;
INSERT INTO upstream_compact_empty SELECT i, 'x' FROM generate_series(1, 0) AS g(i);
SELECT * FROM ducklake.merge_adjacent_files('upstream_compact_empty'::regclass);
SELECT count(*) FROM upstream_compact_empty;
DROP TABLE upstream_compact_empty;
