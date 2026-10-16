-- File pruning compares min/max stats inside PostgreSQL. It must agree with
-- DuckDB: byte order for strings whatever the metadata columns' collation is,
-- and value order for numerics and temporals.

-- Simulate a non-C database collation.
ALTER TABLE ducklake.ducklake_file_column_stats
    ALTER COLUMN min_value TYPE varchar COLLATE "en_US",
    ALTER COLUMN max_value TYPE varchar COLLATE "en_US";
SELECT 'a' < 'B' COLLATE "en_US" AS collation_differs_from_bytes;

CREATE TABLE sc (k text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 0, 'sc'::regclass);
INSERT INTO sc VALUES ('a'), ('B');
CREATE TABLE sc_dim (k text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 0, 'sc_dim'::regclass);
INSERT INTO sc_dim VALUES ('B');

SELECT count(*) FROM sc WHERE k = 'B';
SELECT count(*) FROM sc WHERE k = 'a';
SELECT count(*) FROM sc WHERE k >= 'B';
SELECT count(*) FROM sc WHERE k <= 'a';
SELECT count(*) FROM sc JOIN sc_dim USING (k);

DROP TABLE sc, sc_dim;
ALTER TABLE ducklake.ducklake_file_column_stats
    ALTER COLUMN min_value TYPE varchar COLLATE "default",
    ALTER COLUMN max_value TYPE varchar COLLATE "default";

-- Typed comparisons: one file per row.
CREATE TABLE sv (r real, d date, ts timestamp, ts_tz timestamptz, b bool) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 0, 'sv'::regclass);
INSERT INTO sv VALUES (1.1, '2024-01-02', '2024-01-02 10:00:00', '2024-01-02 10:00:00+00', false);
INSERT INTO sv VALUES (2.5, '2025-06-01', '2025-06-01 00:00:00', '2025-06-01 00:00:00+00', true);
SELECT count(*) FROM sv WHERE r = 1.1::real;
SELECT count(*) FROM sv WHERE r > 2.0::real;
SELECT count(*) FROM sv WHERE d >= '2025-01-01';
SELECT count(*) FROM sv WHERE d = '2024-01-02';
SELECT count(*) FROM sv WHERE ts < '2024-06-01';
SELECT count(*) FROM sv WHERE ts_tz > '2025-05-31 23:00:00+00';
SELECT count(*) FROM sv WHERE b = true;
DROP TABLE sv;
