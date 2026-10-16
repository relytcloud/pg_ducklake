-- File pruning on string stats must use byte order, as DuckDB does, whatever
-- the metadata columns' collation is. Simulate a non-C database collation.
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
