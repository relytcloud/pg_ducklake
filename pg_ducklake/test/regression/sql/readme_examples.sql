-- Example from README: Your first Data Lake in PostgreSQL
CALL ducklake.set_option('data_inlining_row_limit', 0);
CREATE TABLE my_table (
    id INT,
    name TEXT,
    age INT
) USING ducklake;

INSERT INTO my_table VALUES (1, 'Alice', 25), (2, 'Bob', 30);

-- Each commit is a snapshot; capture the one before the DELETE.
SELECT max(snapshot_id) AS before_delete FROM ducklake.ducklake_snapshot \gset

DELETE FROM my_table WHERE id = 1;

SELECT * FROM my_table;

-- Time-travel back to the snapshot that still had Alice.
SELECT * FROM ducklake.time_travel('my_table'::regclass, :before_delete);

DROP TABLE my_table;

-- Example from README: Create a PostgreSQL row-store (heap) table.
CREATE TABLE row_store_table AS
SELECT i AS id, 'hello pg_ducklake' AS msg
FROM generate_series(1, 10000) AS i;

-- Create a DuckLake column-store table via ETL.
CREATE TABLE col_store_table USING ducklake AS
SELECT *
FROM row_store_table;

-- Run analytics against the converted table.
SELECT id, msg FROM col_store_table ORDER BY id LIMIT 10;

-- Verify access methods via pg_catalog
SELECT c.relname, a.amname
FROM pg_class c
JOIN pg_am a ON c.relam = a.oid
WHERE c.relname IN ('row_store_table', 'col_store_table')
ORDER BY c.relname;

DROP TABLE row_store_table;
DROP TABLE col_store_table;

-- Example from README: CREATE AS with read_csv - using titanic dataset
CREATE TABLE titanic USING ducklake AS
SELECT * FROM ducklake.read_csv('/tmp/pg_ducklake_testdata/titanic.csv');

SELECT *
FROM titanic
ORDER BY "PassengerId"
LIMIT 10;

-- Verify access method for titanic table
SELECT c.relname, a.amname
FROM pg_class c
JOIN pg_am a ON c.relam = a.oid
WHERE c.relname = 'titanic';

DROP TABLE titanic;

-- Example from README: Data Inlining
-- Small inserts are buffered in the catalog and flushed to Parquet in bulk.
CALL ducklake.set_option('data_inlining_row_limit', 100);

CREATE TABLE events (id INT, kind TEXT) USING ducklake;

-- Inlined rows are immediately visible to every reader.
INSERT INTO events VALUES (1, 'login'), (2, 'click');
SELECT * FROM events ORDER BY id;

-- Flush inlined rows out to Parquet (the background worker does this too).
SELECT * FROM ducklake.flush_inlined_data('events'::regclass);

DROP TABLE events;
CALL ducklake.set_option('data_inlining_row_limit', 0);

-- Example from README: Sorted Tables & Bucket Partitioning
CREATE TABLE measurements (
    device_id INT,
    ts TIMESTAMP,
    reading DOUBLE PRECISION
) USING ducklake;

-- Distribute files by a hash bucket on device_id and by month of the timestamp.
CALL ducklake.set_partition('measurements'::regclass, 'bucket(4, device_id)', 'month(ts)');
SELECT * FROM ducklake.get_partition('measurements'::regclass);

-- Keep rows ordered within each file for fast range scans and compression.
CREATE INDEX ON measurements USING ducklake_sorted (device_id, ts);
SELECT * FROM ducklake.get_sort('measurements'::regclass);

INSERT INTO measurements VALUES
    (1, '2024-01-15 10:00', 21.5),
    (2, '2024-02-20 11:00', 22.1),
    (1, '2024-01-18 09:30', 19.8);
SELECT * FROM measurements ORDER BY device_id, ts;

DROP TABLE measurements;

-- Example from README: Maintenance
CREATE TABLE logs (id INT, msg TEXT) USING ducklake;

-- Several small inserts create several small Parquet files.
INSERT INTO logs VALUES (1, 'a');
INSERT INTO logs VALUES (2, 'b');
INSERT INTO logs VALUES (3, 'c');

-- Compact small adjacent files into fewer, larger ones.
SELECT * FROM ducklake.merge_adjacent_files('logs'::regclass);

-- Expire snapshots beyond a retention window, then delete their files.
CALL ducklake.set_option('expire_older_than', '7 days');
SELECT count(*) >= 0 AS ok FROM ducklake.expire_snapshots();
SELECT count(*) >= 0 AS ok FROM ducklake.cleanup_old_files();

SELECT * FROM logs ORDER BY id;

DROP TABLE logs;
