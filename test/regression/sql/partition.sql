-- Test ducklake.set_partition() and ducklake.reset_partition()

-- Disable data inlining so inserts create actual parquet files
CALL ducklake.set_option('data_inlining_row_limit', 0);

-- 1. Basic column partition + metadata verification
CREATE TABLE part_basic (id int, category text, val int) USING ducklake;

CALL ducklake.set_partition('part_basic'::regclass, 'category');

SELECT * FROM ducklake.get_partition('part_basic'::regclass);

INSERT INTO part_basic VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30);
SELECT * FROM part_basic ORDER BY id;

-- Verify file partition values
SELECT fpv.partition_key_index, fpv.partition_value
FROM ducklake.ducklake_file_partition_value fpv
JOIN ducklake.ducklake_table t ON fpv.table_id = t.table_id
WHERE t.table_name = 'part_basic'
ORDER BY fpv.partition_key_index, fpv.partition_value;

-- 2. Transform partition (year, month) + metadata verification
CREATE TABLE part_ts (id int, ts timestamp, val int) USING ducklake;

CALL ducklake.set_partition('part_ts'::regclass, 'year(ts)', 'month(ts)');

SELECT * FROM ducklake.get_partition('part_ts'::regclass);

INSERT INTO part_ts VALUES (1, '2024-01-15', 10), (2, '2024-06-20', 20);
SELECT * FROM part_ts ORDER BY id;

-- 3. Multi-key partition
CREATE TABLE part_multi (a text, b text, c int) USING ducklake;

CALL ducklake.set_partition('part_multi'::regclass, 'a', 'b');

SELECT * FROM ducklake.get_partition('part_multi'::regclass);

INSERT INTO part_multi VALUES ('x', 'y', 1), ('x', 'z', 2);
SELECT * FROM part_multi ORDER BY c;

-- 4. Set partition on table with existing data
CREATE TABLE part_existing (id int, grp text, val int) USING ducklake;
INSERT INTO part_existing VALUES (1, 'a', 10), (2, 'b', 20);
SELECT * FROM part_existing ORDER BY id;

-- Set partition after data already exists
CALL ducklake.set_partition('part_existing'::regclass, 'grp');

-- Existing data should still be queryable
SELECT * FROM part_existing ORDER BY id;

-- New inserts go into partitioned files
INSERT INTO part_existing VALUES (3, 'a', 30), (4, 'c', 40);
SELECT * FROM part_existing ORDER BY id;

-- 5. Reset partition + metadata verification
CALL ducklake.reset_partition('part_basic'::regclass);

-- Verify no active partition columns remain
SELECT * FROM ducklake.get_partition('part_basic'::regclass);

INSERT INTO part_basic VALUES (4, 'c', 40);
SELECT * FROM part_basic ORDER BY id;

-- 6. Error: non-ducklake table
CREATE TABLE heap_table (id int);
CALL ducklake.set_partition('heap_table'::regclass, 'id');
DROP TABLE heap_table;

-- Cleanup (must happen before any error-inducing DuckDB queries)
DROP TABLE part_basic;
DROP TABLE part_ts;
DROP TABLE part_multi;
DROP TABLE part_existing;
