-- Test partitioning: CREATE INDEX USING ducklake_partitioned + set_partition/reset_partition

-- Disable data inlining so inserts create actual parquet files
CALL ducklake.set_option('data_inlining_row_limit', 0);

-- ============================================================
-- CREATE INDEX USING ducklake_partitioned
-- ============================================================

-- 1. Basic single-column partition via CREATE INDEX
CREATE TABLE pidx_basic (id int, category text, val int) USING ducklake;

CREATE INDEX pidx_basic_idx ON pidx_basic USING ducklake_partitioned (category);

SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'pidx_basic_idx';

SELECT * FROM ducklake.get_partition('pidx_basic'::regclass);

-- 2. Multi-key with transforms
CREATE TABLE pidx_ts (id int, ts timestamp, val int) USING ducklake;

CREATE INDEX pidx_ts_idx ON pidx_ts USING ducklake_partitioned (ducklake.year(ts), ducklake.month(ts));

SELECT * FROM ducklake.get_partition('pidx_ts'::regclass);

-- 3. Mixed column + transform
CREATE TABLE pidx_mixed (id int, region text, created_at timestamp) USING ducklake;

CREATE INDEX pidx_mixed_idx ON pidx_mixed USING ducklake_partitioned (region, ducklake.year(created_at));

SELECT * FROM ducklake.get_partition('pidx_mixed'::regclass);

-- 4. DROP INDEX resets partition and removes pg_class entry
DROP INDEX pidx_basic_idx;

SELECT c.relname FROM pg_class c WHERE c.relname = 'pidx_basic_idx';

SELECT * FROM ducklake.get_partition('pidx_basic'::regclass);

-- 5. Error: non-DuckLake table
CREATE TABLE heap_part_table (id int);
CREATE INDEX ON heap_part_table USING ducklake_partitioned (id);
DROP TABLE heap_part_table;

-- 6. Error: ASC/DESC on partition key
CREATE INDEX ON pidx_basic USING ducklake_partitioned (category DESC);

-- 7. Error: NULLS FIRST/LAST on partition key
CREATE INDEX ON pidx_basic USING ducklake_partitioned (category NULLS FIRST);

-- 8. Error: CONCURRENTLY
CREATE INDEX CONCURRENTLY ON pidx_basic USING ducklake_partitioned (category);

-- 9. Error: UNIQUE
CREATE UNIQUE INDEX ON pidx_basic USING ducklake_partitioned (category);

-- ============================================================
-- set_partition / reset_partition (procedure-based)
-- ============================================================

-- 10. Basic column partition + metadata verification
CREATE TABLE part_basic (id int, category text, val int) USING ducklake;

CALL ducklake.set_partition('part_basic'::regclass, 'category');

-- set_partition syncs a ducklake_partitioned index to pg_class
SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
JOIN pg_index i ON i.indexrelid = c.oid
WHERE i.indrelid = 'part_basic'::regclass AND am.amname = 'ducklake_partitioned';

SELECT * FROM ducklake.get_partition('part_basic'::regclass);

INSERT INTO part_basic VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30);
SELECT * FROM part_basic ORDER BY id;

-- Verify file partition values
SELECT fpv.partition_key_index, fpv.partition_value
FROM ducklake.ducklake_file_partition_value fpv
JOIN ducklake.ducklake_table t ON fpv.table_id = t.table_id
WHERE t.table_name = 'part_basic'
ORDER BY fpv.partition_key_index, fpv.partition_value;

-- 11. Transform partition (year, month) + metadata verification
CREATE TABLE part_ts (id int, ts timestamp, val int) USING ducklake;

CALL ducklake.set_partition('part_ts'::regclass, 'year(ts)', 'month(ts)');

SELECT * FROM ducklake.get_partition('part_ts'::regclass);

INSERT INTO part_ts VALUES (1, '2024-01-15', 10), (2, '2024-06-20', 20);
SELECT * FROM part_ts ORDER BY id;

-- 12. Multi-key partition
CREATE TABLE part_multi (a text, b text, c int) USING ducklake;

CALL ducklake.set_partition('part_multi'::regclass, 'a', 'b');

SELECT * FROM ducklake.get_partition('part_multi'::regclass);

INSERT INTO part_multi VALUES ('x', 'y', 1), ('x', 'z', 2);
SELECT * FROM part_multi ORDER BY c;

-- 13. Set partition on table with existing data
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

-- 14. set_partition replaces an existing ducklake_partitioned index
CREATE INDEX part_basic_named ON part_basic USING ducklake_partitioned (id);
SELECT * FROM ducklake.get_partition('part_basic'::regclass);

CALL ducklake.set_partition('part_basic'::regclass, 'category');
SELECT * FROM ducklake.get_partition('part_basic'::regclass);

-- verify old named index is gone and new auto-named one exists
SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
JOIN pg_index i ON i.indexrelid = c.oid
WHERE i.indrelid = 'part_basic'::regclass AND am.amname = 'ducklake_partitioned';

-- 15. Reset partition + metadata verification
CALL ducklake.reset_partition('part_basic'::regclass);

-- Verify no active partition columns remain
SELECT * FROM ducklake.get_partition('part_basic'::regclass);

-- Verify index is gone after reset
SELECT c.relname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
JOIN pg_index i ON i.indexrelid = c.oid
WHERE i.indrelid = 'part_basic'::regclass AND am.amname = 'ducklake_partitioned';

INSERT INTO part_basic VALUES (4, 'c', 40);
SELECT * FROM part_basic ORDER BY id;

-- 16. Error: non-ducklake table
CREATE TABLE heap_table (id int);
CALL ducklake.set_partition('heap_table'::regclass, 'id');
DROP TABLE heap_table;

-- Cleanup (must happen before any error-inducing DuckDB queries)
DROP TABLE pidx_basic;
DROP TABLE pidx_ts;
DROP TABLE pidx_mixed;
DROP TABLE part_basic;
DROP TABLE part_ts;
DROP TABLE part_multi;
DROP TABLE part_existing;
