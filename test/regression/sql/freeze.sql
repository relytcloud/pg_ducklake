-- Test ducklake.freeze() — export metadata to a standalone .ducklake file

-- Disable data inlining so we get actual Parquet files
CALL ducklake.set_option('data_inlining_row_limit', 0);

-- Setup: create a table with data
CREATE TABLE freeze_test (id int, name text) USING ducklake;
INSERT INTO freeze_test VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM freeze_test ORDER BY id;

-- Test 1: Basic freeze �� attach as a real DuckLake and query
CALL ducklake.freeze('/tmp/test_freeze.ducklake');

SET client_min_messages = warning;
SELECT duckdb.raw_query($$ ATTACH 'ducklake:/tmp/test_freeze.ducklake' AS __frozen__ $$);
RESET client_min_messages;

SELECT * FROM duckdb.query($$ SELECT * FROM __frozen__.public.freeze_test ORDER BY id $$);

SET client_min_messages = warning;
SELECT duckdb.raw_query($$ DETACH __frozen__ $$);
RESET client_min_messages;

-- Test 2: Freeze with custom data_path — verify metadata was updated
CALL ducklake.freeze('/tmp/test_freeze_custom.ducklake', 's3://my-bucket/data/');

SET client_min_messages = warning;
SELECT duckdb.raw_query($$ ATTACH '/tmp/test_freeze_custom.ducklake' AS __frozen_meta__ $$);
RESET client_min_messages;

SELECT * FROM duckdb.query($$ SELECT value AS data_path FROM __frozen_meta__.main.ducklake_metadata WHERE key = 'data_path' $$);

SET client_min_messages = warning;
SELECT duckdb.raw_query($$ DETACH __frozen_meta__ $$);
RESET client_min_messages;

-- Test 3: Freeze with inlined data — flush must be called separately
CALL ducklake.set_option('data_inlining_row_limit', 100);
CREATE TABLE freeze_inlined (a int) USING ducklake;
INSERT INTO freeze_inlined VALUES (1), (2), (3);
SELECT * FROM freeze_inlined ORDER BY a;

-- Confirm data is inlined
SELECT count(*) > 0 AS has_inlined
FROM ducklake.ducklake_inlined_data_tables
WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table
                  WHERE table_name = 'freeze_inlined' AND end_snapshot IS NULL);

-- Flush inlined data as a separate statement before freeze
CALL ducklake.flush_inlined_data();
CALL ducklake.freeze('/tmp/test_freeze_inlined.ducklake');

-- Attach as real DuckLake and query — proves flush materialized the data
SET client_min_messages = warning;
SELECT duckdb.raw_query($$ ATTACH 'ducklake:/tmp/test_freeze_inlined.ducklake' AS __frozen__ $$);
RESET client_min_messages;

SELECT * FROM duckdb.query($$ SELECT * FROM __frozen__.public.freeze_inlined ORDER BY a $$);

SET client_min_messages = warning;
SELECT duckdb.raw_query($$ DETACH __frozen__ $$);
RESET client_min_messages;

-- Test 4: Error — NULL output_path
CALL ducklake.freeze(NULL);

-- Cleanup
DROP TABLE freeze_test;
DROP TABLE freeze_inlined;
CALL ducklake.set_option('data_inlining_row_limit', 0);
\! rm -f /tmp/test_freeze.ducklake /tmp/test_freeze_custom.ducklake /tmp/test_freeze_inlined.ducklake
