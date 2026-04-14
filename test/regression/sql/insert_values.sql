-- Test for INSERT ... VALUES direct insert optimization
-- Regression test for #176: direct insert must create ducklake_table_stats

SET ducklake.enable_direct_insert = true;
CALL ducklake.set_option('data_inlining_row_limit', 1000);

-- Create table and ensure inlined data table exists
CREATE TABLE insert_values_t (id int, val text) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('insert_values_t'::regclass);

-- First direct insert should create ducklake_table_stats row (#176)
INSERT INTO insert_values_t VALUES (1, 'one');

SELECT record_count, next_row_id
FROM ducklake.ducklake_table_stats
JOIN ducklake.ducklake_table dt USING (table_id)
WHERE dt.table_name = 'insert_values_t' AND dt.end_snapshot IS NULL;

-- Second insert should increment stats
INSERT INTO insert_values_t VALUES (2, 'two'), (3, 'three');

SELECT record_count, next_row_id
FROM ducklake.ducklake_table_stats
JOIN ducklake.ducklake_table dt USING (table_id)
WHERE dt.table_name = 'insert_values_t' AND dt.end_snapshot IS NULL;

-- Verify no duplicate row_ids (the original symptom of the bug)
SELECT id, val FROM insert_values_t ORDER BY id;

-- Verify UPDATE/DELETE work (depends on correct row_id allocation)
UPDATE insert_values_t SET val = 'ONE' WHERE id = 1;
SELECT id, val FROM insert_values_t ORDER BY id;

DELETE FROM insert_values_t WHERE id = 2;
SELECT id, val FROM insert_values_t ORDER BY id;

DROP TABLE insert_values_t;
