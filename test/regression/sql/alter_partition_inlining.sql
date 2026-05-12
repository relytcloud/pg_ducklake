-- Regression for GitHub issue #197:
-- ALTER on a DuckLake table that bumps per-table schema_version
-- (set_partition, ADD COLUMN, ...) must not permanently block direct
-- insert into the inlined data table.
--
-- Pre-fix: GetTableInliningState() compared idt.schema_version against
-- MAX(ducklake_schema_versions.schema_version) and rejected with
-- TI_SCHEMA_VERSION_MISMATCH whenever any schema-bumping DDL ran,
-- forcing every subsequent INSERT into the slow path.
--
-- Post-fix: pg_ducklake aligns with DuckLake's own contract -- the
-- inlined table's schema_version names the heap table we write to,
-- regardless of ducklake_schema_versions.  Direct insert keeps working.

CALL ducklake.set_option('data_inlining_row_limit', 100);

CREATE OR REPLACE VIEW direct_insert_stats_nonzero AS
    SELECT pattern, reason, count FROM ducklake.direct_insert_stats()
    WHERE count > 0 ORDER BY pattern, reason;

-- ------------------------------------------------------------------
-- 1. set_partition -- DuckLake bumps per-table schema_version
-- ------------------------------------------------------------------
CREATE TABLE api_part (a INT, b INT) USING ducklake;
INSERT INTO api_part VALUES (1, 1), (2, 2);                  -- seeds inlined table

-- After seeding: api_part has 2 rows, single inlined heap table holds them.
SELECT a, b FROM api_part ORDER BY a;
SELECT count(*) AS idt_rows
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table dt USING (table_id)
WHERE dt.table_name = 'api_part' AND dt.end_snapshot IS NULL;
SELECT idt.table_name AS seed_inlined
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table dt USING (table_id)
WHERE dt.table_name = 'api_part' AND dt.end_snapshot IS NULL
\gset
SELECT row_id, a, b FROM ducklake.:"seed_inlined" ORDER BY row_id;

CALL ducklake.set_partition('api_part'::regclass, 'a');      -- bumps schema_version

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO api_part VALUES (3, 3);

-- Pre-fix:  unmatched / schema_version_mismatch = 1
-- Post-fix: matched_values / ok = 1
SELECT * FROM direct_insert_stats_nonzero;
SELECT a, b FROM api_part ORDER BY a;

-- After ALTER + INSERT: two inlined heap tables exist; the new row
-- landed in the one DuckLake created at the bumped schema_version
-- while the seed rows stay in the old one.
SELECT count(*) AS idt_rows
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table dt USING (table_id)
WHERE dt.table_name = 'api_part' AND dt.end_snapshot IS NULL;

SELECT idt.table_name AS old_inlined
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table dt USING (table_id)
WHERE dt.table_name = 'api_part' AND dt.end_snapshot IS NULL
ORDER BY idt.schema_version ASC LIMIT 1
\gset
SELECT idt.table_name AS new_inlined
FROM ducklake.ducklake_inlined_data_tables idt
JOIN ducklake.ducklake_table dt USING (table_id)
WHERE dt.table_name = 'api_part' AND dt.end_snapshot IS NULL
ORDER BY idt.schema_version DESC LIMIT 1
\gset

-- Old (seed-sv) heap table: still holds the pre-ALTER rows, no new row here.
SELECT 'old' AS which, row_id, a, b FROM ducklake.:"old_inlined" ORDER BY row_id;
-- New (bumped-sv) heap table: receives the post-ALTER row only.
SELECT 'new' AS which, row_id, a, b FROM ducklake.:"new_inlined" ORDER BY row_id;

DROP TABLE api_part;

-- ------------------------------------------------------------------
-- 2. set_sort -- DuckLake does NOT bump per-table schema_version,
--    so direct insert already works pre-fix.  Regression guard only.
-- ------------------------------------------------------------------
CREATE TABLE api_sort (a INT, b INT) USING ducklake;
INSERT INTO api_sort VALUES (1, 1), (2, 2);
CALL ducklake.set_sort('api_sort'::regclass, 'a ASC');

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO api_sort VALUES (3, 3);

-- Both pre-fix and post-fix: matched_values / ok = 1
SELECT * FROM direct_insert_stats_nonzero;
SELECT a, b FROM api_sort ORDER BY a;

DROP TABLE api_sort;

-- ------------------------------------------------------------------
-- 3. ADD COLUMN -- bumps schema_version (same trigger as case 1).
--    direct_insert_stats.sql already asserts the counter; here we
--    additionally verify the row makes it into the table.
-- ------------------------------------------------------------------
CREATE TABLE api_add (a INT, b INT) USING ducklake;
INSERT INTO api_add VALUES (1, 1), (2, 2);
ALTER TABLE api_add ADD COLUMN c INT DEFAULT 0;

SELECT ducklake.reset_direct_insert_stats();
INSERT INTO api_add (a, b, c) VALUES (3, 3, 3);

-- Pre-fix:  unmatched / schema_version_mismatch = 1
-- Post-fix: matched_values / ok = 1
SELECT * FROM direct_insert_stats_nonzero;
SELECT a, b, c FROM api_add ORDER BY a;

DROP TABLE api_add;

-- ------------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------------
DROP VIEW direct_insert_stats_nonzero;
SELECT ducklake.reset_direct_insert_stats();
CALL ducklake.set_option('data_inlining_row_limit', 0);
