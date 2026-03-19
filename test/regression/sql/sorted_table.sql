-- Test sorted table support: CREATE INDEX USING ducklake_sorted + set_sort/reset_sort

-- ============================================================
-- CREATE INDEX USING ducklake_sorted
-- ============================================================

-- 1. Basic single-column sort
CREATE TABLE sort_idx_basic (id int, val int) USING ducklake;

CREATE INDEX sort_idx_basic_idx ON sort_idx_basic USING ducklake_sorted (val);

SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'sort_idx_basic_idx';

SELECT * FROM ducklake.get_sort('sort_idx_basic'::regclass);

-- 2. Multi-key with directions and nulls
CREATE TABLE sort_idx_multi (a int, b text, c int) USING ducklake;

CREATE INDEX sort_idx_multi_idx ON sort_idx_multi USING ducklake_sorted (a ASC NULLS LAST, b DESC NULLS FIRST);

SELECT * FROM ducklake.get_sort('sort_idx_multi'::regclass);

-- 3. Expression-based sort key
CREATE TABLE sort_idx_expr (id int, ts timestamp, val int) USING ducklake;

CREATE INDEX sort_idx_expr_idx ON sort_idx_expr USING ducklake_sorted (date_trunc('day', ts));

SELECT * FROM ducklake.get_sort('sort_idx_expr'::regclass);

-- 4. DROP INDEX resets sort order and removes pg_class entry
DROP INDEX sort_idx_basic_idx;

SELECT c.relname FROM pg_class c WHERE c.relname = 'sort_idx_basic_idx';

SELECT * FROM ducklake.get_sort('sort_idx_basic'::regclass);

-- 5. ALTER INDEX RENAME TO
ALTER INDEX sort_idx_multi_idx RENAME TO sort_idx_multi_renamed;

SELECT * FROM ducklake.get_sort('sort_idx_multi'::regclass);

-- 6. Error: non-DuckLake table
CREATE TABLE heap_sort_table (id int);
CREATE INDEX ON heap_sort_table USING ducklake_sorted (id);
DROP TABLE heap_sort_table;

-- 7. Error: CONCURRENTLY
CREATE INDEX CONCURRENTLY ON sort_idx_basic USING ducklake_sorted (val);

-- 8. Error: UNIQUE
CREATE UNIQUE INDEX ON sort_idx_basic USING ducklake_sorted (val);

-- 9. Error: WHERE clause
CREATE INDEX ON sort_idx_basic USING ducklake_sorted (val) WHERE val > 0;

-- 10. Error: INCLUDE
CREATE INDEX ON sort_idx_basic USING ducklake_sorted (val) INCLUDE (id);

-- ============================================================
-- set_sort / reset_sort (procedure-based alternative)
-- ============================================================

-- 11. set_sort basic
CREATE TABLE sort_basic (id int, val int) USING ducklake;

CALL ducklake.set_sort('sort_basic'::regclass, 'val ASC');

-- set_sort syncs a ducklake_sorted index to pg_class via snapshot trigger
SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
JOIN pg_index i ON i.indexrelid = c.oid
WHERE i.indrelid = 'sort_basic'::regclass AND am.amname = 'ducklake_sorted';

SELECT * FROM ducklake.get_sort('sort_basic'::regclass);

-- 12. Multi-key sort with direction and null order
CREATE TABLE sort_multi (a int, b text, c int) USING ducklake;

CALL ducklake.set_sort('sort_multi'::regclass, 'a ASC NULLS LAST', 'b DESC NULLS FIRST');

SELECT * FROM ducklake.get_sort('sort_multi'::regclass);

-- 13. Expression-based sort key
CREATE TABLE sort_expr (id int, ts timestamp, val int) USING ducklake;

CALL ducklake.set_sort('sort_expr'::regclass, 'date_trunc(''day'', ts) ASC');

SELECT * FROM ducklake.get_sort('sort_expr'::regclass);

-- 14. set_sort replaces an existing ducklake_sorted index
CREATE INDEX sort_basic_named ON sort_basic USING ducklake_sorted (id DESC);
SELECT * FROM ducklake.get_sort('sort_basic'::regclass);

CALL ducklake.set_sort('sort_basic'::regclass, 'val ASC');
SELECT * FROM ducklake.get_sort('sort_basic'::regclass);

-- verify old named index is gone and new auto-named one exists
SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
JOIN pg_index i ON i.indexrelid = c.oid
WHERE i.indrelid = 'sort_basic'::regclass AND am.amname = 'ducklake_sorted';

-- 15. Reset sort
CALL ducklake.reset_sort('sort_basic'::regclass);

SELECT * FROM ducklake.get_sort('sort_basic'::regclass);

-- 16. Error: non-ducklake table
CREATE TABLE heap_sort_table2 (id int);
CALL ducklake.set_sort('heap_sort_table2'::regclass, 'id ASC');
DROP TABLE heap_sort_table2;

-- ============================================================
-- ALTER TABLE with sorted index
-- ============================================================

-- 17. ADD COLUMN preserves sort order
CREATE TABLE sort_alter (id int, val int) USING ducklake;
CREATE INDEX sort_alter_idx ON sort_alter USING ducklake_sorted (val);
SELECT * FROM ducklake.get_sort('sort_alter'::regclass);

ALTER TABLE sort_alter ADD COLUMN extra text;
SELECT * FROM ducklake.get_sort('sort_alter'::regclass);

-- verify index still exists
SELECT c.relname, am.amname
FROM pg_class c JOIN pg_am am ON c.relam = am.oid
WHERE c.relname = 'sort_alter_idx';

-- 18. DROP COLUMN on non-sort column preserves sort order
ALTER TABLE sort_alter DROP COLUMN extra;
SELECT * FROM ducklake.get_sort('sort_alter'::regclass);

-- 19. DROP COLUMN on sort key column cascade-drops the pg_class index.
-- NOTE: DuckDB sort metadata is not reset because ALTER TABLE DROP COLUMN
-- cascade does not go through the utility hook. DuckDB keeps stale sort
-- metadata until the next compaction or explicit reset_sort.
ALTER TABLE sort_alter DROP COLUMN val;

SELECT c.relname FROM pg_class c WHERE c.relname = 'sort_alter_idx';
SELECT * FROM ducklake.get_sort('sort_alter'::regclass);

-- 20. DROP TABLE with sorted index
CREATE TABLE sort_drop_tbl (id int, val int) USING ducklake;
CREATE INDEX ON sort_drop_tbl USING ducklake_sorted (val);
DROP TABLE sort_drop_tbl;

-- Cleanup
DROP TABLE sort_idx_basic;
DROP INDEX sort_idx_multi_renamed;
DROP TABLE sort_idx_multi;
DROP INDEX sort_idx_expr_idx;
DROP TABLE sort_idx_expr;
DROP TABLE sort_basic;
DROP TABLE sort_multi;
DROP TABLE sort_expr;
DROP TABLE sort_alter;
