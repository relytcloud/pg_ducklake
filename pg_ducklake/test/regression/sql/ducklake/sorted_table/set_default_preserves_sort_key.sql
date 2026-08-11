-- Upstream: test/sql/sorted_table/set_default_preserves_sort_key.test
-- SET DEFAULT followed by a sort-column rename preserves the sort key.
CREATE TABLE upstream_sort_default (a integer, b integer) USING ducklake;
INSERT INTO upstream_sort_default VALUES (3, 30), (1, 10);
CALL ducklake.set_sort('upstream_sort_default'::regclass, 'a ASC');
BEGIN;
ALTER TABLE upstream_sort_default ALTER COLUMN b SET DEFAULT 42;
ALTER TABLE upstream_sort_default RENAME COLUMN a TO a_renamed;
COMMIT;
INSERT INTO upstream_sort_default (a_renamed) VALUES (2);
SELECT * FROM ducklake.get_sort('upstream_sort_default'::regclass);
SELECT * FROM upstream_sort_default ORDER BY a_renamed;
DROP TABLE upstream_sort_default;
