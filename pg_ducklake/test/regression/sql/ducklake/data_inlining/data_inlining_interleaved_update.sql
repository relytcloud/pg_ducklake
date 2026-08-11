-- Upstream: test/sql/data_inlining/data_inlining_interleaved_update.test
CALL ducklake.set_option('data_inlining_row_limit', 10);
CREATE TABLE upstream_inline_interleave (id integer, val text) USING ducklake;
INSERT INTO upstream_inline_interleave VALUES (1, 'a'), (2, 'b');
SELECT max(snapshot_id) AS initial_snapshot FROM ducklake.ducklake_snapshot \gset
BEGIN;
INSERT INTO upstream_inline_interleave VALUES (3, 'c');
UPDATE upstream_inline_interleave SET val = 'aa' WHERE id = 1;
INSERT INTO upstream_inline_interleave VALUES (4, 'd');
COMMIT;
SELECT max(snapshot_id) AS first_snapshot FROM ducklake.ducklake_snapshot \gset
SELECT ducklake.rowid(), id, val FROM upstream_inline_interleave ORDER BY id;
SELECT r['change_type']::text AS change_type, count(*) AS rows,
       array_agg(r['id']::integer ORDER BY r['id']::integer) AS ids
FROM ducklake.table_changes('upstream_inline_interleave'::regclass, :first_snapshot, :first_snapshot) AS r
GROUP BY r['change_type']::text ORDER BY change_type;
BEGIN;
UPDATE upstream_inline_interleave SET val = 'bb' WHERE id = 2;
INSERT INTO upstream_inline_interleave VALUES (5, 'e');
COMMIT;
SELECT max(snapshot_id) AS second_snapshot FROM ducklake.ducklake_snapshot \gset
SELECT ducklake.rowid(), id, val FROM upstream_inline_interleave ORDER BY id;
SELECT r['change_type']::text AS change_type, count(*) AS rows,
       array_agg(r['id']::integer ORDER BY r['id']::integer) AS ids
FROM ducklake.table_changes('upstream_inline_interleave'::regclass, :second_snapshot, :second_snapshot) AS r
GROUP BY r['change_type']::text ORDER BY change_type;
DROP TABLE upstream_inline_interleave;
CALL ducklake.set_option('data_inlining_row_limit', 0);
