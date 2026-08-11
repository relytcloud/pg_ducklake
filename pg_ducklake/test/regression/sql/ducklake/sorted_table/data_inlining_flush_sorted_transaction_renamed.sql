-- Upstream: test/sql/sorted_table/data_inlining_flush_sorted_transaction_renamed.test
-- Skip: Physical sorted flush order is not exposed by the PostgreSQL API.
-- Transactional sort and rename operations remain usable by inline flush.
CREATE TABLE upstream_flush_sort_tx_rename (id integer, key_one integer, key_two text) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_flush_sort_tx_rename'::regclass);
INSERT INTO upstream_flush_sort_tx_rename VALUES (2, 20, 'b'), (1, 10, 'a');
BEGIN;
CALL ducklake.set_sort('upstream_flush_sort_tx_rename'::regclass, 'key_one ASC NULLS LAST', 'key_two ASC NULLS LAST');
ALTER TABLE upstream_flush_sort_tx_rename ADD COLUMN bonus text;
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_flush_sort_tx_rename'::regclass);
COMMIT;
BEGIN;
ALTER TABLE upstream_flush_sort_tx_rename RENAME COLUMN key_one TO first_key;
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_flush_sort_tx_rename'::regclass);
COMMIT;
SELECT * FROM ducklake.get_sort('upstream_flush_sort_tx_rename'::regclass);
SELECT id, first_key, key_two, bonus FROM upstream_flush_sort_tx_rename ORDER BY id;
DROP TABLE upstream_flush_sort_tx_rename;
