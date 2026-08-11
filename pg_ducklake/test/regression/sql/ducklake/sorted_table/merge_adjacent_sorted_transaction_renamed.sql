-- Upstream: test/sql/sorted_table/merge_adjacent_sorted_transaction_renamed.test
-- Skip: Physical sorted compaction order is not exposed by the PostgreSQL API.
-- Transactional column renames update sort expressions used by compaction.
CREATE TABLE upstream_merge_sort_tx_rename (id bigint, key_one bigint, key_two text) USING ducklake;
INSERT INTO upstream_merge_sort_tx_rename VALUES (2, 20, 'b'), (1, 10, 'a');
BEGIN;
CALL ducklake.set_sort('upstream_merge_sort_tx_rename'::regclass, 'key_one ASC NULLS LAST', 'key_two ASC NULLS LAST');
ALTER TABLE upstream_merge_sort_tx_rename RENAME COLUMN key_one TO first_key;
COMMIT;
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_merge_sort_tx_rename'::regclass);
SELECT * FROM ducklake.get_sort('upstream_merge_sort_tx_rename'::regclass);
SELECT id, first_key, key_two FROM upstream_merge_sort_tx_rename ORDER BY id;
DROP TABLE upstream_merge_sort_tx_rename;
