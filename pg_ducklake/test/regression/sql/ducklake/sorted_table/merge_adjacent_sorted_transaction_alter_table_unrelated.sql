-- Upstream: test/sql/sorted_table/merge_adjacent_sorted_transaction_alter_table_unrelated.test
-- Skip: Physical sorted compaction order is not exposed by the PostgreSQL API.
-- Transactional sort metadata survives an unrelated column addition.
CREATE TABLE upstream_merge_sort_tx_alter (id bigint, key_one bigint, key_two text) USING ducklake;
INSERT INTO upstream_merge_sort_tx_alter VALUES (2, 20, 'b'), (1, 10, 'a');
BEGIN;
CALL ducklake.set_sort('upstream_merge_sort_tx_alter'::regclass, 'key_one ASC NULLS LAST', 'key_two ASC NULLS LAST');
ALTER TABLE upstream_merge_sort_tx_alter ADD COLUMN note text;
COMMIT;
SELECT count(*) > 0 AS merged FROM ducklake.merge_adjacent_files('upstream_merge_sort_tx_alter'::regclass);
SELECT * FROM ducklake.get_sort('upstream_merge_sort_tx_alter'::regclass);
SELECT * FROM upstream_merge_sort_tx_alter ORDER BY id;
DROP TABLE upstream_merge_sort_tx_alter;
