-- Upstream: test/sql/sorted_table/data_inlining_flush_sorted_expression_deletes_999_transaction.test
-- Skip: Requires implementation: transactional expression sorting is unsupported; do not bless the aborted transaction.
-- The expression-sort/delete flush regression also works inside a transaction.
CREATE TABLE upstream_flush_expression_delete_tx (id integer, val integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_flush_expression_delete_tx'::regclass);
INSERT INTO upstream_flush_expression_delete_tx VALUES (2, 0), (1, 0), (3, 0);
UPDATE upstream_flush_expression_delete_tx SET val = val + 1 WHERE id = 2;
UPDATE upstream_flush_expression_delete_tx SET val = val + 1 WHERE id = 2;
UPDATE upstream_flush_expression_delete_tx SET val = val + 1 WHERE id = 2;
BEGIN;
CALL ducklake.set_sort('upstream_flush_expression_delete_tx'::regclass, '(id + 0) ASC NULLS LAST');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_flush_expression_delete_tx'::regclass);
SELECT id, val FROM upstream_flush_expression_delete_tx ORDER BY id;
COMMIT;
SELECT id, val FROM upstream_flush_expression_delete_tx ORDER BY id;
DROP TABLE upstream_flush_expression_delete_tx;
