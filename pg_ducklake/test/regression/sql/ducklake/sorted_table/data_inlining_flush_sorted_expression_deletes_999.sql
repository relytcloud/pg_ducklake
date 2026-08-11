-- Upstream: test/sql/sorted_table/data_inlining_flush_sorted_expression_deletes_999.test
-- Skip: Requires implementation: expression sorting is unsupported, so the delete-position invariant cannot run.
-- A non-column sort key must not corrupt delete positions during flush.
CREATE TABLE upstream_flush_expression_delete (id integer, val integer) USING ducklake;
CALL ducklake.set_option('data_inlining_row_limit', 100, 'upstream_flush_expression_delete'::regclass);
INSERT INTO upstream_flush_expression_delete VALUES (2, 0), (1, 0), (3, 0);
UPDATE upstream_flush_expression_delete SET val = val + 1 WHERE id = 2;
UPDATE upstream_flush_expression_delete SET val = val + 1 WHERE id = 2;
UPDATE upstream_flush_expression_delete SET val = val + 1 WHERE id = 2;
CALL ducklake.set_sort('upstream_flush_expression_delete'::regclass, '(id + 0) ASC NULLS LAST');
SELECT count(*) > 0 AS flushed FROM ducklake.flush_inlined_data('upstream_flush_expression_delete'::regclass);
SELECT id, val FROM upstream_flush_expression_delete ORDER BY id;
DROP TABLE upstream_flush_expression_delete;
