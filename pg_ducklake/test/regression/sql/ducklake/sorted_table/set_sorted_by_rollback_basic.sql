-- Upstream: test/sql/sorted_table/set_sorted_by_rollback_basic.test
-- Sort metadata changes are rolled back with their transaction.
CREATE TABLE upstream_sort_rollback_basic (a integer, b text) USING ducklake;
SELECT max(snapshot_id) AS snapshot_before FROM ducklake.ducklake_snapshot \gset
BEGIN;
CALL ducklake.set_sort('upstream_sort_rollback_basic'::regclass, 'a ASC NULLS LAST');
ROLLBACK;
SELECT max(snapshot_id) = :snapshot_before AS rollback_created_no_snapshot FROM ducklake.ducklake_snapshot;
SELECT count(*) AS active_sort_keys FROM ducklake.get_sort('upstream_sort_rollback_basic'::regclass);
BEGIN;
CALL ducklake.set_sort('upstream_sort_rollback_basic'::regclass, 'a ASC');
CALL ducklake.set_sort('upstream_sort_rollback_basic'::regclass, 'b DESC');
ROLLBACK;
SELECT max(snapshot_id) = :snapshot_before AS second_rollback_created_no_snapshot FROM ducklake.ducklake_snapshot;
SELECT count(*) AS active_sort_keys FROM ducklake.get_sort('upstream_sort_rollback_basic'::regclass);
CALL ducklake.set_sort('upstream_sort_rollback_basic'::regclass, 'a DESC');
SELECT * FROM ducklake.get_sort('upstream_sort_rollback_basic'::regclass);
DROP TABLE upstream_sort_rollback_basic;
