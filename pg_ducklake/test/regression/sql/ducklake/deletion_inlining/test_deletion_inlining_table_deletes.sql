-- Upstream: test/sql/deletion_inlining/test_deletion_inlining_table_deletes.test
-- Skip: Requires implementation: table_deletions currently depends on unavailable struct_pack support.
CALL ducklake.set_option('data_inlining_row_limit', 5);
CREATE TABLE upstream_delete_feed USING ducklake AS SELECT g AS i FROM generate_series(0,19) g;
INSERT INTO upstream_delete_feed SELECT g FROM generate_series(100,114) g;
SELECT max(snapshot_id) AS vinsert FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_delete_feed WHERE i < 3;
SELECT max(snapshot_id) AS vd1 FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_delete_feed WHERE i BETWEEN 100 AND 101;
SELECT max(snapshot_id) AS vd2 FROM ducklake.ducklake_snapshot \gset
DELETE FROM upstream_delete_feed WHERE i IN (5,10,105);
SELECT max(snapshot_id) AS vd3 FROM ducklake.ducklake_snapshot \gset
SELECT count(*) FROM ducklake.table_deletions('upstream_delete_feed'::regclass, :vinsert, :vinsert);
SELECT count(*) FROM ducklake.table_deletions('upstream_delete_feed'::regclass, :vd1, :vd1);
SELECT count(*) FROM ducklake.table_deletions('upstream_delete_feed'::regclass, :vd2, :vd2);
SELECT count(*) FROM ducklake.table_deletions('upstream_delete_feed'::regclass, :vd3, :vd3);
SELECT count(*) FROM ducklake.table_deletions('upstream_delete_feed'::regclass, :vd1, :vd3);
DROP TABLE upstream_delete_feed;
CALL ducklake.set_option('data_inlining_row_limit', 0);
