-- Regression test for issue #222: timestamp time travel at the creation
-- snapshot of the first table in a brand-new schema.
--
-- CREATE TABLE ... USING ducklake auto-creates the DuckLake schema before
-- the table, both committed via SPI inside the single wrapping PG
-- transaction, so the two snapshots can share an identical snapshot_time.
-- Without a snapshot_id tie-breaker, resolving a timestamp AT clause that
-- lands on the tie could pick the earlier (schema-only) snapshot, where the
-- table does not exist yet, and error.
CREATE SCHEMA tt_new_schema;
SET search_path = tt_new_schema;
CREATE TABLE tt_new (id int, name text) USING ducklake;
SELECT max(snapshot_id) AS v_create FROM ducklake.ducklake_snapshot \gset
SELECT snapshot_time AS create_ts FROM ducklake.ducklake_snapshot WHERE snapshot_id = :v_create \gset

-- Time travel to the creation timestamp must not error: the table exists
-- and is empty. (Schema-qualified: the single-arg overload otherwise
-- defaults unqualified names to "public".)
SELECT * FROM ducklake.time_travel('tt_new_schema.tt_new', :'create_ts'::timestamptz);

INSERT INTO tt_new VALUES (1, 'Alice');
SELECT max(snapshot_id) AS v_insert FROM ducklake.ducklake_snapshot \gset
SELECT snapshot_time AS insert_ts FROM ducklake.ducklake_snapshot WHERE snapshot_id = :v_insert \gset

-- Time travel to the first-insert timestamp returns the inserted row.
SELECT * FROM ducklake.time_travel('tt_new_schema.tt_new', :'insert_ts'::timestamptz);

-- Cleanup
DROP TABLE tt_new;
DROP SCHEMA tt_new_schema;
