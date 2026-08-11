-- Upstream: test/sql/transaction/create_conflict.test
-- Tables and views share a namespace and transaction-local conflicts must be detected.

CREATE TABLE upstream_create_conflict (i integer, j integer) USING ducklake;
BEGIN;
SAVEPOINT expected_duplicate_table;
CREATE TABLE upstream_create_conflict (i text) USING ducklake;
ROLLBACK TO SAVEPOINT expected_duplicate_table;
COMMIT;
CREATE TABLE IF NOT EXISTS upstream_create_conflict (i text) USING ducklake;
SELECT * FROM upstream_create_conflict;

-- PostgreSQL has no CREATE OR REPLACE TABLE syntax; map it to an atomic drop/create.
BEGIN;
DROP TABLE upstream_create_conflict;
CREATE TABLE upstream_create_conflict (i text) USING ducklake;
COMMIT;
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'upstream_create_conflict'
ORDER BY ordinal_position;
SELECT count(*) AS replacement_is_empty FROM upstream_create_conflict;

BEGIN;
SAVEPOINT expected_view_table_conflict;
CREATE VIEW upstream_create_conflict AS SELECT 42 AS i;
ROLLBACK TO SAVEPOINT expected_view_table_conflict;
COMMIT;
CREATE VIEW upstream_conflict_view AS SELECT 42 AS i;
-- PostgreSQL has no CREATE VIEW IF NOT EXISTS syntax; perform its no-op semantics.
DO $do$
BEGIN
  IF to_regclass('upstream_conflict_view') IS NULL THEN
    EXECUTE 'CREATE VIEW upstream_conflict_view AS SELECT 84 AS i';
  END IF;
END
$do$;
SELECT * FROM upstream_conflict_view;
CREATE OR REPLACE VIEW upstream_conflict_view AS SELECT 84 AS i;
SELECT * FROM upstream_conflict_view;
BEGIN;
SAVEPOINT expected_table_view_conflict;
CREATE TABLE upstream_conflict_view (i integer) USING ducklake;
ROLLBACK TO SAVEPOINT expected_table_view_conflict;
COMMIT;
DROP VIEW upstream_conflict_view;

BEGIN;
CREATE VIEW upstream_conflict_view AS SELECT 42 AS i;
SAVEPOINT expected_duplicate_view;
CREATE VIEW upstream_conflict_view AS SELECT 84 AS i;
ROLLBACK TO SAVEPOINT expected_duplicate_view;
ROLLBACK;

BEGIN;
CREATE TABLE upstream_conflict_t1 USING ducklake AS SELECT 42 AS i;
SAVEPOINT expected_view_table_tx_conflict;
CREATE VIEW upstream_conflict_t1 AS SELECT 84 AS i;
ROLLBACK TO SAVEPOINT expected_view_table_tx_conflict;
ROLLBACK;

DROP TABLE upstream_create_conflict;
