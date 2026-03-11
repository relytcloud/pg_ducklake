-- Test access control for DuckLake tables
-- Verifies which PostgreSQL permission checks apply to DuckLake tables.
--
-- Predefined roles (ducklake_superuser, ducklake_writer, ducklake_reader) are
-- created by the extension with duckdb_group membership and ducklake metadata
-- grants. This test creates LOGIN users in those roles and verifies behavior.
--
-- Current state: pg_duckdb's planner hook sets permInfos = NULL, so most
-- DML permission checks are bypassed when queries are routed through DuckDB.

-- Verify predefined roles exist
SELECT rolname FROM pg_roles
WHERE rolname IN ('ducklake_superuser', 'ducklake_writer', 'ducklake_reader')
ORDER BY rolname;

-- Re-grant on metadata tables (in case prior tests created new tables
-- in the ducklake schema that lack the original extension-time grants).
GRANT ALL ON ALL TABLES IN SCHEMA ducklake TO ducklake_superuser, ducklake_writer, ducklake_reader;
GRANT ALL ON ALL SEQUENCES IN SCHEMA ducklake TO ducklake_superuser, ducklake_writer, ducklake_reader;

-- Setup: create login users with membership in predefined roles
CREATE USER test_lake_admin IN ROLE ducklake_superuser, pg_read_server_files, pg_write_server_files;
CREATE USER test_lake_writer IN ROLE ducklake_writer, pg_read_server_files, pg_write_server_files;
CREATE USER test_lake_reader IN ROLE ducklake_reader, pg_read_server_files, pg_write_server_files;

-- test_no_access has duckdb_group (can run DuckDB queries) but no ducklake role
CREATE USER test_no_access IN ROLE duckdb_group, pg_read_server_files, pg_write_server_files;
-- test_no_access still needs ducklake metadata access for DuckDB SPI
GRANT ALL ON ALL TABLES IN SCHEMA ducklake TO test_no_access;
GRANT ALL ON ALL SEQUENCES IN SCHEMA ducklake TO test_no_access;

-- Create test table as superuser
CREATE TABLE acl_test (id int, name text, secret text) USING ducklake;
INSERT INTO acl_test VALUES (1, 'Alice', 'pw1'), (2, 'Bob', 'pw2');

-- Re-grant for test_no_access: this user is deliberately not in any
-- predefined ducklake role, so it does not receive automatic grants on
-- inlined data tables.  Grant manually so later tests are deterministic.
GRANT ALL ON ALL TABLES IN SCHEMA ducklake TO test_no_access;

-- Grant privileges on test table to predefined roles
GRANT ALL ON TABLE acl_test TO ducklake_superuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE acl_test TO ducklake_writer;
GRANT SELECT ON TABLE acl_test TO ducklake_reader;

-- ============================================================
-- 1. Full-access role: all DML works
-- ============================================================
SET ROLE test_lake_admin;

SELECT * FROM acl_test ORDER BY id;
INSERT INTO acl_test VALUES (3, 'Carol', 'pw3');
UPDATE acl_test SET name = 'Updated' WHERE id = 3;
DELETE FROM acl_test WHERE id = 3;

RESET ROLE;

-- ============================================================
-- 2. DDL ownership: ALTER/DROP fail for non-owner
-- ============================================================
SET ROLE test_lake_writer;

ALTER TABLE acl_test ADD COLUMN extra int;
DROP TABLE acl_test;

RESET ROLE;

-- ============================================================
-- 3. VACUUM ownership: non-owner gets WARNING
-- ============================================================
SET ROLE test_lake_writer;
VACUUM acl_test;
RESET ROLE;

-- Owner succeeds
VACUUM acl_test;

-- ============================================================
-- 4. Known gap: DML permissions not enforced
--    Reader can INSERT/UPDATE/DELETE (should fail but doesn't)
-- ============================================================
SET ROLE test_lake_reader;

-- SELECT works (expected)
SELECT count(*) FROM acl_test;

-- These succeed even though reader only has SELECT (known gap)
INSERT INTO acl_test VALUES (10, 'Ghost', 'x');
DELETE FROM acl_test WHERE id = 10;

RESET ROLE;

-- Verify no data leaked from the gap test
SELECT * FROM acl_test ORDER BY id;

-- ============================================================
-- 5. Known gap: no-access role can still query (should fail)
-- ============================================================
SET ROLE test_no_access;

SELECT count(*) FROM acl_test;

RESET ROLE;

-- ============================================================
-- 6. Known gap: column-level privileges not enforced
-- ============================================================
REVOKE ALL ON TABLE acl_test FROM ducklake_reader;
GRANT SELECT (id, name) ON TABLE acl_test TO ducklake_reader;

SET ROLE test_lake_reader;

-- Granted columns work
SELECT id, name FROM acl_test ORDER BY id;

-- Non-granted column also works (known gap)
SELECT secret FROM acl_test ORDER BY id;

RESET ROLE;

-- ============================================================
-- 7. Known gap: time_travel bypasses all checks
-- ============================================================
SELECT max(snapshot_id) AS latest_snap FROM ducklake.ducklake_snapshot \gset
REVOKE ALL ON TABLE acl_test FROM test_no_access;

SET ROLE test_no_access;

SELECT count(*) FROM ducklake.time_travel('acl_test', :latest_snap);

RESET ROLE;

-- ============================================================
-- Cleanup
-- ============================================================
DROP TABLE acl_test;
REVOKE ALL ON ALL TABLES IN SCHEMA ducklake FROM test_no_access;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA ducklake FROM test_no_access;
DROP USER test_lake_admin, test_lake_writer, test_lake_reader, test_no_access;
