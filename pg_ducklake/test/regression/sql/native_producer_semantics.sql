-- Native producers must decline or reject PostgreSQL semantics they do not
-- implement. Safe defaults remain eligible; constraints fall back to DuckDB.

CALL ducklake.set_option('data_inlining_row_limit', 100);
SET ducklake.enable_direct_insert = true;

CREATE TABLE nps_plain (id int, v text DEFAULT 'default') USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('nps_plain'::regclass);
SELECT ducklake.reset_direct_insert_stats();
INSERT INTO nps_plain (id) VALUES (1);
SELECT pattern, reason, count
FROM ducklake.direct_insert_stats() WHERE count > 0
ORDER BY pattern, reason;
SELECT * FROM nps_plain;

-- A native plan cached in autocommit must switch to DuckDB when reused in an
-- explicit transaction instead of reaching the native writer's transaction
-- guard.
PREPARE nps_cached AS INSERT INTO nps_plain VALUES (2, 'cached');
SELECT ducklake.reset_native_writer_stats();
EXECUTE nps_cached;
BEGIN;
EXECUTE nps_cached;
COMMIT;
SELECT count(*) AS cached_rows FROM nps_plain WHERE id = 2;
SELECT count FROM ducklake.native_writer_stats() WHERE event = 'payload_rows';
DEALLOCATE nps_cached;

-- NOT NULL needs ModifyTable's constraint machinery, so it uses the DuckDB
-- fallback. The fallback must still reject invalid input.
CREATE TABLE nps_constrained (id int NOT NULL) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('nps_constrained'::regclass);
SELECT ducklake.reset_direct_insert_stats();
INSERT INTO nps_constrained VALUES (1);
INSERT INTO nps_constrained VALUES (NULL);
SELECT pattern, reason, count
FROM ducklake.direct_insert_stats() WHERE count > 0
ORDER BY pattern, reason;
SELECT * FROM nps_constrained;

-- A volatile default is not evaluated by the native producer. It falls back
-- rather than being evaluated at planning or publication retry time.
CREATE TABLE nps_volatile_default (id int DEFAULT ((random() * 1000)::int)) USING ducklake;
SELECT count(*) FROM ducklake.ensure_inlined_data_table('nps_volatile_default'::regclass);
SELECT ducklake.reset_direct_insert_stats();
INSERT INTO nps_volatile_default DEFAULT VALUES;
SELECT pattern, reason, count
FROM ducklake.direct_insert_stats() WHERE count > 0
ORDER BY pattern, reason;

-- Conflict clauses and unsupported UNNEST target lists do not use the native
-- plan.
SELECT ducklake.reset_direct_insert_stats();
INSERT INTO nps_plain VALUES (2, 'conflict') ON CONFLICT DO NOTHING;
PREPARE nps_partial (int[]) AS
  INSERT INTO nps_plain (id, v) SELECT UNNEST($1), 'constant'::text;
EXECUTE nps_partial(ARRAY[2, 3]);
DEALLOCATE nps_partial;
SELECT pattern, reason, count
FROM ducklake.direct_insert_stats() WHERE count > 0
ORDER BY pattern, reason;

-- PostgreSQL-only triggers and RLS cannot be preserved by the DuckDB fallback,
-- so eligible implicit INSERTs fail closed instead of silently bypassing them.
CREATE FUNCTION nps_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.v := 'triggered';
  RETURN NEW;
END
$$;
CREATE TRIGGER nps_trigger BEFORE INSERT ON nps_plain
FOR EACH ROW EXECUTE FUNCTION nps_trigger_fn();
SET ducklake.enable_direct_insert = false;
INSERT INTO nps_plain VALUES (4, 'original');
SET ducklake.enable_direct_insert = true;
DROP TRIGGER nps_trigger ON nps_plain;
DROP FUNCTION nps_trigger_fn();

CREATE TABLE nps_rule_log (id int);
CREATE RULE nps_rule AS ON INSERT TO nps_plain
DO ALSO INSERT INTO nps_rule_log VALUES (NEW.id);
BEGIN;
INSERT INTO nps_plain VALUES (5, 'rule');
ROLLBACK;
DROP RULE nps_rule ON nps_plain;
DROP TABLE nps_rule_log;

-- ALTER TABLE's DuckLake path currently rejects RLS DDL, so construct the
-- catalog state directly to cover existing/upgraded relations.
UPDATE pg_class SET relrowsecurity = true WHERE oid = 'nps_plain'::regclass;
SET ducklake.enable_direct_insert = false;
INSERT INTO nps_plain VALUES (6, 'rls');
SET ducklake.enable_direct_insert = true;
UPDATE pg_class SET relrowsecurity = false WHERE oid = 'nps_plain'::regclass;

-- Permissions are checked before both fallback gates and again when a cached
-- fallback plan is executed under another role.
CREATE ROLE nps_no_insert;
GRANT USAGE ON SCHEMA public TO nps_no_insert;
GRANT SELECT ON nps_plain TO nps_no_insert;
SET ducklake.enable_direct_insert = false;
PREPARE nps_acl_cached AS INSERT INTO nps_plain VALUES (7, 'cached denied');
EXECUTE nps_acl_cached;
SET ROLE nps_no_insert;
EXECUTE nps_acl_cached;
RESET ROLE;
DEALLOCATE nps_acl_cached;

SET ROLE nps_no_insert;
INSERT INTO nps_plain VALUES (8, 'fallback denied');
BEGIN;
INSERT INTO nps_plain VALUES (9, 'transaction denied');
ROLLBACK;
RESET ROLE;

-- Source SELECT permissions are enforced recursively for nested subqueries,
-- CTEs, disabled-direct and explicit-transaction fallbacks, and cached plans.
CREATE TABLE nps_source (id int, v text);
INSERT INTO nps_source VALUES (10, 'visible'), (11, 'secret');
GRANT INSERT ON nps_plain TO nps_no_insert;
SET ducklake.enable_direct_insert = true;
SET ROLE nps_no_insert;
INSERT INTO nps_plain
SELECT * FROM (SELECT * FROM (SELECT id, v FROM nps_source) nested_1) nested_2;
RESET ROLE;
SET ducklake.enable_direct_insert = false;
SET ROLE nps_no_insert;
WITH source_cte AS (SELECT id, v FROM nps_source)
INSERT INTO nps_plain SELECT * FROM source_cte;
RESET ROLE;
SET ducklake.enable_direct_insert = true;
SET ROLE nps_no_insert;
BEGIN;
INSERT INTO nps_plain
SELECT (SELECT id FROM nps_source LIMIT 1), 'explicit source';
ROLLBACK;
RESET ROLE;

SET plan_cache_mode = force_generic_plan;
PREPARE nps_source_cached AS
INSERT INTO nps_plain SELECT id, v FROM nps_source;
EXECUTE nps_source_cached;
SET ROLE nps_no_insert;
EXECUTE nps_source_cached;
RESET ROLE;
DEALLOCATE nps_source_cached;
RESET plan_cache_mode;

-- DuckDB cannot preserve source RLS quals. Reject instead of exposing rows a
-- PostgreSQL scan would hide, including through CTEs and a plan cached before
-- RLS was enabled.
PREPARE nps_rls_cached AS
INSERT INTO nps_plain
SELECT * FROM (SELECT * FROM (SELECT id, v FROM nps_source) nested_1) nested_2;
EXECUTE nps_rls_cached;
GRANT SELECT ON nps_source TO nps_no_insert;
ALTER TABLE nps_source ENABLE ROW LEVEL SECURITY;
CREATE POLICY nps_source_policy ON nps_source USING (id = 10);
SET ROLE nps_no_insert;
SELECT * FROM nps_source ORDER BY id;
WITH source_cte AS (SELECT * FROM nps_source)
INSERT INTO nps_plain SELECT * FROM source_cte;
RESET ROLE;
SET ROLE nps_no_insert;
EXECUTE nps_rls_cached;
RESET ROLE;
DEALLOCATE nps_rls_cached;

SET ducklake.enable_direct_insert = true;
REVOKE ALL ON nps_source FROM nps_no_insert;
REVOKE ALL ON nps_plain FROM nps_no_insert;
REVOKE ALL ON SCHEMA public FROM nps_no_insert;
DROP POLICY nps_source_policy ON nps_source;
DROP TABLE nps_source;
DROP ROLE nps_no_insert;

DROP TABLE nps_volatile_default;
DROP TABLE nps_constrained;
DROP TABLE nps_plain;
CALL ducklake.set_option('data_inlining_row_limit', 0);
