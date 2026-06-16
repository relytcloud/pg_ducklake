-- S3 / Azure secret management surface (ducklake_secret FDW).
--
-- The real S3 round-trip (create_s3_secret + reading/writing s3:// data) needs
-- the httpfs extension, which pg_ducklake autoinstalls at runtime (network).
-- That is covered by the e2e MinIO suite. Here we cover only the hermetic
-- surface: object existence, signatures, and validator paths that reject
-- BEFORE any DuckDB CREATE SECRET runs (so no httpfs load is triggered).

-- Convenience function signatures + defaults are locked.
SELECT proname, pg_get_function_arguments(p.oid) AS args
FROM pg_proc p
WHERE pronamespace = 'ducklake'::regnamespace
  AND proname IN ('create_s3_secret', 'create_azure_secret')
ORDER BY proname;

-- The dedicated secret FDW is registered and distinct from ducklake_fdw.
SELECT f.fdwname, h.proname AS handler, v.proname AS validator
FROM pg_foreign_data_wrapper f
LEFT JOIN pg_proc h ON h.oid = f.fdwhandler
LEFT JOIN pg_proc v ON v.oid = f.fdwvalidator
WHERE f.fdwname LIKE 'ducklake%'
ORDER BY f.fdwname;

-- Validator: a secret SERVER must specify TYPE (errors before any CREATE SECRET).
CREATE SERVER notype_secret FOREIGN DATA WRAPPER ducklake_secret
  OPTIONS (region 'us-east-1');

-- Validator: an unknown secret type is rejected by DuckDB without loading httpfs.
CREATE SERVER bad_secret TYPE 'not_a_type' FOREIGN DATA WRAPPER ducklake_secret;

-- Validator: the FDW object itself takes no options.
ALTER FOREIGN DATA WRAPPER ducklake_secret OPTIONS (ADD anything 'x');

-- Helper: create_s3_secret rejects types outside {s3,gcs,r2} before any DDL.
SELECT ducklake.create_s3_secret('invalid_type', 'k', 's');
SELECT ducklake.create_s3_secret('azure', 'k', 's');

-- DROP of an absent secret server gives a clean PG error (no secret-hook interference).
DROP SERVER no_such_secret_server;
