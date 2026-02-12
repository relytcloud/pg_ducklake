CREATE FUNCTION pg_ducklake_next_verify()
RETURNS text
LANGUAGE C
AS 'MODULE_PATHNAME', 'pg_ducklake_next_verify';

COMMENT ON FUNCTION pg_ducklake_next_verify() IS
'Verifies pg_duckdb dependency by referencing its C interface, installs/loads DuckLake in DuckDB via duckdb.raw_query(), then executes DuckLake operations.';
