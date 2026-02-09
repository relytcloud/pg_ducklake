CREATE SCHEMA ducklake;

GRANT USAGE ON SCHEMA ducklake TO PUBLIC;

-- If duckdb.postgres_role is configured let's grant it access to the ducklake schema.
DO $$
DECLARE
    role_name text;
BEGIN
    SELECT current_setting('duckdb.postgres_role') INTO role_name;
    IF role_name != '' AND NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = role_name) THEN
        EXECUTE 'GRANT USAGE ON SCHEMA ducklake TO ' || quote_ident(current_setting('duckdb.postgres_role'));
    END IF;
END
$$;

CREATE FUNCTION ducklake._am_handler(internal)
RETURNS table_am_handler
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_am_handler'
LANGUAGE C;

CREATE ACCESS METHOD ducklake
TYPE TABLE
HANDLER ducklake._am_handler;

CREATE FUNCTION ducklake._create_table_trigger()
RETURNS event_trigger
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_create_table_trigger'
LANGUAGE C;

CREATE EVENT TRIGGER ducklake_create_table_trigger ON ddl_command_end
WHEN tag IN ('CREATE TABLE', 'CREATE TABLE AS')
EXECUTE FUNCTION ducklake._create_table_trigger();

CREATE FUNCTION ducklake._drop_trigger()
RETURNS event_trigger
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_drop_trigger'
LANGUAGE C;

CREATE EVENT TRIGGER ducklake_drop_trigger ON sql_drop
EXECUTE FUNCTION ducklake._drop_trigger();

CREATE FUNCTION ducklake._initialize() RETURNS void
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_initialize'
LANGUAGE C;

-- Initialize DuckDB when extension is created
DO $$
BEGIN
    PERFORM ducklake._initialize();
END
$$;

-- DuckLake Foreign Data Wrapper
CREATE FUNCTION ducklake.fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'ducklake_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION ducklake.fdw_validator(
    options text[],
    catalog oid
)
RETURNS void
AS 'MODULE_PATHNAME', 'ducklake_fdw_validator'
LANGUAGE C STRICT PARALLEL SAFE;

CREATE FOREIGN DATA WRAPPER ducklake_fdw
  HANDLER ducklake.fdw_handler
  VALIDATOR ducklake.fdw_validator;

-- DuckLake maintenance function for cleaning up old files
CREATE FUNCTION ducklake.ducklake_cleanup_old_files(older_than interval DEFAULT NULL)
RETURNS bigint
AS 'MODULE_PATHNAME', 'ducklake_cleanup_old_files'
LANGUAGE C;

COMMENT ON FUNCTION ducklake.ducklake_cleanup_old_files(interval) IS
'Clean up old files scheduled for deletion from the DuckLake database.
Parameters:
  older_than - Interval (e.g., ''24 hours'', ''7 days'', ''30 minutes'').
               If NULL (default), removes ALL scheduled files.
Returns the number of files cleaned up.';

-- DuckLake set_option function
CREATE PROCEDURE ducklake.set_option(
    option_name text,
    value "any",
    scope regclass DEFAULT NULL
)
AS 'MODULE_PATHNAME', 'ducklake_set_option'
LANGUAGE C;

COMMENT ON PROCEDURE ducklake.set_option(text, "any", regclass) IS
'Set a DuckLake option.
Parameters:
  option_name - Name of the option to set.
  value       - Value to set the option to.
  scope       - Optional table to apply the option to (NULL for global).';

-- DuckLake options function
CREATE FUNCTION ducklake.options(
    OUT option_name text,
    OUT description text,
    OUT value text,
    OUT scope text,
    OUT scope_entry text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

COMMENT ON FUNCTION ducklake.options() IS
'List all DuckLake options.
Returns:
  option_name - Name of the option.
  description - Description of the option.
  value       - Current value of the option.
  scope       - Scope of the option (GLOBAL, SCHEMA, TABLE, DEFAULT).
  scope_entry - The specific schema or table the option applies to.';
