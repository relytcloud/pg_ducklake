CREATE SCHEMA ducklake;

GRANT USAGE ON SCHEMA ducklake TO PUBLIC;

-- Table Access Method
CREATE FUNCTION ducklake._am_handler(internal)
    RETURNS table_am_handler
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_am_handler'
    LANGUAGE C;

CREATE ACCESS METHOD ducklake
    TYPE TABLE
    HANDLER ducklake._am_handler;

-- DDL Event Triggers
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

-- Initialization function
CREATE FUNCTION ducklake._initialize()
    RETURNS void
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_initialize'
    LANGUAGE C;

-- Initialize DuckLake catalog when extension is created
DO $$
BEGIN
    PERFORM ducklake._initialize();
END
$$;

-- set_option procedure
CREATE PROCEDURE ducklake.set_option(
    option_name text,
    value "any",
    scope regclass DEFAULT NULL
)
AS 'MODULE_PATHNAME', 'ducklake_set_option'
LANGUAGE C;

-- options function (DuckDB-only — pg_duckdb routes the query to DuckDB)
CREATE FUNCTION ducklake.options(
    OUT option_name text,
    OUT description text,
    OUT value text,
    OUT scope text,
    OUT scope_entry text
)
RETURNS SETOF record
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- flush_inlined_data procedure
CREATE PROCEDURE ducklake.flush_inlined_data(
    scope regclass DEFAULT NULL
)
AS 'MODULE_PATHNAME', 'ducklake_flush_inlined_data'
LANGUAGE C;

-- cleanup_old_files function
CREATE FUNCTION ducklake.ducklake_cleanup_old_files(
    older_than interval DEFAULT NULL
)
RETURNS bigint
AS 'MODULE_PATHNAME', 'ducklake_cleanup_old_files'
LANGUAGE C;
