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

-- ALTER TABLE Event Trigger
CREATE FUNCTION ducklake._alter_table_trigger()
    RETURNS event_trigger
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_alter_table_trigger'
    LANGUAGE C;

CREATE EVENT TRIGGER ducklake_alter_table_trigger ON ddl_command_end
    WHEN tag IN ('ALTER TABLE')
    EXECUTE FUNCTION ducklake._alter_table_trigger();

-- Metadata sync trigger function: DuckDB→PG catalog sync.
-- When an external DuckDB client creates/drops tables (writing directly to
-- ducklake metadata tables), this trigger creates/drops corresponding
-- pg_class entries so the tables become visible from PostgreSQL.
-- The trigger itself is created by the metadata manager during initialization.
CREATE FUNCTION ducklake._snapshot_trigger()
    RETURNS trigger
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_snapshot_trigger'
    LANGUAGE C;

-- Initialization function
CREATE FUNCTION ducklake._initialize()
    RETURNS void
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_initialize'
    LANGUAGE C;

-- Initialize DuckLake catalog when extension is created.
-- Must run after _snapshot_trigger is registered, since initialization
-- creates the trigger on ducklake_snapshot.
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

-- time_travel by version (DuckDB-only — pg_duckdb routes the query to DuckDB)
CREATE FUNCTION ducklake.time_travel(table_name text, version bigint)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- time_travel by timestamp (DuckDB-only — pg_duckdb routes the query to DuckDB)
CREATE FUNCTION ducklake.time_travel(table_name text, "timestamp" timestamptz)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- freeze: export metadata to a standalone .ducklake file
--
-- If data inlining is enabled, call ducklake.flush_inlined_data() before
-- freezing to ensure all rows are materialized as Parquet files.
CREATE PROCEDURE ducklake.freeze(
    output_path text
)
AS 'MODULE_PATHNAME', 'ducklake_freeze'
LANGUAGE C;

-- cleanup_old_files function
CREATE FUNCTION ducklake.ducklake_cleanup_old_files(
    older_than interval DEFAULT NULL
)
RETURNS bigint
AS 'MODULE_PATHNAME', 'ducklake_cleanup_old_files'
LANGUAGE C;
