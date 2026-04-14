CREATE SCHEMA ducklake;

GRANT USAGE ON SCHEMA ducklake TO PUBLIC;

-- ============================================================
-- Table Access Method
-- ============================================================

CREATE FUNCTION ducklake._am_handler(internal)
    RETURNS table_am_handler
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_am_handler'
    LANGUAGE C;

CREATE ACCESS METHOD ducklake
    TYPE TABLE
    HANDLER ducklake._am_handler;

-- ============================================================
-- Sorted Index Access Method
-- ============================================================

CREATE FUNCTION ducklake._sorted_am_handler(internal)
    RETURNS index_am_handler
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_sorted_am_handler'
    LANGUAGE C;

CREATE ACCESS METHOD ducklake_sorted
    TYPE INDEX
    HANDLER ducklake._sorted_am_handler;

-- Default operator family and classes for ducklake_sorted.
-- These are STORAGE-only (no operators or functions) so that CREATE INDEX
-- accepts columns of common types without requiring explicit opclass.

CREATE OPERATOR FAMILY ducklake.sorted_ops USING ducklake_sorted;

CREATE OPERATOR CLASS ducklake.bool_sorted_ops DEFAULT FOR TYPE bool
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE bool;
CREATE OPERATOR CLASS ducklake.int2_sorted_ops DEFAULT FOR TYPE int2
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE int2;
CREATE OPERATOR CLASS ducklake.int4_sorted_ops DEFAULT FOR TYPE int4
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE int4;
CREATE OPERATOR CLASS ducklake.int8_sorted_ops DEFAULT FOR TYPE int8
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE int8;
CREATE OPERATOR CLASS ducklake.float4_sorted_ops DEFAULT FOR TYPE float4
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE float4;
CREATE OPERATOR CLASS ducklake.float8_sorted_ops DEFAULT FOR TYPE float8
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE float8;
CREATE OPERATOR CLASS ducklake.numeric_sorted_ops DEFAULT FOR TYPE numeric
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE numeric;
CREATE OPERATOR CLASS ducklake.text_sorted_ops DEFAULT FOR TYPE text
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE text;
CREATE OPERATOR CLASS ducklake.varchar_sorted_ops DEFAULT FOR TYPE varchar
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE varchar;
CREATE OPERATOR CLASS ducklake.bpchar_sorted_ops DEFAULT FOR TYPE bpchar
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE bpchar;
CREATE OPERATOR CLASS ducklake.date_sorted_ops DEFAULT FOR TYPE date
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE date;
CREATE OPERATOR CLASS ducklake.timestamp_sorted_ops DEFAULT FOR TYPE timestamp
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE timestamp;
CREATE OPERATOR CLASS ducklake.timestamptz_sorted_ops DEFAULT FOR TYPE timestamptz
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE timestamptz;
CREATE OPERATOR CLASS ducklake.interval_sorted_ops DEFAULT FOR TYPE interval
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE interval;
CREATE OPERATOR CLASS ducklake.uuid_sorted_ops DEFAULT FOR TYPE uuid
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE uuid;
CREATE OPERATOR CLASS ducklake.oid_sorted_ops DEFAULT FOR TYPE oid
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE oid;
CREATE OPERATOR CLASS ducklake.bytea_sorted_ops DEFAULT FOR TYPE bytea
    USING ducklake_sorted FAMILY ducklake.sorted_ops AS STORAGE bytea;

-- ============================================================
-- Event Triggers
-- ============================================================

CREATE FUNCTION ducklake._create_table_trigger()
    RETURNS event_trigger
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_create_table_trigger'
    LANGUAGE C;

CREATE EVENT TRIGGER ducklake_create_table_trigger ON ddl_command_end
    WHEN tag IN ('CREATE TABLE', 'CREATE TABLE AS')
    EXECUTE FUNCTION ducklake._create_table_trigger();

CREATE FUNCTION ducklake._drop_table_trigger()
    RETURNS event_trigger
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_drop_table_trigger'
    LANGUAGE C;

CREATE EVENT TRIGGER ducklake_drop_table_trigger ON sql_drop
    EXECUTE FUNCTION ducklake._drop_table_trigger();

CREATE FUNCTION ducklake._alter_table_trigger()
    RETURNS event_trigger
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_alter_table_trigger'
    LANGUAGE C;

CREATE EVENT TRIGGER ducklake_alter_table_trigger ON ddl_command_end
    WHEN tag IN ('ALTER TABLE')
    EXECUTE FUNCTION ducklake._alter_table_trigger();

CREATE FUNCTION ducklake._comment_trigger()
    RETURNS event_trigger
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_comment_trigger'
    LANGUAGE C;

CREATE EVENT TRIGGER ducklake_comment_trigger ON ddl_command_end
    WHEN tag IN ('COMMENT')
    EXECUTE FUNCTION ducklake._comment_trigger();

-- Metadata sync trigger function: DuckDB->PG catalog sync.
-- When an external DuckDB client creates/drops tables (writing directly to
-- ducklake metadata tables), this trigger creates/drops corresponding
-- pg_class entries so the tables become visible from PostgreSQL.
-- The trigger itself is created by the metadata manager during initialization.
CREATE FUNCTION ducklake._snapshot_trigger()
    RETURNS trigger
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_snapshot_trigger'
    LANGUAGE C;

-- ============================================================
-- Foreign Data Wrapper
-- ============================================================

CREATE FUNCTION ducklake._fdw_handler()
    RETURNS fdw_handler
    AS 'MODULE_PATHNAME', 'ducklake_fdw_handler'
    LANGUAGE C STRICT;

CREATE FUNCTION ducklake._fdw_validator(text[], oid)
    RETURNS void
    AS 'MODULE_PATHNAME', 'ducklake_fdw_validator'
    LANGUAGE C STRICT PARALLEL SAFE;

CREATE FOREIGN DATA WRAPPER ducklake_fdw
    HANDLER ducklake._fdw_handler
    VALIDATOR ducklake._fdw_validator;

-- ============================================================
-- Functions & Procedures
--
-- Kind legend:
--   passthrough     SQL stub; pg_duckdb routes the query to DuckDB as-is
--   rewrite         planner rewrites regclass -> (schema, table) then routes
--   duckdb-only     CALL intercepted by utility hook, executed in DuckDB
--   native          procedure runs in PostgreSQL (C language)
--   pure SQL        executes entirely in PostgreSQL
-- ============================================================

-- Options -----------------------------------------------------------

-- duckdb-only proc
CREATE PROCEDURE ducklake.set_option(
    option_name text,
    value "any"
)
AS 'MODULE_PATHNAME', 'ducklake_only_procedure'
LANGUAGE C;

-- duckdb-only proc (table-scoped)
CREATE PROCEDURE ducklake.set_option(
    option_name text,
    value "any",
    scope regclass
)
AS 'MODULE_PATHNAME', 'ducklake_only_procedure'
LANGUAGE C;

-- duckdb-only proc (schema-scoped)
CREATE PROCEDURE ducklake.set_option(
    option_name text,
    value "any",
    scope regnamespace
)
AS 'MODULE_PATHNAME', 'ducklake_only_procedure'
LANGUAGE C;

-- passthrough
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

-- Flush -------------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.flush_inlined_data()
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.flush_inlined_data(schema_name text, table_name text)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- rewrite -> flush_inlined_data(text, text)
CREATE FUNCTION ducklake.flush_inlined_data(scope regclass)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.ensure_inlined_data_table(schema_name text, table_name text)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- rewrite -> ensure_inlined_data_table(text, text)
CREATE FUNCTION ducklake.ensure_inlined_data_table(scope regclass)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- Partitioning ------------------------------------------------------

-- native proc
CREATE PROCEDURE ducklake.set_partition(scope regclass, VARIADIC partition_by text[])
AS 'MODULE_PATHNAME', 'ducklake_set_partition'
LANGUAGE C;

-- native proc
CREATE PROCEDURE ducklake.reset_partition(scope regclass)
AS 'MODULE_PATHNAME', 'ducklake_reset_partition'
LANGUAGE C;

-- pure SQL
CREATE FUNCTION ducklake.get_partition(
    scope regclass,
    OUT partition_key_index bigint,
    OUT column_name varchar,
    OUT transform varchar
)
RETURNS SETOF record
LANGUAGE SQL STABLE
SET search_path = pg_catalog, pg_temp
AS $$
SELECT pc.partition_key_index, c.column_name, pc.transform
FROM ducklake.ducklake_partition_info pi
JOIN ducklake.ducklake_partition_column pc USING (partition_id)
JOIN ducklake.ducklake_column c
  ON pc.column_id = c.column_id AND pc.table_id = c.table_id
JOIN ducklake.ducklake_table t ON pi.table_id = t.table_id
JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
WHERE t.table_name = (SELECT relname FROM pg_class WHERE oid = scope)
  AND s.schema_name = (SELECT nspname FROM pg_namespace
                        WHERE oid = (SELECT relnamespace FROM pg_class WHERE oid = scope))
  AND pi.end_snapshot IS NULL
  AND c.end_snapshot IS NULL
  AND t.end_snapshot IS NULL
  AND s.end_snapshot IS NULL
ORDER BY pc.partition_key_index
$$;

-- Sorted Tables ----------------------------------------------------

-- native proc
CREATE PROCEDURE ducklake.set_sort(scope regclass, VARIADIC sorted_by text[])
AS 'MODULE_PATHNAME', 'ducklake_set_sort'
LANGUAGE C;

-- native proc
CREATE PROCEDURE ducklake.reset_sort(scope regclass)
AS 'MODULE_PATHNAME', 'ducklake_reset_sort'
LANGUAGE C;

-- pure SQL
CREATE FUNCTION ducklake.get_sort(
    scope regclass,
    OUT sort_key_index bigint,
    OUT expression varchar,
    OUT direction varchar,
    OUT null_order varchar
)
RETURNS SETOF record
LANGUAGE SQL STABLE
SET search_path = pg_catalog, pg_temp
AS $$
SELECT se.sort_key_index, se.expression, se.sort_direction, se.null_order
FROM ducklake.ducklake_sort_info si
JOIN ducklake.ducklake_sort_expression se USING (sort_id)
JOIN ducklake.ducklake_table t ON si.table_id = t.table_id
JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
WHERE t.table_name = (SELECT relname FROM pg_class WHERE oid = scope)
  AND s.schema_name = (SELECT nspname FROM pg_namespace
                        WHERE oid = (SELECT relnamespace FROM pg_class WHERE oid = scope))
  AND si.end_snapshot IS NULL
  AND t.end_snapshot IS NULL
  AND s.end_snapshot IS NULL
ORDER BY se.sort_key_index
$$;

-- Snapshots ---------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.snapshots()
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.current_snapshot()
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.last_committed_snapshot()
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- duckdb-only proc
CREATE PROCEDURE ducklake.set_commit_message(
    author text,
    message text
)
AS 'MODULE_PATHNAME', 'ducklake_only_procedure'
LANGUAGE C;

-- Metadata ----------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.table_info()
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.list_files(schema_name text, table_name text)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- rewrite -> list_files(text, text)
CREATE FUNCTION ducklake.list_files(scope regclass)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- Time Travel -------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.time_travel(table_name text, version bigint)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.time_travel(table_name text, "timestamp" timestamptz)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough (schema + table)
CREATE FUNCTION ducklake.time_travel(schema_name text, table_name text, version bigint)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough (schema + table)
CREATE FUNCTION ducklake.time_travel(schema_name text, table_name text, "timestamp" timestamptz)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- rewrite -> time_travel(text, text, bigint)
CREATE FUNCTION ducklake.time_travel(scope regclass, version bigint)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- rewrite -> time_travel(text, text, timestamptz)
CREATE FUNCTION ducklake.time_travel(scope regclass, "timestamp" timestamptz)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- Change Feed -------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.table_insertions(
    schema_name text, table_name text,
    start_snapshot bigint, end_snapshot bigint
)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.table_insertions(
    schema_name text, table_name text,
    start_snapshot timestamptz, end_snapshot timestamptz
)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- rewrite -> table_insertions(text, text, bigint, bigint)
CREATE FUNCTION ducklake.table_insertions(
    scope regclass, start_snapshot bigint, end_snapshot bigint)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- rewrite -> table_insertions(text, text, timestamptz, timestamptz)
CREATE FUNCTION ducklake.table_insertions(
    scope regclass, start_snapshot timestamptz, end_snapshot timestamptz)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.table_deletions(
    schema_name text, table_name text,
    start_snapshot bigint, end_snapshot bigint
)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.table_deletions(
    schema_name text, table_name text,
    start_snapshot timestamptz, end_snapshot timestamptz
)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- rewrite -> table_deletions(text, text, bigint, bigint)
CREATE FUNCTION ducklake.table_deletions(
    scope regclass, start_snapshot bigint, end_snapshot bigint)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- rewrite -> table_deletions(text, text, timestamptz, timestamptz)
CREATE FUNCTION ducklake.table_deletions(
    scope regclass, start_snapshot timestamptz, end_snapshot timestamptz)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.table_changes(
    schema_name text, table_name text,
    start_snapshot bigint, end_snapshot bigint
)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.table_changes(
    schema_name text, table_name text,
    start_snapshot timestamptz, end_snapshot timestamptz
)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- rewrite -> table_changes(text, text, bigint, bigint)
CREATE FUNCTION ducklake.table_changes(
    scope regclass, start_snapshot bigint, end_snapshot bigint)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- rewrite -> table_changes(text, text, timestamptz, timestamptz)
CREATE FUNCTION ducklake.table_changes(
    scope regclass, start_snapshot timestamptz, end_snapshot timestamptz)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- Cleanup -----------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.cleanup_old_files()
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.cleanup_old_files(older_than interval)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.cleanup_orphaned_files()
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- Maintenance -------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.merge_adjacent_files()
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.merge_adjacent_files(schema_name text, table_name text)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- rewrite -> merge_adjacent_files(text, text)
CREATE FUNCTION ducklake.merge_adjacent_files(scope regclass)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.rewrite_data_files()
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.rewrite_data_files(schema_name text, table_name text)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- rewrite -> rewrite_data_files(text, text)
CREATE FUNCTION ducklake.rewrite_data_files(scope regclass)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.expire_snapshots()
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS '$libdir/pg_duckdb', 'duckdb_only_function'
LANGUAGE C;

-- Freeze ------------------------------------------------------------

-- native proc: export metadata to a standalone .ducklake file.
-- If data inlining is enabled, call ducklake.flush_inlined_data() before
-- freezing to ensure all rows are materialized as Parquet files.
CREATE PROCEDURE ducklake.freeze(
    output_path text
)
AS 'MODULE_PATHNAME', 'ducklake_freeze'
LANGUAGE C;

-- ============================================================
-- Variant Type
-- ============================================================

-- DuckDB-only column type for ducklake tables.
-- I/O functions store text representation; actual data is handled by DuckDB.
CREATE FUNCTION ducklake._variant_in(cstring) RETURNS ducklake.variant
    AS 'MODULE_PATHNAME', 'ducklake_variant_in' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake._variant_out(ducklake.variant) RETURNS cstring
    AS 'MODULE_PATHNAME', 'ducklake_variant_out' LANGUAGE C IMMUTABLE STRICT;
CREATE TYPE ducklake.variant (
    INTERNALLENGTH = VARIABLE,
    INPUT = ducklake._variant_in,
    OUTPUT = ducklake._variant_out
);

-- Variant field extraction (DuckDB-only stubs).
-- The planner hook rewrites -> / ->> operators to the corresponding FuncExpr
-- nodes before pg_duckdb deparses the query. In DuckDB, scalar macros expand
-- these to json_extract / json_extract_string calls on v::VARCHAR.
--
-- -> returns variant (preserves JSON structure, enables chaining).
-- ->> returns text (extracts as string).
CREATE FUNCTION ducklake.pg_variant_extract_json(ducklake.variant, text)
    RETURNS ducklake.variant
    AS '$libdir/pg_duckdb', 'duckdb_only_function' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.pg_variant_extract_json_idx(ducklake.variant, int4)
    RETURNS ducklake.variant
    AS '$libdir/pg_duckdb', 'duckdb_only_function' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.pg_variant_extract(ducklake.variant, text)
    RETURNS text
    AS '$libdir/pg_duckdb', 'duckdb_only_function' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.pg_variant_extract_idx(ducklake.variant, int4)
    RETURNS text
    AS '$libdir/pg_duckdb', 'duckdb_only_function' LANGUAGE C IMMUTABLE STRICT;

-- Operators -> and ->> for variant field extraction (PG JSON-like syntax).
-- Placed in pg_catalog so they are always in search_path.
-- -> returns variant, ->> returns text (matching PG json/jsonb semantics).
CREATE OPERATOR pg_catalog.-> (
    LEFTARG = ducklake.variant, RIGHTARG = text,
    FUNCTION = ducklake.pg_variant_extract_json);
CREATE OPERATOR pg_catalog.-> (
    LEFTARG = ducklake.variant, RIGHTARG = int4,
    FUNCTION = ducklake.pg_variant_extract_json_idx);
CREATE OPERATOR pg_catalog.->> (
    LEFTARG = ducklake.variant, RIGHTARG = text,
    FUNCTION = ducklake.pg_variant_extract);
CREATE OPERATOR pg_catalog.->> (
    LEFTARG = ducklake.variant, RIGHTARG = int4,
    FUNCTION = ducklake.pg_variant_extract_idx);

-- ============================================================
-- Virtual Columns
-- ============================================================

-- DuckLake virtual column accessors.  These are scalar DuckDB-only stubs;
-- in DuckDB a scalar macro expands each to the corresponding virtual column
-- reference (e.g. row_id() -> rowid).  Use in SELECT to access virtual
-- columns that are not part of the regular column list.
CREATE FUNCTION ducklake.rowid()
    RETURNS bigint
    AS '$libdir/pg_duckdb', 'duckdb_only_function' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.snapshot_id()
    RETURNS bigint
    AS '$libdir/pg_duckdb', 'duckdb_only_function' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.filename()
    RETURNS text
    AS '$libdir/pg_duckdb', 'duckdb_only_function' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.file_row_number()
    RETURNS bigint
    AS '$libdir/pg_duckdb', 'duckdb_only_function' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.file_index()
    RETURNS bigint
    AS '$libdir/pg_duckdb', 'duckdb_only_function' LANGUAGE C IMMUTABLE STRICT;

-- ============================================================
-- Bootstrap
-- ============================================================

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

-- Predefined roles for DuckLake access control.
-- https://ducklake.select/docs/stable/duckdb/guides/access_control
--
-- Role names are configured via GUCs (ducklake.superuser_role,
-- ducklake.writer_role, ducklake.reader_role). Set an empty string to skip
-- creating that role. Defaults: ducklake_superuser, ducklake_writer,
-- ducklake_reader.
--
-- These are GROUP roles (NOLOGIN). Create LOGIN users and grant membership:
--   CREATE USER analyst IN ROLE ducklake_reader;
DO $$
DECLARE
    duckdb_role text;
    role_names text[];
    role_name text;
BEGIN
    role_names := ARRAY[
        current_setting('ducklake.superuser_role'),
        current_setting('ducklake.writer_role'),
        current_setting('ducklake.reader_role')
    ];

    FOREACH role_name IN ARRAY role_names LOOP
        IF role_name != '' AND NOT EXISTS (
            SELECT FROM pg_catalog.pg_roles WHERE rolname = role_name
        ) THEN
            EXECUTE 'CREATE ROLE ' || quote_ident(role_name);
        END IF;
    END LOOP;

    SELECT current_setting('duckdb.postgres_role') INTO duckdb_role;
    IF duckdb_role != '' AND EXISTS (
        SELECT FROM pg_catalog.pg_roles WHERE rolname = duckdb_role
    ) THEN
        FOREACH role_name IN ARRAY role_names LOOP
            IF role_name != '' THEN
                EXECUTE format('GRANT %I TO %I', duckdb_role, role_name);
            END IF;
        END LOOP;
    END IF;

    FOREACH role_name IN ARRAY role_names LOOP
        IF role_name != '' THEN
            EXECUTE format('GRANT ALL ON ALL TABLES IN SCHEMA ducklake TO %I', role_name);
            EXECUTE format('GRANT ALL ON ALL SEQUENCES IN SCHEMA ducklake TO %I', role_name);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA ducklake GRANT ALL ON TABLES TO %I', role_name);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA ducklake GRANT ALL ON SEQUENCES TO %I', role_name);
        END IF;
    END LOOP;
END
$$;
