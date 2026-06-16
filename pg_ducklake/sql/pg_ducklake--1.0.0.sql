CREATE SCHEMA ducklake;

GRANT USAGE ON SCHEMA ducklake TO PUBLIC;

-- ============================================================
-- Types
-- ============================================================

-- ducklake.row: return type of the passthrough functions. The planner routes
-- them to DuckDB, so the I/O functions never run in PG. r['col'] resolves to
-- ducklake.unresolved_type, so row itself carries no casts.
CREATE TYPE ducklake.row;

CREATE FUNCTION ducklake.row_in(cstring) RETURNS ducklake.row
    AS 'MODULE_PATHNAME', 'ducklake_row_in' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.row_out(ducklake.row) RETURNS cstring
    AS 'MODULE_PATHNAME', 'ducklake_row_out' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.row_subscript(internal) RETURNS internal
    AS 'MODULE_PATHNAME', 'ducklake_row_subscript' LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE ducklake.row (
    INTERNALLENGTH = VARIABLE,
    INPUT = ducklake.row_in,
    OUTPUT = ducklake.row_out,
    SUBSCRIPT = ducklake.row_subscript
);

-- ducklake.unresolved_type: element type of a subscript (r['col']). Its PG type
-- is unknown until DuckDB runs, so it carries WITH INOUT casts to every
-- supported type (so r['col']::int parses) and chains under more subscripts.
CREATE TYPE ducklake.unresolved_type;

CREATE FUNCTION ducklake.unresolved_type_in(cstring) RETURNS ducklake.unresolved_type
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_in' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.unresolved_type_out(ducklake.unresolved_type) RETURNS cstring
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_out' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.unresolved_type_subscript(internal) RETURNS internal
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_subscript' LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE ducklake.unresolved_type (
    INTERNALLENGTH = VARIABLE,
    INPUT = ducklake.unresolved_type_in,
    OUTPUT = ducklake.unresolved_type_out,
    SUBSCRIPT = ducklake.unresolved_type_subscript
);

-- "AS ASSIGNMENT" to boolean so an unresolved value can be the final
-- expression of a WHERE clause; the rest are explicit (WITH INOUT).
CREATE CAST (ducklake.unresolved_type AS boolean)        WITH INOUT AS ASSIGNMENT;
CREATE CAST (ducklake.unresolved_type AS boolean[])      WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS char)           WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS char[])         WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS smallint)       WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS smallint[])     WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS integer)        WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS integer[])      WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS bigint)         WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS bigint[])       WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS varchar)        WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS varchar[])      WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS date)           WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS date[])         WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS timestamp)      WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS timestamp[])    WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS timestamptz)    WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS real)           WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS real[])         WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS double precision)   WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS double precision[]) WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS numeric)        WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS numeric[])      WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS uuid)           WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS uuid[])         WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS json)           WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS json[])         WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS jsonb)          WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS jsonb[])        WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS interval)       WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS interval[])     WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS time)           WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS time[])         WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS timetz)         WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS timetz[])       WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS bit)            WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS bit[])          WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS bytea)          WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS bytea[])        WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS text)           WITH INOUT;
CREATE CAST (ducklake.unresolved_type AS text[])         WITH INOUT;

-- ducklake.struct: passthrough type for DuckDB STRUCT/UNION/MAP results with no
-- concrete PG composite type.
CREATE TYPE ducklake.struct;

CREATE FUNCTION ducklake.struct_in(cstring) RETURNS ducklake.struct
    AS 'MODULE_PATHNAME', 'ducklake_struct_in' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.struct_out(ducklake.struct) RETURNS cstring
    AS 'MODULE_PATHNAME', 'ducklake_struct_out' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.struct_subscript(internal) RETURNS internal
    AS 'MODULE_PATHNAME', 'ducklake_struct_subscript' LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE ducklake.struct (
    INTERNALLENGTH = VARIABLE,
    INPUT = ducklake.struct_in,
    OUTPUT = ducklake.struct_out,
    SUBSCRIPT = ducklake.struct_subscript
);

-- ducklake.variant ---------------------------------------------------
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

-- Variant extraction stubs: the planner hook rewrites -> / ->> to these;
-- DuckDB scalar macros expand them to json_extract(_string) on v::VARCHAR.
-- -> returns variant (chainable), ->> returns text.
CREATE FUNCTION ducklake.pg_variant_extract_json(ducklake.variant, text)
    RETURNS ducklake.variant
    AS 'MODULE_PATHNAME', 'ducklake_only_function' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.pg_variant_extract_json_idx(ducklake.variant, int4)
    RETURNS ducklake.variant
    AS 'MODULE_PATHNAME', 'ducklake_only_function' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.pg_variant_extract(ducklake.variant, text)
    RETURNS text
    AS 'MODULE_PATHNAME', 'ducklake_only_function' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.pg_variant_extract_idx(ducklake.variant, int4)
    RETURNS text
    AS 'MODULE_PATHNAME', 'ducklake_only_function' LANGUAGE C IMMUTABLE STRICT;

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
-- Access Methods
-- ============================================================

-- Table access method: ducklake -------------------------------------

CREATE FUNCTION ducklake._am_handler(internal)
    RETURNS table_am_handler
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_am_handler'
    LANGUAGE C;

CREATE ACCESS METHOD ducklake
    TYPE TABLE
    HANDLER ducklake._am_handler;

-- Sorted-index access method: ducklake_sorted -----------------------
-- The user-facing sort API (ducklake.set_sort / reset_sort / get_sort) lives
-- in the Functions section. The opclasses below are STORAGE-only (no operators
-- or functions) so CREATE INDEX accepts common column types without an
-- explicit opclass.

CREATE FUNCTION ducklake._sorted_am_handler(internal)
    RETURNS index_am_handler
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_sorted_am_handler'
    LANGUAGE C;

CREATE ACCESS METHOD ducklake_sorted
    TYPE INDEX
    HANDLER ducklake._sorted_am_handler;

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

-- DuckDB->PG catalog sync: creates/drops pg_class entries when an external
-- DuckDB client changes ducklake metadata tables. Must exist before the
-- Bootstrap section runs _initialize(), which registers a trigger calling it.
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
-- Secrets (cloud storage credentials)
-- A FOREIGN SERVER (public options) + USER MAPPING (secret options) on the
-- ducklake_secret FDW becomes a DuckDB CREATE SECRET on each connection.
-- ============================================================

CREATE FUNCTION ducklake._secret_fdw_handler()
    RETURNS fdw_handler
    AS 'MODULE_PATHNAME', 'ducklake_secret_fdw_handler'
    LANGUAGE C STRICT;

CREATE FUNCTION ducklake._secret_fdw_validator(text[], oid)
    RETURNS void
    AS 'MODULE_PATHNAME', 'ducklake_secret_fdw_validator'
    LANGUAGE C STRICT PARALLEL SAFE;

CREATE FOREIGN DATA WRAPPER ducklake_secret
    HANDLER ducklake._secret_fdw_handler
    VALIDATOR ducklake._secret_fdw_validator;

-- Convenience wrapper: creates the SERVER + USER MAPPING for an s3/gcs/r2 secret
-- and returns the generated server name.
CREATE FUNCTION ducklake.create_s3_secret(
    type          TEXT,
    key_id        TEXT,
    secret        TEXT,
    session_token TEXT DEFAULT '',
    region        TEXT DEFAULT '',
    url_style     TEXT DEFAULT '',
    provider      TEXT DEFAULT '',
    endpoint      TEXT DEFAULT '',
    scope         TEXT DEFAULT '',
    validation    TEXT DEFAULT '',
    use_ssl       TEXT DEFAULT ''
)
RETURNS TEXT
LANGUAGE C AS 'MODULE_PATHNAME', 'ducklake_create_s3_secret';

CREATE FUNCTION ducklake.create_azure_secret(connection_string TEXT, scope TEXT DEFAULT '')
RETURNS TEXT
LANGUAGE C AS 'MODULE_PATHNAME', 'ducklake_create_azure_secret';

-- ============================================================
-- Functions & Procedures
-- Kinds: passthrough (routed to DuckDB as-is), rewrite (regclass ->
-- (schema, table) then routed), duckdb-only (CALL run in DuckDB),
-- native (C, runs in PG), pure SQL (runs in PG).
-- ============================================================

-- Options -----------------------------------------------------------

-- duckdb-only proc
CREATE PROCEDURE ducklake.set_option(
    option_name text,
    value "any"
)
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- duckdb-only proc (table-scoped)
CREATE PROCEDURE ducklake.set_option(
    option_name text,
    value "any",
    scope regclass
)
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- duckdb-only proc (schema-scoped)
CREATE PROCEDURE ducklake.set_option(
    option_name text,
    value "any",
    scope regnamespace
)
AS 'MODULE_PATHNAME', 'ducklake_only_function'
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
ROWS 64
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- Flush -------------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.flush_inlined_data()
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.flush_inlined_data(schema_name text, table_name text)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- rewrite -> flush_inlined_data(text, text)
CREATE FUNCTION ducklake.flush_inlined_data(scope regclass)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.ensure_inlined_data_table(schema_name text, table_name text)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- rewrite -> ensure_inlined_data_table(text, text)
CREATE FUNCTION ducklake.ensure_inlined_data_table(scope regclass)
RETURNS SETOF ducklake.row
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

-- pure SQL. Body references ducklake.ducklake_* metadata tables created by
-- _initialize() at the end of this script; relation resolution is deferred to
-- call time.
CREATE FUNCTION ducklake.get_partition(
    scope regclass,
    OUT partition_key_index bigint,
    OUT column_name varchar,
    OUT transform varchar
)
RETURNS SETOF record
LANGUAGE SQL STABLE PARALLEL SAFE ROWS 16
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
LANGUAGE SQL STABLE PARALLEL SAFE ROWS 16
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
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.current_snapshot()
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.last_committed_snapshot()
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- duckdb-only proc
CREATE PROCEDURE ducklake.set_commit_message(
    author text,
    message text
)
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- Metadata ----------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.table_info()
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.list_files(schema_name text, table_name text)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- rewrite -> list_files(text, text)
CREATE FUNCTION ducklake.list_files(scope regclass)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- Time Travel -------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.time_travel(table_name text, version bigint)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.time_travel(table_name text, "timestamp" timestamptz)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough (schema + table)
CREATE FUNCTION ducklake.time_travel(schema_name text, table_name text, version bigint)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough (schema + table)
CREATE FUNCTION ducklake.time_travel(schema_name text, table_name text, "timestamp" timestamptz)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- rewrite -> time_travel(text, text, bigint)
CREATE FUNCTION ducklake.time_travel(scope regclass, version bigint)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- rewrite -> time_travel(text, text, timestamptz)
CREATE FUNCTION ducklake.time_travel(scope regclass, "timestamp" timestamptz)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- Change Feed -------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.table_insertions(
    schema_name text, table_name text,
    start_snapshot bigint, end_snapshot bigint
)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.table_insertions(
    schema_name text, table_name text,
    start_snapshot timestamptz, end_snapshot timestamptz
)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- rewrite -> table_insertions(text, text, bigint, bigint)
CREATE FUNCTION ducklake.table_insertions(
    scope regclass, start_snapshot bigint, end_snapshot bigint)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- rewrite -> table_insertions(text, text, timestamptz, timestamptz)
CREATE FUNCTION ducklake.table_insertions(
    scope regclass, start_snapshot timestamptz, end_snapshot timestamptz)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.table_deletions(
    schema_name text, table_name text,
    start_snapshot bigint, end_snapshot bigint
)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.table_deletions(
    schema_name text, table_name text,
    start_snapshot timestamptz, end_snapshot timestamptz
)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- rewrite -> table_deletions(text, text, bigint, bigint)
CREATE FUNCTION ducklake.table_deletions(
    scope regclass, start_snapshot bigint, end_snapshot bigint)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- rewrite -> table_deletions(text, text, timestamptz, timestamptz)
CREATE FUNCTION ducklake.table_deletions(
    scope regclass, start_snapshot timestamptz, end_snapshot timestamptz)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.table_changes(
    schema_name text, table_name text,
    start_snapshot bigint, end_snapshot bigint
)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.table_changes(
    schema_name text, table_name text,
    start_snapshot timestamptz, end_snapshot timestamptz
)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- rewrite -> table_changes(text, text, bigint, bigint)
CREATE FUNCTION ducklake.table_changes(
    scope regclass, start_snapshot bigint, end_snapshot bigint)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- rewrite -> table_changes(text, text, timestamptz, timestamptz)
CREATE FUNCTION ducklake.table_changes(
    scope regclass, start_snapshot timestamptz, end_snapshot timestamptz)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- Cleanup -----------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.cleanup_old_files()
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.cleanup_old_files(older_than interval)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.cleanup_orphaned_files()
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- Maintenance -------------------------------------------------------

-- passthrough
CREATE FUNCTION ducklake.merge_adjacent_files()
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.merge_adjacent_files(schema_name text, table_name text)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- rewrite -> merge_adjacent_files(text, text)
CREATE FUNCTION ducklake.merge_adjacent_files(scope regclass)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.rewrite_data_files()
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.rewrite_data_files(schema_name text, table_name text)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- rewrite -> rewrite_data_files(text, text)
CREATE FUNCTION ducklake.rewrite_data_files(scope regclass)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_function_mapping'
LANGUAGE C;

-- passthrough
CREATE FUNCTION ducklake.expire_snapshots()
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- Diagnostics -------------------------------------------------------

-- native SRF: planner/exec counters for the direct-insert optimization.
-- Counters live in shared memory and persist until postmaster restart or
-- ducklake.reset_direct_insert_stats(). Fixed row set: matched_unnest/
-- matched_values + one unmatched row per non-ok reason (9 rows total).
CREATE FUNCTION ducklake.direct_insert_stats()
    RETURNS TABLE (pattern text, reason text, count bigint)
    LANGUAGE C VOLATILE PARALLEL RESTRICTED ROWS 9
    AS 'MODULE_PATHNAME', 'ducklake_direct_insert_stats';

-- native
CREATE FUNCTION ducklake.reset_direct_insert_stats()
    RETURNS void
    LANGUAGE C VOLATILE
    AS 'MODULE_PATHNAME', 'ducklake_reset_direct_insert_stats';

-- Freeze ------------------------------------------------------------

-- native proc: export metadata to a standalone .ducklake file.
-- If data inlining is enabled, call ducklake.flush_inlined_data() before
-- freezing to ensure all rows are materialized as Parquet files.
CREATE PROCEDURE ducklake.freeze(
    output_path text
)
AS 'MODULE_PATHNAME', 'ducklake_freeze'
LANGUAGE C;

-- Virtual Columns ---------------------------------------------------

-- Virtual column accessors: scalar DuckDB-only stubs; a DuckDB macro
-- expands each to the corresponding virtual column (e.g. row_id() -> rowid).
-- VOLATILE (not IMMUTABLE): they return a different value per row, so the
-- planner must not constant-fold them.
CREATE FUNCTION ducklake.rowid()
    RETURNS bigint
    AS 'MODULE_PATHNAME', 'ducklake_only_function' LANGUAGE C VOLATILE;
CREATE FUNCTION ducklake.snapshot_id()
    RETURNS bigint
    AS 'MODULE_PATHNAME', 'ducklake_only_function' LANGUAGE C VOLATILE;
CREATE FUNCTION ducklake.filename()
    RETURNS text
    AS 'MODULE_PATHNAME', 'ducklake_only_function' LANGUAGE C VOLATILE;
CREATE FUNCTION ducklake.file_row_number()
    RETURNS bigint
    AS 'MODULE_PATHNAME', 'ducklake_only_function' LANGUAGE C VOLATILE;
CREATE FUNCTION ducklake.file_index()
    RETURNS bigint
    AS 'MODULE_PATHNAME', 'ducklake_only_function' LANGUAGE C VOLATILE;

-- ============================================================
-- File readers
-- ============================================================
-- Installed in the ducklake schema; DuckDB routing is keyed on prosrc, not the
-- schema.

CREATE FUNCTION ducklake.read_csv(path text, all_varchar BOOLEAN DEFAULT FALSE,
                                               allow_quoted_nulls BOOLEAN DEFAULT TRUE,
                                               auto_detect BOOLEAN DEFAULT TRUE,
                                               auto_type_candidates TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               compression VARCHAR DEFAULT 'auto',
                                               dateformat VARCHAR DEFAULT '',
                                               decimal_separator VARCHAR DEFAULT '.',
                                               delim VARCHAR DEFAULT ',',
                                               escape VARCHAR DEFAULT '"',
                                               filename BOOLEAN DEFAULT FALSE,
                                               force_not_null TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               header BOOLEAN DEFAULT FALSE,
                                               hive_partitioning BOOLEAN DEFAULT FALSE,
                                               ignore_errors BOOLEAN DEFAULT FALSE,
                                               max_line_size BIGINT DEFAULT 2097152,
                                               names TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               new_line VARCHAR DEFAULT '',
                                               normalize_names BOOLEAN DEFAULT FALSE,
                                               null_padding BOOLEAN DEFAULT FALSE,
                                               nullstr TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               parallel BOOLEAN DEFAULT FALSE,
                                               quote VARCHAR DEFAULT '"',
                                               sample_size BIGINT DEFAULT 20480,
                                               sep VARCHAR DEFAULT ',',
                                               skip BIGINT DEFAULT 0,
                                               timestampformat VARCHAR DEFAULT '',
                                               types TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

CREATE FUNCTION ducklake.read_csv(path text[], all_varchar BOOLEAN DEFAULT FALSE,
                                                  allow_quoted_nulls BOOLEAN DEFAULT TRUE,
                                                  auto_detect BOOLEAN DEFAULT TRUE,
                                                  auto_type_candidates TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  compression VARCHAR DEFAULT 'auto',
                                                  dateformat VARCHAR DEFAULT '',
                                                  decimal_separator VARCHAR DEFAULT '.',
                                                  delim VARCHAR DEFAULT ',',
                                                  escape VARCHAR DEFAULT '"',
                                                  filename BOOLEAN DEFAULT FALSE,
                                                  force_not_null TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  header BOOLEAN DEFAULT FALSE,
                                                  hive_partitioning BOOLEAN DEFAULT FALSE,
                                                  ignore_errors BOOLEAN DEFAULT FALSE,
                                                  max_line_size BIGINT DEFAULT 2097152,
                                                  names TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  new_line VARCHAR DEFAULT '',
                                                  normalize_names BOOLEAN DEFAULT FALSE,
                                                  null_padding BOOLEAN DEFAULT FALSE,
                                                  nullstr TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  parallel BOOLEAN DEFAULT FALSE,
                                                  quote VARCHAR DEFAULT '"',
                                                  sample_size BIGINT DEFAULT 20480,
                                                  sep VARCHAR DEFAULT ',',
                                                  skip BIGINT DEFAULT 0,
                                                  timestampformat VARCHAR DEFAULT '',
                                                  types TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

CREATE FUNCTION ducklake.read_parquet(path text, binary_as_string BOOLEAN DEFAULT FALSE,
                                                   filename BOOLEAN DEFAULT FALSE,
                                                   file_row_number BOOLEAN DEFAULT FALSE,
                                                   hive_partitioning BOOLEAN DEFAULT FALSE,
                                                   union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

CREATE FUNCTION ducklake.read_parquet(path text[], binary_as_string BOOLEAN DEFAULT FALSE,
                                                     filename BOOLEAN DEFAULT FALSE,
                                                     file_row_number BOOLEAN DEFAULT FALSE,
                                                     hive_partitioning BOOLEAN DEFAULT FALSE,
                                                     union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF ducklake.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'ducklake_only_function'
LANGUAGE C;

-- ============================================================
-- Admin utilities
-- ============================================================

-- Tear down and recreate the per-backend DuckDB instance.
CREATE PROCEDURE ducklake.recycle_ddb()
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME', 'ducklake_recycle_ddb';

-- Run an arbitrary statement against the embedded DuckDB instance, ignoring its
-- result. Unstable/debug API: granted to the superuser role only (below).
CREATE FUNCTION ducklake.raw_query(query TEXT)
    RETURNS void
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME', 'ducklake_duckdb_raw_query';
REVOKE ALL ON FUNCTION ducklake.raw_query(TEXT) FROM PUBLIC;

-- Run an arbitrary DuckDB query and return its rows. Unstable/debug API:
-- granted to the superuser role only (below).
CREATE FUNCTION ducklake.query(query TEXT)
    RETURNS SETOF ducklake.row
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_only_function'
    LANGUAGE C;
REVOKE ALL ON FUNCTION ducklake.query(TEXT) FROM PUBLIC;

-- ============================================================
-- Bootstrap
-- ============================================================

-- Initializes the DuckLake catalog (attaches DuckDB). Requires
-- ducklake._snapshot_trigger() (Event Triggers) and the ducklake.* metadata
-- reading functions to already exist; initialization registers a trigger on
-- ducklake_snapshot that calls _snapshot_trigger().
CREATE FUNCTION ducklake._initialize()
    RETURNS void
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME', 'ducklake_initialize'
    LANGUAGE C;

DO $$
BEGIN
    PERFORM ducklake._initialize();
END
$$;

-- ============================================================
-- Access control / roles
-- ============================================================

-- Predefined NOLOGIN group roles, named via the
-- ducklake.superuser_role/writer_role/reader_role GUCs (empty string skips
-- creation). Runs after _initialize() so the schema grants cover the tables it
-- created. Roles are cluster-global and left behind on DROP EXTENSION.
DO $$
DECLARE
    superuser_role text;
    role_names text[];
    role_name text;
BEGIN
    superuser_role := current_setting('ducklake.superuser_role');
    role_names := ARRAY[
        superuser_role,
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

    FOREACH role_name IN ARRAY role_names LOOP
        IF role_name != '' THEN
            EXECUTE format('GRANT ALL ON ALL TABLES IN SCHEMA ducklake TO %I', role_name);
            EXECUTE format('GRANT ALL ON ALL SEQUENCES IN SCHEMA ducklake TO %I', role_name);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA ducklake GRANT ALL ON TABLES TO %I', role_name);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA ducklake GRANT ALL ON SEQUENCES TO %I', role_name);
        END IF;
    END LOOP;

    -- The arbitrary-DuckDB-SQL escape hatches are REVOKE'd from PUBLIC at
    -- definition; grant them to the superuser role only.
    IF superuser_role != '' THEN
        EXECUTE format('GRANT EXECUTE ON FUNCTION ducklake.query(text) TO %I', superuser_role);
        EXECUTE format('GRANT EXECUTE ON FUNCTION ducklake.raw_query(text) TO %I', superuser_role);
    END IF;
END
$$;
