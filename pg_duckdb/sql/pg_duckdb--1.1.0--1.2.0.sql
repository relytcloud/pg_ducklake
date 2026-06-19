-- Apache Arrow IPC reader backed by the bundled paleolimbot/duckdb-nanoarrow
-- extension. Mirrors read_parquet: planner-routed duckdb_only_function stubs.

CREATE FUNCTION @extschema@.read_arrow(path text)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.read_arrow(path text[])
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;
