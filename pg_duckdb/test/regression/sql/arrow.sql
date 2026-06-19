-- Apache Arrow support via the bundled nanoarrow extension.
-- Verifies that nanoarrow is loaded by default and that read_arrow() works
-- for both Arrow IPC stream (.arrows) and Arrow IPC file (.arrow) formats.

SET duckdb.force_execution = true;

-- Sanity: nanoarrow is loaded as a built-in (no install required).
SET duckdb.force_execution = false;
SELECT * FROM duckdb.query($$ SELECT extension_name, loaded FROM duckdb_extensions() WHERE extension_name = 'nanoarrow' $$);
SET duckdb.force_execution = true;

-- Set up a small dataset and round-trip it through Arrow IPC stream.
\set pwd `pwd`
\set arrow_stream_path '\'' :pwd '/tmp_check/pg_duckdb_arrow.arrows' '\''
\set arrow_file_path   '\'' :pwd '/tmp_check/pg_duckdb_arrow.arrow'  '\''

CREATE TABLE t_arrow(i INT, label TEXT);
INSERT INTO t_arrow SELECT g, 'row_' || g FROM generate_series(1, 5) g;

-- Arrow IPC stream round-trip.
COPY t_arrow TO :arrow_stream_path (FORMAT ARROWS);
SELECT r['i'], r['label'] FROM read_arrow(:arrow_stream_path) r ORDER BY r['i'];

-- Arrow IPC file round-trip.
COPY t_arrow TO :arrow_file_path (FORMAT ARROW);
SELECT r['i'], r['label'] FROM read_arrow(:arrow_file_path) r ORDER BY r['i'];

-- Array variant: union the two files.
SELECT count(*) FROM read_arrow(ARRAY[:arrow_stream_path, :arrow_file_path]);

DROP TABLE t_arrow;
