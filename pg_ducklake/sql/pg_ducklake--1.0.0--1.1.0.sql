-- Operators on ducklake.unresolved_type, mirroring pg_duckdb's set on
-- duckdb.unresolved_type. Subscript results (r['col']) carry this type, so
-- without these PostgreSQL rejects expressions like r['a'] * interval or
-- WHERE r['b'] > 5 at parse time even though execution happens in DuckDB.
-- The backing functions are DuckDB-only stubs that error if PostgreSQL ever
-- executes them.

-- Dummy functions for binary operators with unresolved type on either or both
-- sides, and for prefix operators.
CREATE FUNCTION ducklake.unresolved_type_operator(ducklake.unresolved_type, "any") RETURNS ducklake.unresolved_type
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.unresolved_type_operator("any", ducklake.unresolved_type) RETURNS ducklake.unresolved_type
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.unresolved_type_operator(ducklake.unresolved_type, ducklake.unresolved_type) RETURNS ducklake.unresolved_type
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.unresolved_type_operator(ducklake.unresolved_type) RETURNS ducklake.unresolved_type
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ducklake.unresolved_type_operator_bool(ducklake.unresolved_type, "any") RETURNS boolean
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.unresolved_type_operator_bool("any", ducklake.unresolved_type) RETURNS boolean
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION ducklake.unresolved_type_operator_bool(ducklake.unresolved_type, ducklake.unresolved_type) RETURNS boolean
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

-- prefix operators + and -
CREATE OPERATOR pg_catalog.+ (
    RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog.- (
    RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator
);

-- comparison operators
CREATE OPERATOR pg_catalog.= (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.= (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = "any",
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.= (
    LEFTARG = "any", RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.<> (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.<> (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = "any",
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.<> (
    LEFTARG = "any", RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.< (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.< (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = "any",
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.< (
    LEFTARG = "any", RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.<= (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.<= (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = "any",
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.<= (
    LEFTARG = "any", RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.> (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.> (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = "any",
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.> (
    LEFTARG = "any", RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.>= (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.>= (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = "any",
    FUNCTION = ducklake.unresolved_type_operator_bool
);
CREATE OPERATOR pg_catalog.>= (
    LEFTARG = "any", RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator_bool
);

-- binary math operators
CREATE OPERATOR pg_catalog.+ (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog.+ (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = "any",
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog.+ (
    LEFTARG = "any", RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog.- (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog.- (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = "any",
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog.- (
    LEFTARG = "any", RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog.* (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog.* (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = "any",
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog.* (
    LEFTARG = "any", RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog./ (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog./ (
    LEFTARG = ducklake.unresolved_type, RIGHTARG = "any",
    FUNCTION = ducklake.unresolved_type_operator
);
CREATE OPERATOR pg_catalog./ (
    LEFTARG = "any", RIGHTARG = ducklake.unresolved_type,
    FUNCTION = ducklake.unresolved_type_operator
);

-- B-tree operator class so unresolved values work in ORDER BY
CREATE FUNCTION ducklake.unresolved_type_btree_cmp(ducklake.unresolved_type, ducklake.unresolved_type) RETURNS int
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR CLASS ducklake.unresolved_type_ops
DEFAULT FOR TYPE ducklake.unresolved_type USING btree AS
    OPERATOR 1 < (ducklake.unresolved_type, ducklake.unresolved_type),
    OPERATOR 2 <= (ducklake.unresolved_type, ducklake.unresolved_type),
    OPERATOR 3 = (ducklake.unresolved_type, ducklake.unresolved_type),
    OPERATOR 4 >= (ducklake.unresolved_type, ducklake.unresolved_type),
    OPERATOR 5 > (ducklake.unresolved_type, ducklake.unresolved_type),
    FUNCTION 1 ducklake.unresolved_type_btree_cmp(ducklake.unresolved_type, ducklake.unresolved_type);

-- Hash operator class so unresolved values work in GROUP BY and DISTINCT
CREATE FUNCTION ducklake.unresolved_type_hash(ducklake.unresolved_type) RETURNS int
    AS 'MODULE_PATHNAME', 'ducklake_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR CLASS ducklake.unresolved_type_hash_ops
DEFAULT FOR TYPE ducklake.unresolved_type USING hash AS
    OPERATOR 1 = (ducklake.unresolved_type, ducklake.unresolved_type),
    FUNCTION 1 ducklake.unresolved_type_hash(ducklake.unresolved_type);

-- DuckDB-dialect partition transform functions (GitHub issue #223).
--
-- DuckLake generates flush_inlined_data()'s per-file "already deleted rows"
-- bookkeeping query with the table's partition expression in DuckDB dialect:
-- year(col)/month(col)/day(col)/hour(col) and, for bucket partitions,
-- (murmur3_32(col) & 2147483647) % N. pg_ducklake executes that query in
-- PostgreSQL over SPI (search_path forced to this schema), so these DuckDB
-- function names must exist here with DuckDB-identical semantics. Each
-- expression compares against the partition value the DuckDB writer baked
-- into the data file, so any semantic drift mis-tracks deleted rows.

-- Time transforms. DuckDB's year()/month()/day()/hour() return BIGINT; the
-- timestamptz variants depend on the session TimeZone (like DuckDB's, which
-- is synced from PostgreSQL when the embedded instance is created), hence
-- STABLE rather than IMMUTABLE.
CREATE FUNCTION ducklake.year(date) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT EXTRACT(YEAR FROM $1)::bigint $$;
CREATE FUNCTION ducklake.month(date) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT EXTRACT(MONTH FROM $1)::bigint $$;
CREATE FUNCTION ducklake.day(date) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT EXTRACT(DAY FROM $1)::bigint $$;
-- hour of a date is 0 (DuckDB implicitly casts date -> timestamp at midnight)
CREATE FUNCTION ducklake.hour(date) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT EXTRACT(HOUR FROM $1::timestamp)::bigint $$;

CREATE FUNCTION ducklake.year(timestamp) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT EXTRACT(YEAR FROM $1)::bigint $$;
CREATE FUNCTION ducklake.month(timestamp) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT EXTRACT(MONTH FROM $1)::bigint $$;
CREATE FUNCTION ducklake.day(timestamp) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT EXTRACT(DAY FROM $1)::bigint $$;
CREATE FUNCTION ducklake.hour(timestamp) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT EXTRACT(HOUR FROM $1)::bigint $$;

-- DuckDB accepts only hour() on TIME (year/month/day fail to bind there)
CREATE FUNCTION ducklake.hour(time) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT EXTRACT(HOUR FROM $1)::bigint $$;

CREATE FUNCTION ducklake.year(timestamptz) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql STABLE STRICT
    AS $$ SELECT EXTRACT(YEAR FROM $1)::bigint $$;
CREATE FUNCTION ducklake.month(timestamptz) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql STABLE STRICT
    AS $$ SELECT EXTRACT(MONTH FROM $1)::bigint $$;
CREATE FUNCTION ducklake.day(timestamptz) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql STABLE STRICT
    AS $$ SELECT EXTRACT(DAY FROM $1)::bigint $$;
CREATE FUNCTION ducklake.hour(timestamptz) RETURNS bigint
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql STABLE STRICT
    AS $$ SELECT EXTRACT(HOUR FROM $1)::bigint $$;

-- Iceberg-compatible murmur3 x86 32-bit hash (seed 0), bit-exact with
-- DuckLake's murmur3_32. One overload per exact input type: the generated
-- SQL always casts the column to its DuckLake type, and exact matches keep
-- PostgreSQL's function resolution from picking a wrong-encoding overload
-- (e.g. an int must hash as a sign-extended 8-byte long, not as a double).
CREATE FUNCTION ducklake.murmur3_32(boolean) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_bool';
CREATE FUNCTION ducklake.murmur3_32(smallint) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_int2';
CREATE FUNCTION ducklake.murmur3_32(integer) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_int4';
CREATE FUNCTION ducklake.murmur3_32(bigint) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_int8';
CREATE FUNCTION ducklake.murmur3_32(real) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_float4';
CREATE FUNCTION ducklake.murmur3_32(double precision) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_float8';
CREATE FUNCTION ducklake.murmur3_32(text) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_text';
-- raw bytes, for BLOB columns and VARCHAR columns' BYTEA storage
CREATE FUNCTION ducklake.murmur3_32(bytea) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_text';
CREATE FUNCTION ducklake.murmur3_32(date) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_date';
CREATE FUNCTION ducklake.murmur3_32(time) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_time';
CREATE FUNCTION ducklake.murmur3_32(timestamp) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_timestamp';
CREATE FUNCTION ducklake.murmur3_32(timestamptz) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C IMMUTABLE STRICT AS 'MODULE_PATHNAME', 'ducklake_murmur3_timestamp';
-- DuckDB hashes a DECIMAL of width <= 18 as its unscaled value in an 8-byte
-- long (DECIMAL(18,3) 12.5 hashes as 12500). Wider decimals hash their
-- fixed-scale string form; the flush filter renders those as ::text instead
-- of calling this overload. Without an exact numeric overload, PostgreSQL
-- would resolve murmur3_32(numeric) to the double precision one (the
-- preferred numeric type) and silently mis-hash every bucket.
CREATE FUNCTION ducklake.murmur3_32(numeric) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT ducklake.murmur3_32(($1 * power(10::numeric, scale($1)))::bigint) $$;
-- DuckDB hashes a UUID's lowercase hyphenated string form
CREATE FUNCTION ducklake.murmur3_32(uuid) RETURNS integer
    SET search_path = pg_catalog, pg_temp
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT ducklake.murmur3_32($1::text) $$;
