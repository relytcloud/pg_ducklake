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
