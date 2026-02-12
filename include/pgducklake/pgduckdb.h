/*
 * pg_duckdb C interface declarations
 *
 * This header declares the minimal C interface exported by pg_duckdb.
 * Only include this in PostgreSQL-facing translation units.
 */

#include "access/tableam.h"

/*
 * RegisterDuckdbTableAm - Register a custom table access method with pg_duckdb
 * Exported by pg_duckdb with __attribute__((visibility("default")))
 */
extern bool RegisterDuckdbTableAm(const char *name, const TableAmRoutine *am);
