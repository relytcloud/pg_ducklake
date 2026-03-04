/*
 * pgducklake_fdw.hpp — Foreign Data Wrapper for DuckLake tables
 *
 * Provides read-only access to DuckLake tables (both PostgreSQL-backed and
 * frozen HTTP-hosted) via PostgreSQL's FDW infrastructure.  Queries are
 * routed through DuckDB by registering the foreign tables as "external
 * DuckDB tables" with pg_duckdb's hook system.
 */

#pragma once

struct Query; /* forward-declare PostgreSQL Query node */

namespace pgducklake {
void InitFDW();
void RegisterForeignTablesInQuery(Query *query);
} // namespace pgducklake
