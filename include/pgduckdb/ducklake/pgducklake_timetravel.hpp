#pragma once

#include <string>

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {

/*
 * Time-travel query support for DuckLake tables.
 *
 * Two mechanisms for setting snapshot timestamps:
 *
 * 1. Global GUC: SET ducklake.as_of = '2025-01-01';
 *    Applies to ALL DuckLake tables in the session.
 *
 * 2. Per-table snapshots: SELECT ducklake.set_table_snapshot('orders'::regclass, '2025-01-01'::timestamp);
 *    Applies only to the specified table, takes priority over the GUC.
 *
 * After setting, normal SQL queries automatically get AT clauses during deparsing:
 *   SELECT * FROM orders;
 *   -> DuckDB: SELECT * FROM pgducklake.public.orders AT (TIMESTAMP => '2025-01-01')
 */

/*
 * Get the time-travel timestamp for a relation OID.
 * Checks per-table snapshot first, then global GUC.
 * Returns NULL if no time-travel is active.
 */
const char *GetTimeTravelTimestamp(Oid relid);

/*
 * Generate a DuckDB AT clause for a timestamp.
 * Returns a string like " AT (TIMESTAMP => '2025-01-01')" (with leading space).
 */
std::string GenerateAtClause(const char *timestamp);

/*
 * Set a per-table snapshot timestamp.
 */
void SetTableSnapshot(Oid relid, const char *timestamp);

/*
 * Clear all per-table snapshot timestamps.
 */
void ClearTableSnapshots();

/*
 * Validate that DML operations are not attempted with time travel active.
 * Throws an error if time travel is set for the query.
 *
 * This must be called early in query planning for INSERT/UPDATE/DELETE.
 */
void ValidateNoTimeTravelForDML(void *query);

/*
 * If time travel is active for the relation, return a rewritten query
 * string in DuckDB syntax. Otherwise returns NULL.
 */
char *MaybeApplyTimeTravelSnapshot(Oid relid, const char *db_and_schema, const char *relname, bool is_ducklake_table);

} // namespace pgduckdb
