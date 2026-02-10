#pragma once

#include <optional>
#include <vector>

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {

/*
 * Information about an INSERT...SELECT UNNEST pattern that can bypass DuckDB execution.
 * When this pattern is detected, we can insert directly into the PostgreSQL inlined
 * data table without going through DuckDB, avoiding multiple value conversions.
 */
struct InlineBypassInfo {
	Oid target_table_oid;              /* OID of the target DuckLake table */
	std::vector<int> param_indices;    /* Which $N parameter for each column (0-based) */
	std::vector<Oid> array_elem_types; /* Element type of each array parameter */
	std::vector<Oid> array_types;      /* Array type OID of each parameter */
	uint64_t data_inlining_row_limit;  /* Row limit for inlining (0 = disabled) */
	const char *schema_name;           /* Schema name of target table */
	const char *table_name;            /* Table name */
};

/*
 * Detect if a query matches the inline bypass pattern:
 *   INSERT INTO ducklake_table SELECT UNNEST($1::type[]), UNNEST($2::type[]), ...
 *
 * Returns the bypass info if pattern matches and inlining is enabled,
 * otherwise returns std::nullopt and we fall back to normal DuckDB execution.
 */
std::optional<InlineBypassInfo> DetectInlineBypassPattern(Query *query);

/*
 * Create a PlannedStmt for the inline bypass custom scan.
 * This creates a minimal plan that will execute via our custom scan callbacks.
 */
PlannedStmt *CreateInlineBypassPlan(const InlineBypassInfo &info, Query *query);

/*
 * Initialize the inline bypass custom scan methods.
 * Called during extension initialization.
 */
void InitInlineBypassNode(void);

} // namespace pgduckdb
