#pragma once

#include "duckdb.hpp"

#include "pgddb/pg/declarations.hpp"
#include "pgddb/pgddb_duckdb.hpp"

#include "pgddb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgddb {

extern bool explain_analyze;
extern duckdb::ExplainFormat explain_format;
extern bool explain_ctas;

PlannedStmt *PlanNode(Query *parse, int cursor_options, bool throw_error);
duckdb::unique_ptr<duckdb::PreparedStatement> Prepare(const Query *query, const char *explain_prefix = nullptr);
bool ContainsPostgresTable(Node *node, void *context);

// Deparse `query` back to the DuckDB-dialect SQL string the CustomScan executes.
std::string DeparseQuery(const Query *query);

} // namespace pgddb
