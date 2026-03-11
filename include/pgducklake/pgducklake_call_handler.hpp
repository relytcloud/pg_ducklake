#pragma once

/*
 * pgducklake_call_handler.hpp -- CALL statement handler for DuckDB routing.
 *
 * Handles CALL statements intercepted by pg_ducklake's utility hook.
 * Converts PG CallStmt arguments to DuckDB SQL literals and executes
 * the procedure via the DuckLake catalog.
 */

struct CallStmt;

namespace pgducklake {

void HandleDuckdbCall(CallStmt *call, const char *query_string);

} // namespace pgducklake
