/*
 * pgducklake_functions_mapping.cpp -- regclass-to-text function rewrite stub.
 *
 * @scope extension: func ducklake_function_mapping (safety-net stub)
 *
 * The planner hook (pgducklake_hooks.cpp) rewrites regclass overloads of
 * table-scoped DuckLake functions into their text-arg counterparts before
 * pg_duckdb routes them to DuckDB. This file provides a safety-net C
 * function that errors if the rewrite did not happen.
 */

#include <duckdb/common/error_data.hpp>

#include "pgducklake/utility/cpp_wrapper.hpp"

extern "C" {
#include "fmgr.h"
}

extern "C" {

DECLARE_PG_FUNCTION(ducklake_function_mapping) {
  ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                  errmsg("regclass function was not rewritten by planner hook"),
                  errhint("Use the (schema_name text, table_name text) form "
                          "for dynamic table references.")));
  PG_RETURN_NULL();
}

} // extern "C"
