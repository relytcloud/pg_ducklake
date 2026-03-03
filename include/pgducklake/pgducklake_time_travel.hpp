#pragma once

/*
 * pgducklake_time_travel.hpp — Time-travel query support for DuckLake tables
 *
 * Declares the DuckDB table function `time_travel(table_name, version/timestamp)`
 * that enables querying DuckLake tables at historical snapshots.
 */

#include "duckdb/function/function_set.hpp"

namespace duckdb {
class DatabaseInstance;
}

namespace pgducklake {

duckdb::TableFunctionSet GetTimeTravelFunctions();
void RegisterTimeTravelFunction(duckdb::DatabaseInstance &db);

} // namespace pgducklake
