#pragma once

/* DuckLake FDW name constant - use this instead of hardcoding the string */
#define DUCKLAKE_FDW_NAME "ducklake_fdw"

namespace pgduckdb {
/* Tracks CREATE TABLE AS ... WITH [NO] DATA for DuckLake tables */
extern bool ducklake_ctas_skip_data;
} // namespace pgduckdb
