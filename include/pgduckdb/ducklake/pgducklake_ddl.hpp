#pragma once

namespace pgduckdb {
/* Tracks CREATE TABLE AS ... WITH [NO] DATA for DuckLake tables */
extern bool ducklake_ctas_skip_data;
} // namespace pgduckdb
