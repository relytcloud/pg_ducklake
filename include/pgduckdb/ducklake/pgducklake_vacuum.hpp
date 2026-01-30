#pragma once

#include "pgduckdb/ducklake/pgducklake_defs.hpp"
#include "pgduckdb/pg/declarations.hpp"

extern "C" {
/* Perform VACUUM operation on a DuckLake table */
void DuckLakeVacuum(Relation onerel, VacuumParams *params, BufferAccessStrategy bstrategy);
}
