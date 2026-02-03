#pragma once

#include "pgduckdb/pg/declarations.hpp"

/* DuckLake FDW name constant */
#define DUCKLAKE_FDW_NAME "ducklake_fdw"

namespace pgduckdb {

void RegisterDucklakeForeignTable(Oid foreign_table_oid);
bool IsDucklakeForeignTable(Oid relid);
char *GetDucklakeForeignTableName(Oid foreign_table_oid);
void InferAndPopulateForeignTableColumns(CreateForeignTableStmt *stmt);
void RegisterForeignTablesInQuery(Query *query);

} // namespace pgduckdb
