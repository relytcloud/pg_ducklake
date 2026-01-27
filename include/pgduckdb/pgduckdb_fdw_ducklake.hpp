#pragma once

#include "pgduckdb/pg/declarations.hpp"

namespace pgduckdb {

void RegisterDucklakeForeignTable(Oid foreign_table_oid);
bool IsDucklakeForeignTable(Oid relid);
char *GetDucklakeForeignTableName(Oid foreign_table_oid);
void InferAndPopulateForeignTableColumns(CreateForeignTableStmt *stmt);
void RegisterForeignTablesInQuery(Query *query);

} // namespace pgduckdb
