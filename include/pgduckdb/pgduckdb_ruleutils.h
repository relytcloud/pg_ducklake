#include "postgres.h"
#include "nodes/parsenodes.h"

extern char *pgduckdb_relation_name(Oid relid);
extern char *pgduckdb_get_querydef(Query *query);
extern char *pgduckdb_get_tabledef(Oid relation_id);
extern char *pgduckdb_get_alter_tabledef(Oid relation_oid, AlterTableStmt *alter_stmt);
extern char *pgduckdb_get_rename_relationdef(Oid relation_oid, RenameStmt *rename_stmt);
