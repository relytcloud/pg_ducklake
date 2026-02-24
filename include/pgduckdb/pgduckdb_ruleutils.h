#include "postgres.h"

extern char *pgduckdb_relation_name(Oid relid);
extern char *pgduckdb_get_querydef(Query *query);
extern char *pgduckdb_get_tabledef(Oid relation_id);
