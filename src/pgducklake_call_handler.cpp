/*
 * pgducklake_call_handler.cpp -- CALL statement handler for DuckDB routing.
 *
 * @scope extension: CALL handler for ducklake_only_procedure procs
 *
 * Handles CALL statements that pg_ducklake's utility hook intercepts.
 * Extracts arguments from the PG CallStmt, converts them to DuckDB SQL
 * literals, resolves regclass arguments to schema+table named parameters,
 * and executes the CALL via the DuckLake catalog.
 */

#include <duckdb/parser/keyword_helper.hpp>

#include "pgducklake/pgducklake_call_handler.hpp"
#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgduckdb/pgduckdb_contracts.hpp"

#include <string>
#include <vector>

extern "C" {
#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
}

namespace pgducklake {

static std::string DatumToSqlLiteral(Datum value, Oid type_oid, bool isnull) {
  if (isnull)
    return "NULL";

  Oid typoutput;
  bool typisvarlena;
  getTypeOutputInfo(type_oid, &typoutput, &typisvarlena);
  char *val_str = OidOutputFunctionCall(typoutput, value);

  std::string result;
  switch (type_oid) {
  case BOOLOID:
  case INT2OID:
  case INT4OID:
  case INT8OID:
  case FLOAT4OID:
  case FLOAT8OID:
  case NUMERICOID:
    result = val_str;
    break;
  default:
    result = duckdb::KeywordHelper::WriteQuoted(val_str);
    break;
  }

  pfree(val_str);
  return result;
}

void HandleDuckdbCall(CallStmt *call, const char *query_string) {
  FuncExpr *funcexpr = call->funcexpr;

  char *proc_name = get_func_name(funcexpr->funcid);
  if (!proc_name)
    elog(ERROR, "could not find procedure with OID %u", funcexpr->funcid);

  std::vector<std::string> positional_args;
  std::string named_params;
  ListCell *lc;

  foreach (lc, funcexpr->args) {
    Node *arg = (Node *)lfirst(lc);

    if (!IsA(arg, Const))
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("non-constant arguments are not supported in "
                             "DuckDB-routed procedures")));

    Const *c = (Const *)arg;

    if (c->consttype == REGCLASSOID && !c->constisnull) {
      Oid relid = DatumGetObjectId(c->constvalue);
      char *table_name = get_rel_name(relid);
      if (!table_name)
        elog(ERROR, "could not find relation with OID %u", relid);
      char *schema_name = get_namespace_name(get_rel_namespace(relid));
      if (!schema_name)
        elog(ERROR, "could not find namespace for relation with OID %u", relid);

      named_params +=
          ", table_name => " + duckdb::KeywordHelper::WriteQuoted(table_name);
      named_params +=
          ", schema => " + duckdb::KeywordHelper::WriteQuoted(schema_name);
    } else {
      positional_args.push_back(
          DatumToSqlLiteral(c->constvalue, c->consttype, c->constisnull));
    }
  }

  std::string args_joined;
  for (size_t i = 0; i < positional_args.size(); i++) {
    if (i > 0)
      args_joined += ", ";
    args_joined += positional_args[i];
  }

  std::string query = "CALL " PGDUCKLAKE_DUCKDB_CATALOG "." +
                       duckdb::KeywordHelper::WriteOptionallyQuoted(proc_name) +
                       "(" + args_joined + named_params + ")";

  elog(DEBUG2, "[PGDuckLake] Executing CALL: %s", query.c_str());

  // The utility hook intercepts CALL before standard_ProcessUtility, so
  // pg_duckdb's planner hook never fires and the metadata cache may be stale.
  // Ensure it is valid before calling raw_query, which asserts cache.valid.
  PushActiveSnapshot(GetTransactionSnapshot());
  if (!pgduckdb::DuckdbEnsureCacheValid()) {
    PopActiveSnapshot();
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("pg_duckdb is not available")));
  }

  const char *error_msg = nullptr;
  int result = ExecuteDuckDBQuery(query.c_str(), &error_msg);
  PopActiveSnapshot();
  if (result != 0)
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("%s", error_msg ? error_msg : "unknown error")));
}

} // namespace pgducklake
