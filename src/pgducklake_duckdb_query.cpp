/*
 * pgducklake_duckdb_query.cpp -- Execute DuckDB queries via pg_duckdb.
 *
 * @scope backend: cached raw_query OID, last_error thread_local
 *
 * Wraps pg_duckdb's duckdb.raw_query() UDF so pg_ducklake code can
 * execute DuckDB SQL with error capture.  Used by DDL triggers,
 * VACUUM, freeze, FDW attach, and the utility hook.
 */

#include <duckdb/common/string_util.hpp>
#include <duckdb/parser/keyword_helper.hpp>

#include "pgducklake/pgducklake_duckdb_query.hpp"
#include "pgducklake/pgducklake_guc.hpp"

#include <string>

extern "C" {
#include "postgres.h"

#include "catalog/namespace.h"
#include "fmgr.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/guc.h"
}

namespace pgducklake {

static Oid GetRawQueryFuncOid() {
  static Oid cached = InvalidOid;
  if (!OidIsValid(cached)) {
    List *funcname = list_make2(makeString(pstrdup("duckdb")), makeString(pstrdup("raw_query")));
    Oid argtypes[] = {TEXTOID};
    cached = LookupFuncName(funcname, 1, argtypes, false);
    list_free(funcname);
  }
  return cached;
}

static void DuckdbRawQuery(const char *query) {
  OidFunctionCall1(GetRawQueryFuncOid(), CStringGetTextDatum(query));
}

/*
 * Execute a DuckDB query via pg_duckdb's duckdb.raw_query() UDF.
 * Ensures the DuckLake catalog is attached first.
 *
 * Returns 0 on success, 1 on error.
 * On error, sets *errmsg_out to the error message (if non-null).
 */
int ExecuteDuckDBQuery(const char *query, const char **errmsg_out) {
  static thread_local std::string last_error;

  // Volatile to survive PG_CATCH longjmp
  volatile int result = 0;
  MemoryContext saved_context = CurrentMemoryContext;

  // Suppress NOTICE messages from duckdb.raw_query() which unconditionally
  // emits "result: ..." via elog(NOTICE).
  //
  // FIXME: should we modify pg_duckdb?
  auto save_nestlevel = NewGUCNestLevel();
  SetConfigOption("client_min_messages", "warning", PGC_USERSET, PGC_S_SESSION);

  PG_TRY();
  {
    DuckdbRawQuery(query);
  }
  PG_CATCH();
  {
    MemoryContextSwitchTo(saved_context);
    ErrorData *edata = CopyErrorData();
    FlushErrorState();

    last_error = edata->message ? edata->message : "unknown error";
    FreeErrorData(edata);

    if (errmsg_out)
      *errmsg_out = last_error.c_str();
    result = 1;
  }
  PG_END_TRY();

  AtEOXact_GUC(false, save_nestlevel);

  return result;
}

void SyncDefaultTablePathToDuckDB() {
  if (default_table_path && default_table_path[0] != '\0') {
    std::string set_query =
        "SET ducklake_default_table_path = " + duckdb::KeywordHelper::WriteQuoted(std::string(default_table_path));
    const char *errmsg = nullptr;
    if (ExecuteDuckDBQuery(set_query.c_str(), &errmsg) != 0) {
      elog(WARNING, "failed to sync ducklake.default_table_path to DuckDB: %s", errmsg ? errmsg : "unknown");
    }
  }
}

} // namespace pgducklake
