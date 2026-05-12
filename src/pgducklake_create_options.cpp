/*
 * pgducklake_create_options.cpp -- Implementation of WITH (ducklake.*)
 * clause stripping + application for CREATE TABLE ... USING ducklake.
 *
 * @scope backend: per-process g_pending scratchpad
 *
 * Design rationale: PG core rejects WITH (ns.opt=...) for any namespace
 * other than "toast" (HEAP_RELOPT_NAMESPACES). The utility hook calls
 * StripDucklakeCreateOptions() before standard_ProcessUtility runs to
 * peel the ducklake.* DefElems off the options list and stash them in
 * g_pending; the create-table event trigger then consumes the stash.
 */

#include "pgducklake/pgducklake_create_options.hpp"

#include "pgducklake/pgducklake_duckdb_query.hpp"

#include <duckdb/parser/keyword_helper.hpp>

#include <cstring>
#include <string>

extern "C" {
#include "postgres.h"

#include "commands/defrem.h"
#include "nodes/parsenodes.h"
}

namespace pgducklake {

namespace {

PendingCreateOptions g_pending;

constexpr const char *kNamespace = "ducklake";
constexpr const char *kTablePath = "table_path";

[[noreturn]] void RejectUnknown(const char *name) {
  ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                  errmsg("unrecognized ducklake create option \"ducklake.%s\"", name),
                  errhint("Supported options: ducklake.%s", kTablePath)));
}

bool IsDucklakeNamespace(const DefElem *def) {
  return def->defnamespace != NULL && strcmp(def->defnamespace, kNamespace) == 0;
}

void ParseTablePath(DefElem *def, PendingCreateOptions &out) {
  if (def->arg == NULL)
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("ducklake.%s requires a string value", kTablePath)));
  char *val = defGetString(def);
  if (val == NULL || val[0] == '\0')
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("ducklake.%s cannot be empty", kTablePath),
                    errhint("Omit the option to use the catalog default path.")));
  out.has_table_path = true;
  out.table_path = val;
}

} // namespace

bool StripDucklakeCreateOptions(List **options_ref) {
  if (options_ref == NULL || *options_ref == NIL)
    return false;

  List *options = *options_ref;
  PendingCreateOptions parsed;

  ListCell *lc;
  foreach (lc, options) {
    DefElem *def = lfirst_node(DefElem, lc);
    if (!IsDucklakeNamespace(def))
      continue;

    if (strcmp(def->defname, kTablePath) == 0) {
      if (parsed.has_table_path)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("ducklake.%s specified more than once", kTablePath)));
      ParseTablePath(def, parsed);
    } else {
      RejectUnknown(def->defname);
    }

    options = foreach_delete_current(options, lc);
  }

  if (!parsed.has_table_path) {
    *options_ref = options;
    return false;
  }

  parsed.present = true;
  g_pending = std::move(parsed);
  *options_ref = options;
  return true;
}

PendingCreateOptions TakePendingCreateOptions() {
  PendingCreateOptions out = std::move(g_pending);
  g_pending = PendingCreateOptions {};
  return out;
}

void ClearPendingCreateOptions() {
  g_pending = PendingCreateOptions {};
}

void ApplyTablePathBeforeCreate(const PendingCreateOptions &opts) {
  if (!opts.has_table_path)
    return;
  std::string sql = "SET ducklake_default_table_path = " + duckdb::KeywordHelper::WriteQuoted(opts.table_path);
  const char *duck_err = nullptr;
  if (ExecuteDuckDBQuery(sql.c_str(), &duck_err) != 0)
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("failed to apply ducklake.%s: %s", kTablePath, duck_err ? duck_err : "unknown error")));
}

void RestoreTablePathAfterCreate(const PendingCreateOptions &opts) {
  if (!opts.has_table_path)
    return;
  // RESET back to DuckDB's default (empty) so the WITH-clause value does not
  // leak to subsequent CREATE TABLEs in this session. If the user has the PG
  // GUC set, re-apply it via Sync (which is a no-op when the GUC is empty).
  const char *duck_err = nullptr;
  if (ExecuteDuckDBQuery("RESET ducklake_default_table_path", &duck_err) != 0)
    elog(WARNING, "failed to reset ducklake_default_table_path after CREATE TABLE: %s",
         duck_err ? duck_err : "unknown error");
  SyncDefaultTablePathToDuckDB();
}

} // namespace pgducklake
