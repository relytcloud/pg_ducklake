#pragma once

/*
 * pgducklake_create_options.hpp -- WITH (ducklake.*) clause support for
 * CREATE TABLE ... USING ducklake.
 *
 * The ducklake.* namespace is not accepted by stock PG (HEAP_RELOPT_NAMESPACES
 * only allows "toast"). To support it, the utility hook strips the
 * ducklake.* entries from the parsetree's options list before the standard
 * ProcessUtility runs, stashes them in a per-process scratchpad, and the
 * CREATE TABLE event trigger drains the scratchpad to apply them against
 * the DuckLake session/catalog.
 *
 * v1 options:
 *   ducklake.table_path -- per-table data path override (set via the
 *                          ducklake_default_table_path DuckDB session
 *                          option, which DuckLake's CreateTable reads).
 *
 * Per-table options that go through ducklake_set_option (e.g.
 * data_inlining_row_limit) are intentionally NOT supported here: the
 * upstream set_option refuses transaction-local table_ids, and inside the
 * CREATE TABLE event trigger the new table is still transaction-local.
 * Those options can be set via CALL ducklake.set_option(..., scope) in a
 * separate statement after CREATE TABLE.
 */

#include <string>

/* Forward-declare PG types so this header does NOT pull in postgres.h.
 * Pulling postgres.h here would force every TU that uses this header to
 * include PG headers before DuckDB headers, which triggers the well-known
 * FATAL macro collision (PG's elog.h defines FATAL, DuckDB has
 * ExceptionType::FATAL). */
struct List;

namespace pgducklake {

struct PendingCreateOptions {
  bool present = false;
  bool has_table_path = false;
  std::string table_path;
};

/*
 * Walk a CREATE TABLE options list (DefElem nodes from CreateStmt->options
 * or CreateTableAsStmt->into->options), validate any defnamespace=="ducklake"
 * entries against the v1 allow-list, stash them in the per-process scratchpad,
 * and rewrite *options_ref to the remainder. Returns true if any ducklake.*
 * option was stripped (caller then forwards to standard_ProcessUtility with
 * the stripped list). Raises ereport(ERROR) on unknown ducklake.* names or
 * invalid values.
 */
bool StripDucklakeCreateOptions(List **options_ref);

/* Snapshot + clear the scratchpad. Returned struct has present=false if
 * nothing was stashed. Safe to call multiple times; second call returns the
 * cleared value. */
PendingCreateOptions TakePendingCreateOptions();

/* Discard any pending scratchpad entry without applying it. Called from the
 * hook's error path so a failed CREATE doesn't poison the next one. */
void ClearPendingCreateOptions();

/* Set ducklake_default_table_path in the DuckDB session to opts.table_path
 * before issuing the generated CREATE TABLE DDL. No-op when
 * !opts.has_table_path. */
void ApplyTablePathBeforeCreate(const PendingCreateOptions &opts);

/* Restore ducklake_default_table_path to the value of the
 * ducklake.default_table_path PG GUC (empty when the GUC is unset). Always
 * called after the generated CREATE TABLE DDL when opts.has_table_path so
 * subsequent CREATE TABLEs in the same session see a clean slate. */
void RestoreTablePathAfterCreate(const PendingCreateOptions &opts);

} // namespace pgducklake
