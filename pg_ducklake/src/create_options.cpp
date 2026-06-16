/*
 * create_options.cpp -- WITH (ducklake.*) clause stripping + application for
 * CREATE TABLE ... USING ducklake.
 */

#include "pgducklake/create_options.hpp"
#include "pgducklake/duckdb_manager.hpp"

#include <cstring>
#include <string>

#include <duckdb/parser/keyword_helper.hpp>

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

[[noreturn]] void
RejectUnknown(const char *name) {
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
	                errmsg("unrecognized ducklake create option \"ducklake.%s\"", name),
	                errhint("Supported options: ducklake.%s", kTablePath)));
}

bool
IsDucklakeNamespace(const DefElem *def) {
	return def->defnamespace != NULL && strcmp(def->defnamespace, kNamespace) == 0;
}

void
ParseTablePath(DefElem *def, PendingCreateOptions &out) {
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

bool
StripDucklakeCreateOptions(List **options_ref) {
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
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				                errmsg("ducklake.%s specified more than once", kTablePath)));
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

PendingCreateOptions
TakePendingCreateOptions() {
	PendingCreateOptions out = std::move(g_pending);
	g_pending = PendingCreateOptions {};
	return out;
}

void
ClearPendingCreateOptions() {
	g_pending = PendingCreateOptions {};
}

TablePathOverrideGuard::TablePathOverrideGuard(const PendingCreateOptions &opts) : active_(opts.has_table_path) {
	if (active_)
		DuckDBManager::Get().SetTablePathOverride(opts.table_path);
}

TablePathOverrideGuard::~TablePathOverrideGuard() {
	if (active_)
		DuckDBManager::Get().ClearTablePathOverride();
}

} // namespace pgducklake
