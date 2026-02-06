#include "pgduckdb/ducklake/pgducklake_timetravel.hpp"
#include "pgduckdb/pgduckdb_guc.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

#include <string>
#include <unordered_map>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"
}

namespace pgduckdb {

/*
 * Per-table snapshot storage: maps table name -> timestamp.
 * Set via ducklake.set_table_snapshot() UDF.
 * Cleared via ducklake.clear_table_snapshots() UDF.
 */
static std::unordered_map<std::string, std::string> table_snapshot_map;

void
ClearTableSnapshots() {
	table_snapshot_map.clear();
}

void
SetTableSnapshot(const char *table_name, const char *timestamp) {
	table_snapshot_map[table_name] = timestamp;
}

/*
 * Get the time-travel timestamp for a relation.
 *
 * Priority:
 * 1. Per-table snapshot (set via ducklake.set_table_snapshot())
 * 2. Global GUC (ducklake.as_of)
 *
 * The table_name is matched against both unqualified and schema-qualified names.
 */
const char *
GetTimeTravelTimestamp(const char *relname, const char *schemaname) {
	/* Check per-table snapshot first */
	if (!table_snapshot_map.empty()) {
		/* Try unqualified name */
		auto it = table_snapshot_map.find(relname);
		if (it != table_snapshot_map.end()) {
			return it->second.c_str();
		}

		/* Try schema-qualified name */
		if (schemaname) {
			std::string qualified = std::string(schemaname) + "." + relname;
			it = table_snapshot_map.find(qualified);
			if (it != table_snapshot_map.end()) {
				return it->second.c_str();
			}
		}
	}

	/* Check global GUC */
	if (ducklake_as_of_timestamp && ducklake_as_of_timestamp[0] != '\0') {
		return ducklake_as_of_timestamp;
	}

	return NULL;
}

/*
 * Generate a DuckDB AT clause for a timestamp.
 *
 * Returns a string like " AT (TIMESTAMP => '2025-01-01')" (with leading space).
 */
std::string
GenerateAtClause(const char *timestamp) {
	if (!timestamp) {
		return "";
	}

	std::string result = " AT (TIMESTAMP => '";
	result += timestamp;
	result += "')";
	return result;
}

/*
 * Validate timestamp format.
 *
 * Accepts formats like:
 *   YYYY-MM-DD
 *   YYYY-MM-DD HH:MM:SS
 *   YYYY-MM-DD HH:MM:SS.UUUUUU
 *   YYYY-MM-DDTHH:MM:SS
 *
 * Minimum validation: must have at least YYYY-MM-DD (10 chars).
 */
bool
ValidateTimestampFormat(const char *timestamp) {
	if (!timestamp || strlen(timestamp) < 10) {
		return false;
	}

	if (timestamp[4] != '-' || timestamp[7] != '-') {
		return false;
	}

	for (int i = 0; i < 10; i++) {
		if (i == 4 || i == 7) {
			continue;
		}
		if (!isdigit((unsigned char)timestamp[i])) {
			return false;
		}
	}

	return true;
}

void
ValidateNoTimeTravelForDML(void *query_ptr) {
	Query *query = (Query *)query_ptr;

	/* Only check DML operations */
	if (query->commandType != CMD_INSERT && query->commandType != CMD_UPDATE && query->commandType != CMD_DELETE) {
		return;
	}

	/* Check if global time travel GUC is set */
	if (ducklake_as_of_timestamp && ducklake_as_of_timestamp[0] != '\0') {
		elog(ERROR, "DML operations are not allowed while ducklake.as_of_timestamp is set. "
		            "Reset the timestamp with: RESET ducklake.as_of_timestamp");
	}

	/* Check if any per-table snapshots are set (they would affect the target table) */
	if (query->resultRelation > 0) {
		RangeTblEntry *rte = rt_fetch(query->resultRelation, query->rtable);
		if (rte && rte->relid) {
			const char *relname = get_rel_name(rte->relid);
			const char *schemaname = get_namespace_name(get_rel_namespace(rte->relid));
			if (relname && schemaname) {
				const char *timestamp = GetTimeTravelTimestamp(relname, schemaname);
				if (timestamp) {
					elog(ERROR,
					     "DML operations are not allowed on table '%s' while a time travel snapshot is set. "
					     "Clear the snapshot with: SELECT ducklake.clear_table_snapshots()",
					     relname);
				}
			}
		}
	}
}

} // namespace pgduckdb

extern "C" {

DECLARE_PG_FUNCTION(ducklake_set_table_snapshot) {
	const char *table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	const char *timestamp_val = text_to_cstring(PG_GETARG_TEXT_PP(1));

	if (!pgduckdb::ValidateTimestampFormat(timestamp_val)) {
		elog(ERROR, "invalid timestamp format: '%s'", timestamp_val);
	}

	/* Resolve table name and verify it's a DuckLake table */
	List *names = stringToQualifiedNameList(table_name, NULL);
	RangeVar *rv = makeRangeVarFromNameList(names);
	Oid relid = RangeVarGetRelid(rv, AccessShareLock, true);
	if (!OidIsValid(relid)) {
		elog(ERROR, "table '%s' does not exist", table_name);
	}

	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp)) {
		elog(ERROR, "cache lookup failed for relation %u", relid);
	}
	Form_pg_class relation = (Form_pg_class)GETSTRUCT(tp);
	if (!pgduckdb::IsDucklakeTable(relation)) {
		ReleaseSysCache(tp);
		elog(ERROR, "table '%s' is not a DuckLake table", table_name);
	}
	ReleaseSysCache(tp);

	pgduckdb::SetTableSnapshot(table_name, timestamp_val);

	PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_clear_table_snapshots) {
	pgduckdb::ClearTableSnapshots();

	PG_RETURN_VOID();
}

} // extern "C"
