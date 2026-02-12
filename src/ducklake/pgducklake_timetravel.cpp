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
#include "utils/guc.h"
#include "utils/timestamp.h"
}

namespace pgduckdb {

/*
 * Per-table snapshot storage: maps relation OID -> timestamp string.
 * Set via ducklake.set_table_snapshot() UDF.
 * Cleared via ducklake.clear_table_snapshots() UDF.
 */
static std::unordered_map<Oid, std::string> table_snapshot_map;

void
ClearTableSnapshots() {
	table_snapshot_map.clear();
}

void
SetTableSnapshot(Oid relid, const char *timestamp) {
	table_snapshot_map[relid] = timestamp;
}

/*
 * Get the time-travel timestamp for a relation.
 *
 * Priority:
 * 1. Per-table snapshot (set via ducklake.set_table_snapshot())
 * 2. Global GUC (ducklake.as_of)
 *
 * The lookup is performed by relation OID.
 */
const char *
GetTimeTravelTimestamp(Oid relid) {
	/* Check per-table snapshot first */
	if (!table_snapshot_map.empty()) {
		auto it = table_snapshot_map.find(relid);
		if (it != table_snapshot_map.end()) {
			return it->second.c_str();
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
			if (relname) {
				const char *timestamp = GetTimeTravelTimestamp(rte->relid);
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

char *
MaybeApplyTimeTravelSnapshot(Oid relid, const char *db_and_schema, const char *relname, bool is_ducklake_table) {
	const char *timestamp = GetTimeTravelTimestamp(relid);
	if (!timestamp) {
		return NULL;
	}

	if (!is_ducklake_table) {
		elog(NOTICE, "ducklake.as_of_timestamp is set but '%s' is not a DuckLake table, ignoring", relname);
		return NULL;
	}

	std::string at_clause = GenerateAtClause(timestamp);
	elog(DEBUG1, "time-travel query: table=%s.%s, timestamp=%s", db_and_schema, quote_identifier(relname), timestamp);
	return psprintf("(SELECT * FROM %s.%s%s)", db_and_schema, quote_identifier(relname), at_clause.c_str());
}

} // namespace pgduckdb

extern "C" {

DECLARE_PG_FUNCTION(ducklake_set_table_snapshot) {
	Oid relid = PG_GETARG_OID(0);
	Timestamp timestamp_val = PG_GETARG_TIMESTAMP(1);

	/* Resolve table name and verify it's a DuckLake table */
	if (!OidIsValid(relid)) {
		elog(ERROR, "invalid table OID");
	}

	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp)) {
		elog(ERROR, "cache lookup failed for relation %u", relid);
	}
	Form_pg_class relation = (Form_pg_class)GETSTRUCT(tp);
	if (!pgduckdb::IsDucklakeTable(relation)) {
		ReleaseSysCache(tp);
		elog(ERROR, "table '%s' is not a DuckLake table", get_rel_name(relid));
	}
	ReleaseSysCache(tp);

	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("DateStyle", "ISO, YMD", PGC_USERSET, PGC_S_SESSION);
	char *timestamp_str = DatumGetCString(DirectFunctionCall1(timestamp_out, TimestampGetDatum(timestamp_val)));
	AtEOXact_GUC(false, save_nestlevel);

	pgduckdb::SetTableSnapshot(relid, timestamp_str);

	PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_clear_table_snapshots) {
	pgduckdb::ClearTableSnapshots();

	PG_RETURN_VOID();
}

} // extern "C"
