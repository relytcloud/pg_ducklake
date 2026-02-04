#include "pgduckdb/ducklake/pgducklake_vacuum.hpp"

#include "duckdb/common/helper.hpp"

#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_guc.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"

#include "access/relation.h"
#include "commands/vacuum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

namespace pgduckdb {

static void
DuckLakeVacuum_Cpp(Relation onerel, VacuumParams *params, BufferAccessStrategy /*bstrategy*/) {
	/* Extract relation metadata before entering DuckDB context */
	const char *relname = NameStr(onerel->rd_rel->relname);
	const char *schema_name = get_namespace_name_or_temp(onerel->rd_rel->relnamespace);

	/* Check for NULL schema name (cache lookup failure) */
	if (!schema_name) {
		elog(ERROR, "cache lookup failed for namespace %u", onerel->rd_rel->relnamespace);
	}

	std::string relname_str(relname);
	std::string schema_name_str(schema_name);
	std::string db_name_str(PGDUCKLAKE_DB_NAME);
	bool verbose = params->options & VACOPT_VERBOSE;

	auto connection = DuckDBManager::GetConnection();

	if (verbose) {
		ereport(INFO, (errmsg("vacuuming ducklake table \"%s.%s\"", schema_name, relname)));
	}

	/*
	 * 1. Rewrite data files to remove deleted rows.
	 * The delete threshold is configurable via ducklake.vacuum_delete_threshold GUC.
	 * Default is 0.1 (10%), which is more aggressive than DuckLake's default (0.95).
	 */
	std::string rewrite_query = "SELECT * FROM ducklake_rewrite_data_files(" +
	                            duckdb::KeywordHelper::WriteQuoted(db_name_str, '\'') + ", " +
	                            duckdb::KeywordHelper::WriteQuoted(relname_str, '\'') + ", " +
	                            "schema=" + duckdb::KeywordHelper::WriteQuoted(schema_name_str, '\'') +
	                            ", delete_threshold => " + std::to_string(ducklake_vacuum_delete_threshold) + ")";

	DuckDBQueryOrThrow(*connection, rewrite_query);
	if (verbose) {
		ereport(INFO, (errmsg("rewritten data files with deleted rows")));
	}

	/* 2. Merge adjacent small files to optimize scan performance */
	std::string merge_query = "SELECT * FROM ducklake_merge_adjacent_files(" +
	                          duckdb::KeywordHelper::WriteQuoted(db_name_str, '\'') + ", " +
	                          duckdb::KeywordHelper::WriteQuoted(relname_str, '\'') + ", " +
	                          "schema=" + duckdb::KeywordHelper::WriteQuoted(schema_name_str, '\'') + ")";

	DuckDBQueryOrThrow(*connection, merge_query);
	if (verbose) {
		ereport(INFO, (errmsg("merged adjacent small files")));
	}

	elog(DEBUG1, "vacuumed ducklake table \"%s.%s\"", schema_name_str.c_str(), relname_str.c_str());
}

} // namespace pgduckdb

extern "C" {

void
DuckLakeVacuum(Relation onerel, VacuumParams *params, BufferAccessStrategy bstrategy) {
	InvokeCPPFunc(pgduckdb::DuckLakeVacuum_Cpp, onerel, params, bstrategy);
}

} // extern "C"
