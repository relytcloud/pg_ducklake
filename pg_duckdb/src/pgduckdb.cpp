#include "pgduckdb/pgduckdb.h"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_guc.hpp"

#include "pgddb/pgddb_duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
}

#include "pgduckdb/pgduckdb_background_worker.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgddb/pgddb_node.hpp"
#include "pgddb/pgddb_subscript.h"
#include "pgduckdb/pgduckdb_ruleutils.hpp"
#include "pgduckdb/pgduckdb_table_am.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pgduckdb_xact.hpp"

namespace pgduckdb {
void InitDuckdbWorker();
}

extern "C" {

#ifdef PG_MODULE_MAGIC_EXT
#ifndef PG_DUCKDB_VERSION
// Should always be defined via build system, but keep a fallback here for
// static analysis tools etc.
#define PG_DUCKDB_VERSION "unknown"
#endif
PG_MODULE_MAGIC_EXT(.name = "pg_duckdb", .version = PG_DUCKDB_VERSION);
#else
PG_MODULE_MAGIC;
#endif

void
_PG_init(void) {
	if (!process_shared_preload_libraries_in_progress) {
		ereport(ERROR, (errmsg("pg_duckdb needs to be loaded via shared_preload_libraries"),
		                errhint("Add pg_duckdb to shared_preload_libraries.")));
	}

	pgduckdb::InitDuckDBManager();
	pgduckdb::InitGUC();
	pgduckdb::InitGUCHooks();
	pgduckdb::InitRuleutilsHooks();
	pgduckdb::InitTypeHooks();
	pgduckdb::InitTableAmHook();
	pgddb::pg::subscript_refrestype_hook = [](Oid) {
		return pgduckdb::IsExtensionRegistered() ? pgduckdb::DuckdbUnresolvedTypeOid() : InvalidOid;
	};
	DuckdbInitHooks();
	pgddb::InitNode("DuckDBScan");
	pgduckdb::InitBackgroundWorkersShmem();
	pgduckdb::InitDuckdbWorker();
	pgduckdb::RegisterDuckdbXactCallback();
}
} // extern "C"
