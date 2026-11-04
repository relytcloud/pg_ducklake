/*
 * Shared per-database DuckDB worker: read-only SELECTs that reference no
 * PostgreSQL relation run there; the DuckLake metadata SQL they need comes back
 * as RPCs that the dispatching backend answers over SPI.
 */

#include "pgducklake/duckdb_manager.hpp"
#include "pgducklake/duckdb_worker.hpp"
#include "pgducklake/functions.hpp"
#include "pgducklake/guc.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"

#include <exception>
#include <string>
#include <utility>

#include "pgddb/pg/guc.hpp"
#include "pgddb/pgddb_planner.hpp"
#include "pgddb/worker/worker_protocol.hpp"

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <duckdb/transaction/transaction_context.hpp>

extern "C" {
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/elog.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"
}

#include "pgddb/utility/cpp_wrapper.hpp"

namespace pgducklake {

DuckLakeWorker::DuckLakeWorker()
    : pgddb::DuckdbWorker(Settings {"pg_ducklake duckdb worker", "pg_ducklake", "pgducklake_duckdb_worker_main",
                                    &max_worker_sessions, &worker_page_size, &worker_arena_pages,
                                    &worker_reserved_pages}) {
}

DuckLakeWorker &
DuckLakeWorker::Get() {
	static DuckLakeWorker instance;
	return instance;
}

duckdb::DuckDB &
DuckLakeWorker::Prime() {
	// GetConnection runs RefreshConnectionState: secrets and the table path.
	auto &manager = DuckDBManager::Get();
	manager.GetConnection();
	return manager.GetDatabase();
}

static void
RefreshWorkerConnection() {
	DuckDBManager::Get().GetConnection();
}

/* Re-applies SERVER / USER MAPPING changes (the syscache callbacks mark the
 * secrets stale) to the shared engine before a session starts. Failures only warn. */
void
DuckLakeWorker::BeforeSession() {
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	AcceptInvalidationMessages();

	volatile bool ok = false;
	PG_TRY();
	{
		InvokeCPPFunc(RefreshWorkerConnection);
		ok = true;
	}
	PG_CATCH();
	{
		MemoryContext saved = MemoryContextSwitchTo(TopMemoryContext);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();
		MemoryContextSwitchTo(saved);
		elog(WARNING, "pg_ducklake duckdb worker: failed to refresh connection state: %s",
		     edata->message ? edata->message : "unknown");
		FreeErrorData(edata);
	}
	PG_END_TRY();

	if (ok) {
		PopActiveSnapshot();
		CommitTransactionCommand();
	} else {
		AbortCurrentTransaction();
	}
}

/* Backend, inside the dispatching statement: run the worker's metadata SQL over
 * SPI exactly like the in-process path and ship back the materialized result. */
std::string
DuckLakeWorker::ServeRequest(const std::string &request) {
	if (request.empty()) {
		throw duckdb::IOException("empty duckdb worker request");
	}
	duckdb::string sql = request.substr(1);
	duckdb::unique_ptr<duckdb::QueryResult> result;
	switch (request[0]) {
	case WORKER_REQUEST_QUERY:
		result = ExecuteMetadataQueryLocally(sql);
		break;
	case WORKER_REQUEST_COMMIT:
		try {
			result = ExecuteMetadataCommitLocally(sql);
		} catch (const std::exception &ex) {
			result = duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::ErrorData(ex));
		}
		break;
	default:
		throw duckdb::IOException("unknown duckdb worker request kind %d", (int)request[0]);
	}
	return pgddb::EncodeQueryResult(*result);
}

/* pgddb_dispatch_hook: only read-only SELECTs that need nothing from this
 * backend except DuckLake metadata go to the worker; nullptr = in-process. */
static duckdb::unique_ptr<pgddb::RemoteResult>
DispatchToWorker(const Query *query, ParamListInfo params) {
	if (!use_shared_worker || pgddb::InDuckdbWorker()) {
		return nullptr;
	}
	if (query->commandType != CMD_SELECT || query->hasModifyingCTE) {
		return nullptr;
	}
	// EXPLAIN output comes from this backend's engine.
	if (ActivePortal && ActivePortal->commandTag == CMDTAG_EXPLAIN) {
		return nullptr;
	}
	// Each dispatched statement sees the latest DuckLake snapshot; a transaction block pins one in-process.
	if (IsTransactionBlock() || IsInParallelMode() || RecoveryInProgress()) {
		return nullptr;
	}
	if (DuckDBManager::IsInitialized()) {
		auto *connection = DuckDBManager::Get().GetConnectionUnsafe();
		if (connection && connection->context->transaction.HasActiveTransaction()) {
			return nullptr;
		}
	}
	// Heap, temp and ducklake FDW tables and ducklake-only functions need this backend's engine.
	if (pgddb::ContainsPostgresTable((Node *)query, NULL) || QueryReferencesDucklakeOnlyFunc(query)) {
		return nullptr;
	}

	pgddb::QueryFrame frame;
	frame.sql = pgddb::DeparseQuery(query);
	frame.params = pgddb::DispatchParameters(params);
	frame.settings.emplace_back("TimeZone", duckdb::Value(pgddb::pg::GetConfigOption("TimeZone")));
	if (default_table_path && default_table_path[0] != '\0') {
		frame.settings.emplace_back("ducklake_default_table_path", duckdb::Value(default_table_path));
	}
	return DuckLakeWorker::Get().Dispatch(std::move(frame));
}

void
InitDuckdbWorker() {
	DuckLakeWorker::Get().Init();
	pgddb::pgddb_dispatch_hook = DispatchToWorker;
}

} // namespace pgducklake

extern "C" {

PGDLLEXPORT void
pgducklake_duckdb_worker_main(Datum main_arg) {
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	// Main connects to the database itself and never returns.
	try {
		pgducklake::DuckLakeWorker::Get().Main(DatumGetObjectId(main_arg));
	} catch (const std::exception &e) {
		elog(FATAL, "pg_ducklake duckdb worker: %s", pgducklake::DuckDBErrorMessage(e).c_str());
	}
}

/* ducklake.worker_stats(): one row, all NULL while no worker is registered for this database. */
DECLARE_PG_FUNCTION(ducklake_worker_stats) {
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	if (!rsinfo || !IsA(rsinfo, ReturnSetInfo) || !(rsinfo->allowedModes & SFRM_Materialize)) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("worker_stats must be called in a materializing set context")));
	}

	TupleDesc tupdesc;
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("return type must be a row type")));
	}

	MemoryContext old_context = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	Tuplestorestate *tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = BlessTupleDesc(tupdesc);
	MemoryContextSwitchTo(old_context);

	pgddb::DuckdbWorker::Stats stats = {};
	bool registered = pgducklake::DuckLakeWorker::Get().GetStats(stats);
	Datum values[3] = {Int32GetDatum(stats.pid), Int32GetDatum(stats.active_sessions),
	                   Int64GetDatum((int64)stats.total_sessions)};
	bool nulls[3] = {!registered, !registered, !registered};
	tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);
	return (Datum)0;
}

} // extern "C"
