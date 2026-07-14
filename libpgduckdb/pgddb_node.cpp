#include "duckdb.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/exception.hpp"

#include "pgddb/pgddb_planner.hpp"
#include "pgddb/pgddb_types.hpp"
#include "pgddb/worker/worker_dispatch.hpp"
#include "pgddb/vendor/pg_explain.hpp"
#include "pgddb/pg/explain.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "tcop/pquery.h"
#include "nodes/params.h"
#include "utils/ruleutils.h"
}

#include "pgddb/pgddb_node.hpp"
#include "pgddb/pgddb_duckdb.hpp"
#include "pgddb/utility/cpp_wrapper.hpp"

namespace pgddb {

bool explain_analyze = false;
bool explain_ctas = false;
duckdb::ExplainFormat explain_format = duckdb::ExplainFormat::DEFAULT;

#define NEED_JSON_PLAN(fmt) ((fmt) == duckdb::ExplainFormat::JSON)

CustomScanMethods scan_methods;

DispatchToWorkerHook pgddb_dispatch_to_worker_hook = nullptr;
OpenRemoteScanHook pgddb_open_remote_scan_hook = nullptr;

static CustomExecMethods scan_exec_methods;

typedef struct DuckdbScanState {
	CustomScanState css; /* must be first field */
	const CustomScan *custom_scan;
	const Query *query;
	ParamListInfo params;
	duckdb::Connection *duckdb_connection;
	duckdb::PreparedStatement *prepared_statement;
	bool is_executed;
	bool fetch_next;
	duckdb::unique_ptr<duckdb::QueryResult> query_results;
	duckdb::idx_t column_count;
	duckdb::unique_ptr<duckdb::DataChunk> current_data_chunk;
	duckdb::idx_t current_row;
	duckdb::unique_ptr<WorkerResultStream> worker_stream;
} DuckdbScanState;

static void
CleanupDuckdbScanState(DuckdbScanState *state) {
	MemoryContextReset(state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
	ExecClearTuple(state->css.ss.ss_ScanTupleSlot);

	state->query_results.reset();
	state->current_data_chunk.reset();
	state->worker_stream.reset();

	if (state->prepared_statement) {
		delete state->prepared_statement;
		state->prepared_statement = nullptr;
	}
}

static Node *Duckdb_CreateCustomScanState(CustomScan *cscan);
static void Duckdb_BeginCustomScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *Duckdb_ExecCustomScan(CustomScanState *node);
static void Duckdb_EndCustomScan(CustomScanState *node);
static void Duckdb_ReScanCustomScan(CustomScanState *node);
static void Duckdb_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es);
static inline void formatDuckDbPlanForPG(const char *duckdb_plan, ExplainState *es);

static Node *
Duckdb_CreateCustomScanState(CustomScan *cscan) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)newNode(sizeof(DuckdbScanState), T_CustomScanState);
	CustomScanState *custom_scan_state = &duckdb_scan_state->css;
	duckdb_scan_state->custom_scan = cscan;

	duckdb_scan_state->query = (const Query *)linitial(cscan->custom_private);
	custom_scan_state->methods = &scan_exec_methods;
	return (Node *)custom_scan_state;
}

static void
Duckdb_BeginCustomScan_Cpp(CustomScanState *cscanstate, EState *estate, int /*eflags*/) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)cscanstate;

	StringInfo explain_prefix = makeStringInfo();

	bool is_explain_query = ActivePortal && ActivePortal->commandTag == CMDTAG_EXPLAIN;

	if (is_explain_query) {
		appendStringInfoString(explain_prefix, "EXPLAIN ");

		if (NEED_JSON_PLAN(explain_format))
			appendStringInfoChar(explain_prefix, '(');

		if (explain_analyze) {
			if (explain_ctas) {
				throw duckdb::NotImplementedException(
				    "Cannot use EXPLAIN ANALYZE with CREATE TABLE ... AS when using DuckDB execution");
			}
			if (NEED_JSON_PLAN(explain_format))
				appendStringInfoString(explain_prefix, "ANALYZE, ");
			else
				appendStringInfoString(explain_prefix, "ANALYZE ");
		}

		if (NEED_JSON_PLAN(explain_format)) {
			appendStringInfoString(explain_prefix, "FORMAT JSON )");
		}
	}

	duckdb::unique_ptr<duckdb::PreparedStatement> prepared_query =
	    Prepare(duckdb_scan_state->query, explain_prefix->data);

	if (prepared_query->HasError()) {
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR,
		                        "DuckDB re-planning failed: " + prepared_query->GetError());
	}

	if (!is_explain_query) {
		auto &prepared_result_types = prepared_query->GetTypes();

		size_t target_list_length = static_cast<size_t>(list_length(duckdb_scan_state->custom_scan->custom_scan_tlist));

		if (prepared_result_types.size() != target_list_length) {
			elog(ERROR,
			     "(PGDuckDB/CreatePlan) Number of columns returned by DuckDB query changed between planning and "
			     "execution, expected %zu got %zu",
			     target_list_length, prepared_result_types.size());
		}

		for (size_t i = 0; i < prepared_result_types.size(); i++) {
			Oid postgres_column_oid = pgddb::GetPostgresDuckDBType(prepared_result_types[i], true);

			TargetEntry *target_entry =
			    list_nth_node(TargetEntry, duckdb_scan_state->custom_scan->custom_scan_tlist, i);
			Var *var = castNode(Var, target_entry->expr);
			if (var->vartype != postgres_column_oid) {
				elog(ERROR, "Types returned by duckdb query changed between planning and execution, expected %d got %d",
				     var->vartype, postgres_column_oid);
			}
		}
	}

	// Dispatch to the shared worker when an extension opts in; else execute in-process.
	if (!is_explain_query && pgddb::pgddb_dispatch_to_worker_hook != nullptr) {
		auto stream = pgddb::pgddb_dispatch_to_worker_hook(duckdb_scan_state->query);
		if (stream) {
			duckdb_scan_state->worker_stream = std::move(stream);
		}
	}

	if (duckdb_scan_state->worker_stream) {
		duckdb_scan_state->duckdb_connection = nullptr;
		duckdb_scan_state->prepared_statement = nullptr;
		duckdb_scan_state->column_count = (duckdb::idx_t)list_length(duckdb_scan_state->custom_scan->custom_scan_tlist);
	} else {
		duckdb_scan_state->duckdb_connection = pgddb::GetConnection();
		duckdb_scan_state->prepared_statement = prepared_query.release();
	}
	duckdb_scan_state->params = estate->es_param_list_info;
	duckdb_scan_state->is_executed = false;
	duckdb_scan_state->fetch_next = true;
	duckdb_scan_state->css.ss.ps.ps_ResultTupleDesc = duckdb_scan_state->css.ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	HOLD_CANCEL_INTERRUPTS();
}

void
Duckdb_BeginCustomScan(CustomScanState *cscanstate, EState *estate, int eflags) {
	InvokeCPPFunc(Duckdb_BeginCustomScan_Cpp, cscanstate, estate, eflags);
}

static void
ExecuteQuery(DuckdbScanState *state) {
	auto &prepared = *state->prepared_statement;
	auto pg_params = state->params;
	const auto num_params = pg_params ? pg_params->numParams : 0;
	duckdb::case_insensitive_map_t<duckdb::BoundParameterData> named_values;

	for (int i = 0; i < num_params; i++) {
		ParamExternData *pg_param;
		ParamExternData tmp_workspace;
		duckdb::Value duckdb_param;

		// paramFetch resolves dynamic params; fall back to the static array.
		if (pg_params->paramFetch != NULL) {
			pg_param = pg_params->paramFetch(pg_params, i + 1, false, &tmp_workspace);
		} else {
			pg_param = &pg_params->params[i];
		}

		if (prepared.named_param_map.count(duckdb::to_string(i + 1)) == 0) {
			continue;
		}

		if (pg_param->isnull) {
			duckdb_param = duckdb::Value();
		} else if (OidIsValid(pg_param->ptype)) {
			duckdb_param = pgddb::ConvertPostgresParameterToDuckValue(pg_param->value, pg_param->ptype);
		} else {
			std::ostringstream oss;
			oss << "parameter '" << i << "' has an invalid type (" << pg_param->ptype << ") during query execution";
			throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, oss.str().c_str());
		}
		named_values[duckdb::to_string(i + 1)] = duckdb::BoundParameterData(duckdb_param);
	}

	// Streaming a result that reads a Postgres table races on PG resources (e.g. CTAS); force full materialization.
	bool allow_stream_result = !pgddb::ContainsPostgresTable((Node *)state->query, NULL);
	auto pending = prepared.PendingQuery(named_values, allow_stream_result);
	if (pending->HasError()) {
		return pending->ThrowError();
	}

	duckdb::PendingExecutionResult execution_result = duckdb::PendingExecutionResult::RESULT_NOT_READY;
	while (true) {
		execution_result = pending->ExecuteTask();
		if (duckdb::PendingQueryResult::IsResultReady(execution_result)) {
			break;
		}

		if (QueryCancelPending) {
			auto &connection = state->duckdb_connection;
			connection->Interrupt();
			auto &executor = duckdb::Executor::Get(*connection->context);
			executor.CancelTasks();

			try {
				// Eagerly drain pending tasks: the "Query cancelled" throw below runs
				// PostgresTableReader destructors that touch PG; an exception during that
				// unwind is UB and crashes the process.
				do {
					execution_result = pending->ExecuteTask();
				} while (execution_result != duckdb::PendingExecutionResult::EXECUTION_ERROR &&
				         execution_result != duckdb::PendingExecutionResult::NO_TASKS_AVAILABLE &&
				         execution_result != duckdb::PendingExecutionResult::EXECUTION_FINISHED);

				pending->Close();
			} catch (std::exception &ex) {
			}
			ProcessInterrupts();
			throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, "Query cancelled");
		}
	}

	if (execution_result == duckdb::PendingExecutionResult::EXECUTION_ERROR) {
		return pending->ThrowError();
	}

	state->query_results = pending->Execute();
	state->column_count = state->query_results->ColumnCount();
	state->is_executed = true;
}

static TupleTableSlot *
Duckdb_ExecCustomScan_Cpp(CustomScanState *node) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
	try {
		TupleTableSlot *slot = duckdb_scan_state->css.ss.ss_ScanTupleSlot;
		MemoryContext old_context;

		if (ActivePortal && ActivePortal->commandTag == CMDTAG_EXPLAIN) {
			ExecClearTuple(slot);
			return slot;
		}

		auto fetch_chunk = [duckdb_scan_state]() {
			return duckdb_scan_state->worker_stream ? duckdb_scan_state->worker_stream->Fetch()
			                                        : duckdb_scan_state->query_results->Fetch();
		};

		bool already_executed = duckdb_scan_state->is_executed;
		if (!already_executed) {
			if (duckdb_scan_state->worker_stream) {
				// Execution runs in the shared worker; results arrive via the stream.
				duckdb_scan_state->is_executed = true;
			} else {
				ExecuteQuery(duckdb_scan_state);
			}

			// PG only sets es_processed via ModifyTable, which our CustomScan replaced.
			// DuckDB's DML result is (Count BIGINT): read it into es_processed and end the
			// scan, so the empty tuple means "no RETURNING rows".
			if (duckdb_scan_state->query->commandType != CMD_SELECT) {
				auto chunk = fetch_chunk();
				uint64_t processed = 0;
				if (chunk && chunk->size() > 0 && chunk->ColumnCount() > 0) {
					try {
						processed = chunk->GetValue(0, 0).GetValue<uint64_t>();
					} catch (...) {
						// Result wasn't a single Count column; leave 0.
					}
				}
				while (duckdb_scan_state->worker_stream && fetch_chunk()) {
					// worker stream: drain to the Complete frame
				}
				duckdb_scan_state->css.ss.ps.state->es_processed = processed;
				MemoryContextReset(duckdb_scan_state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
				ExecClearTuple(slot);
				return slot;
			}
		}

		if (duckdb_scan_state->fetch_next) {
			duckdb_scan_state->current_data_chunk = fetch_chunk();
			duckdb_scan_state->current_row = 0;
			duckdb_scan_state->fetch_next = false;
			if (!duckdb_scan_state->current_data_chunk || duckdb_scan_state->current_data_chunk->size() == 0) {
				MemoryContextReset(duckdb_scan_state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
				ExecClearTuple(slot);
				return slot;
			}
		}

		MemoryContextReset(duckdb_scan_state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
		ExecClearTuple(slot);

		old_context = MemoryContextSwitchTo(duckdb_scan_state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

		for (idx_t col = 0; col < duckdb_scan_state->column_count; col++) {
			// FIXME: we should not use the Value API here, it's complicating the LIST conversion logic
			auto value = duckdb_scan_state->current_data_chunk->GetValue(col, duckdb_scan_state->current_row);
			if (value.IsNull()) {
				slot->tts_isnull[col] = true;
			} else {
				slot->tts_isnull[col] = false;
				if (!pgddb::ConvertDuckToPostgresValue(slot, value, col)) {
					throw duckdb::ConversionException("Value conversion failed");
				}
			}
		}

		MemoryContextSwitchTo(old_context);

		duckdb_scan_state->current_row++;
		if (duckdb_scan_state->current_row >= duckdb_scan_state->current_data_chunk->size()) {
			duckdb_scan_state->current_data_chunk.reset();
			duckdb_scan_state->fetch_next = true;
		}

		ExecStoreVirtualTuple(slot);
		return slot;
	} catch (std::exception &ex) {
		// Clean up only on error; on success the DuckDB objects must survive for
		// the next ExecCustomScan call, and EndCustomScan does the final cleanup.
		CleanupDuckdbScanState(duckdb_scan_state);
		throw;
	}
}

static TupleTableSlot *
Duckdb_ExecCustomScan(CustomScanState *node) {
	return InvokeCPPFunc(Duckdb_ExecCustomScan_Cpp, node);
}

static void
Duckdb_EndCustomScan_Cpp(CustomScanState *node) {
	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
	CleanupDuckdbScanState(duckdb_scan_state);
	// Resume only if a hold is actually held: EndCustomScan can run after interrupts
	// were already resumed, and resuming at count 0 trips Assert(QueryCancelHoldoffCount > 0)
	// on cassert builds.
	if (QueryCancelHoldoffCount > 0) {
		RESUME_CANCEL_INTERRUPTS();
	}
}

void
Duckdb_EndCustomScan(CustomScanState *node) {
	InvokeCPPFunc(Duckdb_EndCustomScan_Cpp, node);
}

void
Duckdb_ReScanCustomScan(CustomScanState * /*node*/) {
}

static void
Duckdb_ExplainCustomScan_Cpp(CustomScanState *node, ExplainState *es) {
	// XXX: ExplainOneQueryHook does not run for EXPLAIN EXECUTE, so set these here too.
	// It runs too late to affect the DuckDB query but keeps the code below from crashing
	// (and a second EXPLAIN EXECUTE then shows the intended output).
	explain_analyze = pgddb::pg::IsExplainAnalyze(es);
	explain_format = pgddb::pg::DuckdbExplainFormat(es);

	DuckdbScanState *duckdb_scan_state = (DuckdbScanState *)node;
	ExecuteQuery(duckdb_scan_state);

	auto chunk = duckdb_scan_state->query_results->Fetch();
	if (!chunk || chunk->size() == 0) {
		return;
	}

	/* Is it safe to hardcode this as result of DuckDB explain? */
	auto value = chunk->GetValue(1, 0).GetValue<duckdb::string>();

	/* Fully consume the stream */
	do {
		chunk = duckdb_scan_state->query_results->Fetch();
	} while (chunk && chunk->size() > 0);

	std::ostringstream explain_output;
	explain_output << "\n\n" << value << "\n";
	if (NEED_JSON_PLAN(explain_format)) {

		if (linitial_int(es->grouping_stack) != 0)
			appendStringInfoChar(es->str, ',');
		else
			linitial_int(es->grouping_stack) = 1;
		appendStringInfoChar(es->str, '\n');
		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfoString(es->str, "\"DuckDB Execution Plan\": ");
		formatDuckDbPlanForPG(value.c_str(), es);
	} else
		pgddb::pg::ExplainPropertyText("DuckDB Execution Plan", explain_output.str().c_str(), es);
}

static inline void
formatDuckDbPlanForPG(const char *duckdb_plan, ExplainState *es) {
	const char *ptr = duckdb_plan;
	while (*ptr != '\0') {
		appendStringInfoChar(es->str, *ptr);
		if (*ptr == '\n') {
			appendStringInfoSpaces(es->str, es->indent * 2);
		}

		ptr++;
	}
}

void
Duckdb_ExplainCustomScan(CustomScanState *node, List * /*ancestors*/, ExplainState *es) {
	InvokeCPPFunc(Duckdb_ExplainCustomScan_Cpp, node, es);
}

void
InitNode(const char *custom_scan_name) {
	memset(&scan_methods, 0, sizeof(scan_methods));
	scan_methods.CustomName = custom_scan_name;
	scan_methods.CreateCustomScanState = Duckdb_CreateCustomScanState;
	RegisterCustomScanMethods(&scan_methods);

	memset(&scan_exec_methods, 0, sizeof(scan_exec_methods));
	scan_exec_methods.CustomName = custom_scan_name;

	scan_exec_methods.BeginCustomScan = Duckdb_BeginCustomScan;
	scan_exec_methods.ExecCustomScan = Duckdb_ExecCustomScan;
	scan_exec_methods.EndCustomScan = Duckdb_EndCustomScan;
	scan_exec_methods.ReScanCustomScan = Duckdb_ReScanCustomScan;

	scan_exec_methods.EstimateDSMCustomScan = NULL;
	scan_exec_methods.InitializeDSMCustomScan = NULL;
	scan_exec_methods.ReInitializeDSMCustomScan = NULL;
	scan_exec_methods.InitializeWorkerCustomScan = NULL;
	scan_exec_methods.ShutdownCustomScan = NULL;

	scan_exec_methods.ExplainCustomScan = Duckdb_ExplainCustomScan;
}

} // namespace pgddb
