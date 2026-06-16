#include "pgddb/scan/postgres_table_reader.hpp"
#include "pgddb/pgddb_process_lock.hpp"
#include "pgddb/pgddb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "commands/explain.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif
#include "executor/executor.h"
#include "executor/execParallel.h"
#include "executor/tqueue.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"
}

#include "pgddb/vendor/pg_list.hpp"

#include <cmath>

namespace pgddb {

bool duckdb_log_pg_explain = false;
int duckdb_max_workers_per_postgres_scan = 2;

PostgresTableReader::PostgresTableReader()
    : table_scan_query_desc(nullptr), table_scan_planstate(nullptr), parallel_executor_info(nullptr),
      parallel_worker_readers(nullptr), slot(nullptr), nworkers_launched(0), nreaders(0), next_parallel_reader(0),
      entered_parallel_mode(false), cleaned_up(false) {
}

void
PostgresTableReader::Init(const char *table_scan_query, bool count_tuples_only) {
	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	PostgresScopedStackReset scoped_stack_reset;
	PostgresMemberGuard(PostgresTableReader::InitUnsafe, table_scan_query, count_tuples_only);
}

void
PostgresTableReader::InitUnsafe(const char *table_scan_query, bool count_tuples_only) {
	List *raw_parsetree_list = pg_parse_query(table_scan_query);
	Assert(list_length(raw_parsetree_list) == 1);
	RawStmt *raw_parsetree = linitial_node(RawStmt, raw_parsetree_list);

#if PG_VERSION_NUM >= 150000
	List *query_list = pg_analyze_and_rewrite_fixedparams(raw_parsetree, table_scan_query, nullptr, 0, nullptr);
#else
	List *query_list = pg_analyze_and_rewrite(raw_parsetree, table_scan_query, nullptr, 0, nullptr);
#endif

	Assert(list_length(query_list) == 1);
	Query *query = linitial_node(Query, query_list);

	Assert(list_length(query->rtable) == 1);
	RangeTblEntry *rte = linitial_node(RangeTblEntry, query->rtable);

	char persistence = get_rel_persistence(rte->relid);

	PlannedStmt *planned_stmt = standard_planner(query, table_scan_query, 0, nullptr);

	table_scan_query_desc = CreateQueryDesc(planned_stmt, table_scan_query, GetActiveSnapshot(), InvalidSnapshot,
	                                        None_Receiver, nullptr, nullptr, 0);

	standard_ExecutorStart(table_scan_query_desc, 0);

	table_scan_planstate = ExecInitNode(planned_stmt->planTree, table_scan_query_desc->estate, 0);

	bool run_scan_with_parallel_workers = persistence != RELPERSISTENCE_TEMP;
	run_scan_with_parallel_workers &= CanTableScanRunInParallel(table_scan_query_desc->planstate->plan);

	/* Temp tables cannot be excuted with parallel workers, and whole plan should be parallel aware */
	if (run_scan_with_parallel_workers) {
		InitRunWithParallelScan(planned_stmt, count_tuples_only);
	}

	if (duckdb_log_pg_explain) {
		ExplainState *es = (ExplainState *)palloc0(sizeof(ExplainState));
		es->str = makeStringInfo();
		es->format = EXPLAIN_FORMAT_TEXT;
		ExplainPrintPlan(es, table_scan_query_desc);
		elog(NOTICE, "(PGDuckDB/PostgresTableReader)\n\nQUERY: %s\nRUNNING: %s.\nEXECUTING: \n%s", table_scan_query,
		     !nreaders ? "IN PROCESS THREAD" : psprintf("ON %d PARALLEL WORKER(S)", nreaders), es->str->data);
	}

	slot = ExecInitExtraTupleSlot(table_scan_query_desc->estate, table_scan_planstate->ps_ResultTupleDesc,
	                              &TTSOpsMinimalTuple);
}

void
PostgresTableReader::InitRunWithParallelScan(PlannedStmt *planned_stmt, bool count_tuples_only) {
	int parallel_workers = 0;
	if (count_tuples_only) {
		/* For count_tuples_only we will try to execute aggregate node on table scan */
		planned_stmt->planTree->parallel_aware = true;
		MarkPlanParallelAware((Plan *)table_scan_query_desc->planstate->plan->lefttree);
		parallel_workers = ParallelWorkerNumber(planned_stmt->planTree->lefttree->plan_rows);
	} else {
		MarkPlanParallelAware(table_scan_query_desc->planstate->plan);
		parallel_workers = ParallelWorkerNumber(planned_stmt->planTree->plan_rows);
	}

	bool interrupts_can_be_process = INTERRUPTS_CAN_BE_PROCESSED();
	if (!interrupts_can_be_process) {
		RESUME_CANCEL_INTERRUPTS();
	}

	EnterParallelMode();
	entered_parallel_mode = true;

	ParallelContext *pcxt;
	parallel_executor_info =
	    ExecInitParallelPlan(table_scan_planstate, table_scan_query_desc->estate, nullptr, parallel_workers, -1);
	pcxt = parallel_executor_info->pcxt;
	LaunchParallelWorkers(pcxt);
	nworkers_launched = pcxt->nworkers_launched;

	if (pcxt->nworkers_launched > 0) {
		ExecParallelCreateReaders(parallel_executor_info);
		nreaders = pcxt->nworkers_launched;
		parallel_worker_readers = (void **)palloc(nreaders * sizeof(TupleQueueReader *));
		memcpy(parallel_worker_readers, parallel_executor_info->reader, nreaders * sizeof(TupleQueueReader *));
	}

	if (!interrupts_can_be_process) {
		HOLD_CANCEL_INTERRUPTS();
	}
}

/*
 * The returned slot is allocated in `table_scan_query_desc->estate` and is freed when
 * that query desc is destroyed.
 */
TupleTableSlot *
PostgresTableReader::InitTupleSlot() {
	D_ASSERT(!cleaned_up);
	return PostgresFunctionGuard(ExecInitExtraTupleSlot, table_scan_query_desc->estate,
	                             table_scan_planstate->ps_ResultTupleDesc, &TTSOpsMinimalTuple);
}

PostgresTableReader::~PostgresTableReader() {
	if (cleaned_up) {
		return;
	}
	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	Cleanup();
}

void
PostgresTableReader::Cleanup() {
	D_ASSERT(!cleaned_up);
	cleaned_up = true;
	PostgresScopedStackReset scoped_stack_reset;
	PostgresMemberGuard(PostgresTableReader::CleanupUnsafe);
}

void
PostgresTableReader::CleanupUnsafe() {
	if (table_scan_planstate) {
		ExecEndNode(table_scan_planstate);
		table_scan_planstate = nullptr;
	}

	if (parallel_executor_info != NULL) {
		ExecParallelFinish(parallel_executor_info);
		ExecParallelCleanup(parallel_executor_info);
		parallel_executor_info = nullptr;
	}

	if (parallel_worker_readers) {
		pfree(parallel_worker_readers);
		parallel_worker_readers = nullptr;
	}

	if (table_scan_query_desc) {
		standard_ExecutorFinish(table_scan_query_desc);
		standard_ExecutorEnd(table_scan_query_desc);
		FreeQueryDesc(table_scan_query_desc);
		table_scan_query_desc = nullptr;
	}

	if (entered_parallel_mode) {
		ExitParallelMode();
		entered_parallel_mode = false;
	}
}

/*
 * 0 disables parallelism; cardinality <= 2^16 uses one worker; above that, up to
 * `duckdb_max_workers_per_postgres_scan` capped by `max_parallel_workers`.
 */
int
PostgresTableReader::ParallelWorkerNumber(Cardinality cardinality) {
	static const int cardinality_threshold = 1 << 16;
	if (!duckdb_max_workers_per_postgres_scan) {
		return 0;
	}
	if (cardinality <= cardinality_threshold) {
		return 1;
	}
	return std::min(duckdb_max_workers_per_postgres_scan, max_parallel_workers);
}

bool
PostgresTableReader::CanTableScanRunInParallel(Plan *plan) {
	switch (nodeTag(plan)) {
	case T_SeqScan:
	case T_IndexScan:
	case T_IndexOnlyScan:
	case T_BitmapHeapScan:
		return true;
	case T_Append: {
		ListCell *l;
		foreach (l, ((Append *)plan)->appendplans) {
			if (!CanTableScanRunInParallel((Plan *)lfirst(l))) {
				return false;
			}
		}
		return true;
	}
	/* This is special case for COUNT(*) */
	case T_Agg:
		return true;
	default:
		return false;
	}
}

bool
PostgresTableReader::MarkPlanParallelAware(Plan *plan) {
	switch (nodeTag(plan)) {
	case T_SeqScan:
	case T_IndexScan:
	case T_IndexOnlyScan:
	case T_Append: {
		plan->parallel_aware = true;
		return true;
	}
	case T_BitmapHeapScan: {
		plan->parallel_aware = true;
		return MarkPlanParallelAware(plan->lefttree);
	}
	case T_BitmapIndexScan: {
		((BitmapIndexScan *)plan)->isshared = true;
		return true;
	}
	case T_BitmapAnd: {
		return MarkPlanParallelAware((Plan *)linitial(((BitmapAnd *)plan)->bitmapplans));
	}
	case T_BitmapOr: {
		((BitmapOr *)plan)->isshared = true;
		return MarkPlanParallelAware((Plan *)linitial(((BitmapOr *)plan)->bitmapplans));
	}
	default: {
		std::ostringstream oss;
		oss << "Unknown postgres scan query plan node: " << nodeTag(plan);
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, oss.str().c_str());
	}
	}
}

/* GlobalProcessLock must be held before calling this. */
TupleTableSlot *
PostgresTableReader::GetNextTuple() {
	return PostgresMemberGuard(PostgresTableReader::GetNextTupleUnsafe);
}

TupleTableSlot *
PostgresTableReader::GetNextTupleUnsafe() {
	if (nreaders > 0) {
		MinimalTuple worker_minmal_tuple = GetNextWorkerTuple();
		if (HeapTupleIsValid(worker_minmal_tuple)) {
			ExecStoreMinimalTuple(worker_minmal_tuple, slot, false);
			return slot;
		}
	}

	return ExecNextTupleUnsafe();
}

TupleTableSlot *
PostgresTableReader::ExecNextTupleUnsafe() {
	PostgresScopedStackReset scoped_stack_reset;
	table_scan_query_desc->estate->es_query_dsa = parallel_executor_info ? parallel_executor_info->area : NULL;
	TupleTableSlot *thread_scan_slot = ExecProcNode(table_scan_planstate);
	table_scan_query_desc->estate->es_query_dsa = NULL;
	return TupIsNull(thread_scan_slot) ? ExecClearTuple(slot) : thread_scan_slot;
}

static inline void
CopyMinimalTuple(MinimalTuple src_minimal_tuple, std::vector<uint8_t> &dst_buffer) {
	Size tuple_size = src_minimal_tuple->t_len + MINIMAL_TUPLE_DATA_OFFSET;
	dst_buffer.resize(tuple_size);
	memcpy(dst_buffer.data(), src_minimal_tuple, tuple_size);
}

/*
 * Only valid when the scan runs with parallel workers; caller must hold GlobalProcessLock.
 * Returns false when the scan is complete.
 */
bool
PostgresTableReader::GetNextMinimalWorkerTuple(std::vector<uint8_t> &minimal_tuple_buffer) {
	MinimalTuple worker_minmal_tuple = GetNextWorkerTuple();
	if (HeapTupleIsValid(worker_minmal_tuple)) {
		CopyMinimalTuple(worker_minmal_tuple, minimal_tuple_buffer);
		return true;
	}

	minimal_tuple_buffer.resize(0);
	return false;
}

int
PostgresTableReader::GetNextInProcessTuples(TupleTableSlot **slots, int max) {
	return PostgresMemberGuard(PostgresTableReader::GetNextInProcessTuplesUnsafe, slots, max);
}

int
PostgresTableReader::GetNextInProcessTuplesUnsafe(TupleTableSlot **slots, int max) {
	/* One guard (from GetNextInProcessTuples) covers the whole batch, not one per tuple. */
	int count = 0;
	for (; count < max; count++) {
		TupleTableSlot *thread_scan_slot = ExecNextTupleUnsafe();
		if (TupIsNull(thread_scan_slot)) {
			break;
		}
		/* Copy is required: the scan node reuses `thread_scan_slot` on the next ExecProcNode call. */
		ExecCopySlot(slots[count], thread_scan_slot);
	}
	return count;
}

bool
PostgresTableReader::GetNextCount(uint64_t *count_out) {
	TupleTableSlot *next_slot = GetNextTuple();
	if (TupIsNull(next_slot)) {
		return false;
	}
	slot_getallattrs(next_slot);
	*count_out = next_slot->tts_values[0];
	return true;
}

MinimalTuple
PostgresTableReader::GetNextWorkerTuple() {
	int nvisited = 0;
	TupleQueueReader *reader = NULL;
	MinimalTuple minimal_tuple = NULL;
	bool readerdone = false;
	/* Stop condition guards against another thread having already finished the scan and freed readers. */
	for (; next_parallel_reader < nreaders;) {
		reader = (TupleQueueReader *)parallel_worker_readers[next_parallel_reader];

		minimal_tuple = TupleQueueReaderNext(reader, true, &readerdone);

		if (readerdone) {
			--nreaders;
			if (nreaders == 0) {
				return NULL;
			}

			memmove(&parallel_worker_readers[next_parallel_reader], &parallel_worker_readers[next_parallel_reader + 1],
			        sizeof(TupleQueueReader *) * (nreaders - next_parallel_reader));
			if (next_parallel_reader >= nreaders) {
				next_parallel_reader = 0;
			}
			continue;
		}

		if (minimal_tuple) {
			return minimal_tuple;
		}

		next_parallel_reader++;
		if (next_parallel_reader >= nreaders) {
			next_parallel_reader = 0;
		}

		nvisited++;
		if (nvisited >= nreaders) {
			/* Safe because callers of GetNextTuple()/GetNextWorkerTuple() hold GlobalProcessLock. */
			WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, PG_WAIT_EXTENSION);
			/* No PostgresFunctionGuard: ResetLatch is trivial. */
			ResetLatch(MyLatch);
			nvisited = 0;
		}
	}

	return NULL;
}

} // namespace pgddb
