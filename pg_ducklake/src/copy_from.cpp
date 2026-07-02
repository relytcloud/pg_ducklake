// COPY FROM STDIN into inlined DuckLake tables: reads tuples via PG's COPY
// protocol, converts to the inlined data table's column types, inserts via
// table_multi_insert().

#include "pgducklake/catalog_sync.hpp"
#include "pgducklake/copy_from.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"

extern "C" {
#include "postgres.h"

#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "parser/parse_node.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

namespace pgducklake {

/* Match PG's copyfrom.c MAX_BUFFERED_TUPLES (not exported in headers). */
#define MAX_BUFFERED_TUPLES 1000

/* System columns prepended to the inlined data table: row_id, begin_snapshot, end_snapshot. */
#define INLINED_SYSTEM_COLS 3

struct ColumnConvInfo {
	bool needs_text_conv;     /* true if user type -> VARCHAR via output func */
	FmgrInfo typoutput_finfo; /* cached output function (avoids per-row syscache lookup) */
};

static ColumnConvInfo *
BuildColumnConvInfo(TupleDesc user_tupdesc, TupleDesc inlined_tupdesc) {
	int natts = user_tupdesc->natts;
	ColumnConvInfo *conv = (ColumnConvInfo *)palloc0(sizeof(ColumnConvInfo) * natts);

	for (int i = 0; i < natts; i++) {
		Form_pg_attribute user_att = TupleDescAttr(user_tupdesc, i);
		Form_pg_attribute inl_att = TupleDescAttr(inlined_tupdesc, i + INLINED_SYSTEM_COLS);

		Oid user_type = user_att->atttypid;
		Oid inl_type = inl_att->atttypid;

		if (user_type == inl_type) {
			conv[i].needs_text_conv = false;
		} else if (inl_type == VARCHAROID || inl_type == TEXTOID) {
			/* Inlined stores as VARCHAR (date, timestamp, ubigint, etc.): use PG output function. */
			Oid typoutput;
			bool typisvarlena;
			getTypeOutputInfo(user_type, &typoutput, &typisvarlena);
			fmgr_info(typoutput, &conv[i].typoutput_finfo);
			conv[i].needs_text_conv = true;
		} else {
			/* text and bytea are both varlena with identical in-memory format: pass through. */
			conv[i].needs_text_conv = false;
		}
	}

	return conv;
}

static Relation
OpenInlinedDataTable(uint64_t table_id, uint64_t schema_version, LOCKMODE lockmode) {
	char relname[NAMEDATALEN];
	snprintf(relname, sizeof(relname), "ducklake_inlined_data_%llu_%llu", (unsigned long long)table_id,
	         (unsigned long long)schema_version);

	Oid ducklake_nsp = get_namespace_oid("ducklake", false);
	Oid relid = get_relname_relid(relname, ducklake_nsp);
	if (!OidIsValid(relid)) {
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("inlined data table \"%s\" does not exist", relname),
		                errhint("Call ducklake.ensure_inlined_data_table() first.")));
	}

	return table_open(relid, lockmode);
}

uint64_t
DucklakeCopyFromStdin(CopyStmt *stmt, const char *query_string) {
	Relation user_rel = table_openrv(stmt->relation, RowExclusiveLock);
	Oid user_relid = RelationGetRelid(user_rel);

	uint64_t table_id, schema_version;
	if (!GetTableInliningInfo(user_relid, &table_id, &schema_version)) {
		/* Capture name before close -- RelationGetRelationName is unsafe after table_close. */
		char *relname = pstrdup(RelationGetRelationName(user_rel));
		table_close(user_rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("COPY FROM STDIN requires an inlined data table for \"%s\"", relname),
		                errhint("Call ducklake.ensure_inlined_data_table('%s'::regclass) first.", relname)));
	}

	Relation inlined_rel = OpenInlinedDataTable(table_id, schema_version, RowExclusiveLock);

	TupleDesc user_tupdesc = RelationGetDescr(user_rel);
	TupleDesc inlined_tupdesc = RelationGetDescr(inlined_rel);
	int natts = user_tupdesc->natts;

	if (inlined_tupdesc->natts != natts + INLINED_SYSTEM_COLS) {
		table_close(inlined_rel, RowExclusiveLock);
		table_close(user_rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
		                errmsg("inlined data table column count mismatch: expected %d, got %d",
		                       natts + INLINED_SYSTEM_COLS, inlined_tupdesc->natts)));
	}

	ColumnConvInfo *conv = BuildColumnConvInfo(user_tupdesc, inlined_tupdesc);

	uint64_t begin_snapshot = GetNextSnapshotId();
	uint64_t next_row_id = GetNextRowIdForTable(table_id, schema_version);

	ParseState *pstate = make_parsestate(NULL);
	pstate->p_sourcetext = query_string;
	CopyFromState cstate =
	    BeginCopyFrom(pstate, user_rel, NULL, NULL /* STDIN */, false, NULL, stmt->attlist, stmt->options);

	EState *estate = CreateExecutorState();
	ExprContext *econtext = GetPerTupleExprContext(estate);

	Datum *copy_values = (Datum *)palloc(sizeof(Datum) * natts);
	bool *copy_nulls = (bool *)palloc(sizeof(bool) * natts);

	TupleTableSlot **slots = (TupleTableSlot **)palloc(sizeof(TupleTableSlot *) * MAX_BUFFERED_TUPLES);
	for (int i = 0; i < MAX_BUFFERED_TUPLES; i++) {
		slots[i] = MakeSingleTupleTableSlot(inlined_tupdesc, &TTSOpsVirtual);
	}

	BulkInsertState bistate = GetBulkInsertState();
	CommandId cid = GetCurrentCommandId(true);

	int nslots = 0;
	uint64 rows_inserted = 0;

	while (NextCopyFrom(cstate, econtext, copy_values, copy_nulls)) {
		TupleTableSlot *slot = slots[nslots];
		ExecClearTuple(slot);

		Datum *slot_values = slot->tts_values;
		bool *slot_isnull = slot->tts_isnull;

		slot_values[0] = Int64GetDatum((int64)next_row_id++);
		slot_isnull[0] = false;
		slot_values[1] = Int64GetDatum((int64)begin_snapshot);
		slot_isnull[1] = false;
		slot_values[2] = (Datum)0; /* end_snapshot = NULL */
		slot_isnull[2] = true;

		for (int i = 0; i < natts; i++) {
			int dst = i + INLINED_SYSTEM_COLS;
			if (copy_nulls[i]) {
				slot_values[dst] = (Datum)0;
				slot_isnull[dst] = true;
			} else if (conv[i].needs_text_conv) {
				char *str = OutputFunctionCall(&conv[i].typoutput_finfo, copy_values[i]);
				slot_values[dst] = CStringGetTextDatum(str);
				slot_isnull[dst] = false;
				pfree(str);
			} else {
				slot_values[dst] = copy_values[i];
				slot_isnull[dst] = false;
			}
		}

		ExecStoreVirtualTuple(slot);
		nslots++;

		if (nslots >= MAX_BUFFERED_TUPLES) {
			table_multi_insert(inlined_rel, slots, nslots, cid, 0, bistate);
			rows_inserted += nslots;
			for (int i = 0; i < nslots; i++) {
				ExecClearTuple(slots[i]);
			}
			nslots = 0;
			/* Reset per-batch, not per-tuple: NextCopyFrom Datums accumulate in the econtext until table_multi_insert
			 * materializes them. */
			ResetPerTupleExprContext(estate);
		}
	}

	if (nslots > 0) {
		table_multi_insert(inlined_rel, slots, nslots, cid, 0, bistate);
		rows_inserted += nslots;
	}

	table_finish_bulk_insert(inlined_rel, 0);
	FreeBulkInsertState(bistate);
	EndCopyFrom(cstate);
	free_parsestate(pstate);

	for (int i = 0; i < MAX_BUFFERED_TUPLES; i++) {
		ExecDropSingleTupleTableSlot(slots[i]);
	}
	FreeExecutorState(estate);

	pfree(copy_values);
	pfree(copy_nulls);
	pfree(slots);
	pfree(conv);

	table_close(inlined_rel, RowExclusiveLock);
	table_close(user_rel, RowExclusiveLock);

	if (rows_inserted > 0) {
		SkipSnapshotSyncGuard sync_guard;
		/* Concurrent commits race on MAX(snapshot_id)+1; the loser hits 23505.
		 * Translate to 40001 so client retry adapters retry transparently
		 * (mirrors DirectInsert_ExecCustomScan). */
		MemoryContext old_ctx = CurrentMemoryContext;
		PG_TRY();
		{
			CreateSnapshotForDirectInsert(begin_snapshot, table_id, rows_inserted);
		}
		PG_CATCH();
		{
			MemoryContextSwitchTo(old_ctx);
			ErrorData *edata = CopyErrorData();
			if (edata->sqlerrcode == ERRCODE_UNIQUE_VIOLATION) {
				FreeErrorData(edata);
				FlushErrorState();
				ereport(ERROR,
				        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				         errmsg("ducklake COPY: snapshot_id allocation lost the race"), errhint("Retry the COPY.")));
			}
			FreeErrorData(edata);
			PG_RE_THROW();
		}
		PG_END_TRY();
		CommandCounterIncrement();
	}

	return rows_inserted;
}

} // namespace pgducklake
