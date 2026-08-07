// COPY FROM STDIN into inlined DuckLake tables: reads tuples via PG's COPY
// protocol, converts to the inlined data table's column types, inserts via
// table_multi_insert().

#include "pgducklake/direct_insert/copy_from.hpp"
#include "pgducklake/direct_insert/inline_col_stats.hpp"
#include "pgducklake/direct_insert/native_inline_writer.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"

extern "C" {
#include "postgres.h"

#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "parser/parse_node.h"
#include "utils/acl.h"
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

static char *
OutputFunctionCallIso(FmgrInfo *flinfo, Datum value) {
	char *result = NULL;
	int saved_style = DateStyle;
	int saved_order = DateOrder;
	PG_TRY();
	{
		DateStyle = USE_ISO_DATES;
		DateOrder = DATEORDER_YMD;
		result = OutputFunctionCall(flinfo, value);
		DateStyle = saved_style;
		DateOrder = saved_order;
	}
	PG_CATCH();
	{
		DateStyle = saved_style;
		DateOrder = saved_order;
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
}

static ColumnConvInfo *
BuildColumnConvInfo(TupleDesc user_tupdesc, TupleDesc inlined_tupdesc) {
	int natts = user_tupdesc->natts;
	ColumnConvInfo *conv = (ColumnConvInfo *)palloc0(sizeof(ColumnConvInfo) * natts);

	int inl_column = INLINED_SYSTEM_COLS;
	for (int i = 0; i < natts; i++) {
		Form_pg_attribute user_att = TupleDescAttr(user_tupdesc, i);
		if (user_att->attisdropped) {
			continue;
		}
		Form_pg_attribute inl_att = TupleDescAttr(inlined_tupdesc, inl_column++);

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

static void
CheckCopyPermissions(Relation rel, List *attlist) {
	Oid relid = RelationGetRelid(rel);
	AclResult table_result = pg_class_aclcheck(relid, GetUserId(), ACL_INSERT);
	if (table_result == ACLCHECK_OK) {
		return;
	}

	List *attnums = CopyGetAttnums(RelationGetDescr(rel), rel, attlist);
	ListCell *lc;
	foreach (lc, attnums) {
		AttrNumber attnum = (AttrNumber)lfirst_int(lc);
		if (pg_attribute_aclcheck(relid, attnum, GetUserId(), ACL_INSERT) != ACLCHECK_OK) {
			aclcheck_error(table_result, OBJECT_TABLE, RelationGetRelationName(rel));
		}
	}
}

static void
CheckNativeCopySemantics(Relation rel, CopyStmt *stmt) {
	CheckCopyPermissions(rel, stmt->attlist);

	ListCell *lc;
	foreach (lc, stmt->options) {
		DefElem *option = (DefElem *)lfirst(lc);
		if (strcmp(option->defname, "on_error") == 0) {
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			                errmsg("COPY FROM STDIN with ON_ERROR is not supported for DuckLake tables")));
		}
		if (strcmp(option->defname, "freeze") == 0 && defGetBoolean(option)) {
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			                errmsg("COPY FROM STDIN with FREEZE is not supported for DuckLake tables")));
		}
	}

	if (IsTransactionBlock() || IsolationUsesXactSnapshot()) {
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("COPY FROM STDIN into a DuckLake table is not supported in an explicit transaction"),
		         errdetail("The native COPY writer publishes one statement in an implicit READ COMMITTED transaction, "
		                   "and the DuckDB fallback cannot consume the PostgreSQL COPY stream."),
		         errhint("Run COPY as an autocommit statement.")));
	}
	if (stmt->whereClause != NULL) {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("COPY FROM STDIN with a WHERE clause is not supported for DuckLake tables")));
	}

	TupleDesc tupdesc = RelationGetDescr(rel);
	TupleConstr *constr = tupdesc->constr;
	bool unsupported = rel->rd_rel->relkind != RELKIND_RELATION || rel->trigdesc != NULL || rel->rd_rules != NULL ||
	                   rel->rd_rel->relrowsecurity ||
	                   (constr && (constr->has_not_null || constr->num_check > 0 || constr->has_generated_stored));
#if PG_VERSION_NUM >= 180000
	unsupported = unsupported || (constr && constr->has_generated_virtual);
#endif
	for (int i = 0; !unsupported && i < tupdesc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (!attr->attisdropped) {
			unsupported = attr->attgenerated != '\0' || attr->attidentity != '\0';
		}
	}
	if (unsupported) {
		ereport(
		    ERROR,
		    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		     errmsg("COPY FROM STDIN cannot use the native writer for this DuckLake table"),
		     errdetail("Constraints, generated or identity columns, triggers, rules, and row-level security are not "
		               "supported by the native COPY producer.")));
	}
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
	if (XactReadOnly && !user_rel->rd_islocaltemp) {
		PreventCommandIfReadOnly("COPY FROM");
	}
	Oid user_relid = RelationGetRelid(user_rel);
	CheckNativeCopySemantics(user_rel, stmt);

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

	int live_natts = 0;
	for (int i = 0; i < natts; i++) {
		if (!TupleDescAttr(user_tupdesc, i)->attisdropped) {
			live_natts++;
		}
	}
	if (inlined_tupdesc->natts != live_natts + INLINED_SYSTEM_COLS) {
		table_close(inlined_rel, RowExclusiveLock);
		table_close(user_rel, RowExclusiveLock);
		ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
		                errmsg("inlined data table column count mismatch: expected %d, got %d",
		                       live_natts + INLINED_SYSTEM_COLS, inlined_tupdesc->natts)));
	}

	ColumnConvInfo *conv = BuildColumnConvInfo(user_tupdesc, inlined_tupdesc);

	SPI_connect();
	InlineColStats *col_stats = CreateInlineColStats(table_id, live_natts);
	SPI_finish();
	int stats_col = 0;
	for (int i = 0; i < natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(user_tupdesc, i);
		if (!attr->attisdropped) {
			SetupInlineColStatsColumn(col_stats, stats_col++, attr->atttypid);
		}
	}

	NativeInlineWriteBatch batch =
	    PrepareNativeInlineWrite(user_relid, table_id, schema_version, NATIVE_WRITER_UNKNOWN_ROW_COUNT);
	BindNativeInlineWriteRelation(&batch, inlined_rel);
	uint64_t begin_snapshot = batch.candidate_snapshot_id;
	uint64_t next_row_id = batch.candidate_row_id;

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
	MemoryContext old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	while (NextCopyFrom(cstate, econtext, copy_values, copy_nulls)) {
		NativeWriterStatsAdd(NW_COPY_ROWS_CONSUMED);
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

		stats_col = 0;
		for (int i = 0; i < natts; i++) {
			if (TupleDescAttr(user_tupdesc, i)->attisdropped) {
				continue;
			}
			int dst = stats_col + INLINED_SYSTEM_COLS;
			if (copy_nulls[i]) {
				ObserveInlineColStatsNull(col_stats, stats_col);
				slot_values[dst] = (Datum)0;
				slot_isnull[dst] = true;
			} else if (conv[i].needs_text_conv) {
				ObserveInlineColStatsDatum(col_stats, stats_col, copy_values[i]);
				char *str = OutputFunctionCallIso(&conv[i].typoutput_finfo, copy_values[i]);
				slot_values[dst] = CStringGetTextDatum(str);
				slot_isnull[dst] = false;
				pfree(str);
			} else {
				ObserveInlineColStatsDatum(col_stats, stats_col, copy_values[i]);
				slot_values[dst] = copy_values[i];
				slot_isnull[dst] = false;
			}
			stats_col++;
		}

		ExecStoreVirtualTuple(slot);
		nslots++;

		if (nslots >= MAX_BUFFERED_TUPLES) {
			table_multi_insert(inlined_rel, slots, nslots, cid, 0, bistate);
			RecordNativeInlineWriteRows(&batch, slots, nslots);
			rows_inserted += nslots;
			NativeWriterStatsAdd(NW_PAYLOAD_ROWS, nslots);
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
		RecordNativeInlineWriteRows(&batch, slots, nslots);
		rows_inserted += nslots;
		NativeWriterStatsAdd(NW_PAYLOAD_ROWS, nslots);
	}
	MemoryContextSwitchTo(old_context);

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

	batch.column_stats = FinalizeInlineColStats(col_stats, &batch.column_stats_count);

	/* Keep the inline relation lock until transaction end so captured TIDs
	 * cannot be invalidated by a rewrite before publication. */
	table_close(inlined_rel, NoLock);
	table_close(user_rel, RowExclusiveLock);

	if (rows_inserted > 0) {
		batch.rows_inserted = rows_inserted;
		PublishNativeInlineWrite(batch);
	}

	return rows_inserted;
}

} // namespace pgducklake
