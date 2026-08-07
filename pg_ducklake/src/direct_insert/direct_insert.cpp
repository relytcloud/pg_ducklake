/* Planner-time detection of INSERT patterns (UNNEST($n), VALUES) that
 * bypass DuckDB and write straight into the inlined data table. */

#include "pgducklake/direct_insert/direct_insert.hpp"
#include "pgducklake/duckdb_manager.hpp"
#include "pgducklake/guc.hpp"
#include "pgducklake/direct_insert/inline_col_stats.hpp"
#include "pgducklake/direct_insert/native_inline_writer.hpp"
#include "pgducklake/pgducklake_metadata_manager.hpp"

#include <unordered_map>

#include <duckdb.hpp>

extern "C" {
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#endif
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/value.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tuplestore.h"
}

namespace pgducklake {

/* Session-level metadata caches, stable across EXECUTE calls; cleared on
 * DuckDB instance recycle via ResetDirectInsertCaches(). */
struct InliningInfoCache {
	uint64_t table_id;
	uint64_t schema_version;
	int64_t row_limit;
};

struct InlinedColumnTypesCache {
	List *col_types; // List of Oid, palloc'd in TopMemoryContext
};

static std::unordered_map<Oid, InliningInfoCache> inlining_info_cache;
/* Keyed by (table_id, schema_version) so column-type DDL invalidates it. */
struct TableSchemaKey {
	uint64_t table_id;
	uint64_t schema_version;
	bool
	operator==(const TableSchemaKey &o) const {
		return table_id == o.table_id && schema_version == o.schema_version;
	}
};
struct TableSchemaKeyHash {
	size_t
	operator()(const TableSchemaKey &k) const {
		return std::hash<uint64_t>()(k.table_id) ^ (std::hash<uint64_t>()(k.schema_version) << 1);
	}
};
static std::unordered_map<TableSchemaKey, InlinedColumnTypesCache, TableSchemaKeyHash> inlined_col_types_cache;

void
ResetDirectInsertCaches() {
	inlining_info_cache.clear();
	for (auto &entry : inlined_col_types_cache) {
		list_free(entry.second.col_types);
	}
	inlined_col_types_cache.clear();
}

enum DirectInsertMode {
	DIRECT_INSERT_UNNEST = 0,
	DIRECT_INSERT_VALUES = 1,
};

struct DirectInsertScanState {
	CustomScanState css; // Must be first

	DirectInsertMode mode;
	Oid target_table_oid;
	uint64_t table_id;
	uint64_t schema_version;
	char *inlined_table_name;
	List *column_names; // List of String nodes
	List *column_types; // List of Oid (inlined table types)

	/* UNNEST-specific */
	List *param_ids;     // List of int
	List *element_types; // List of Oid
	int expected_row_count;
	ParamListInfo bound_params;
	ArrayType **unnest_arrays;
	int *unnest_array_lengths;

	/* VALUES-specific */
	int values_num_rows;
	int values_num_cols;
	Node **values_exprs;        // flat [row * num_cols + col] expression nodes
	ExprState **values_estates; // flat [row * num_cols + col]; built in Begin
	Oid *values_src_types;      // per-column source OID

	bool finished;
	int64_t rows_inserted;

	uint64_t begin_snapshot;
	uint64_t next_row_id;
};

static Node *DirectInsert_CreateCustomScanState(CustomScan *cscan);
static void DirectInsert_BeginCustomScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *DirectInsert_ExecCustomScan(CustomScanState *node);
static void DirectInsert_EndCustomScan(CustomScanState *node);
static void DirectInsert_ReScanCustomScan(CustomScanState *node);
static void DirectInsert_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es);

static CustomExecMethods direct_insert_exec_methods = {
    .CustomName = "DuckLakeDirectInsert",
    .BeginCustomScan = DirectInsert_BeginCustomScan,
    .ExecCustomScan = DirectInsert_ExecCustomScan,
    .EndCustomScan = DirectInsert_EndCustomScan,
    .ReScanCustomScan = DirectInsert_ReScanCustomScan,
    .MarkPosCustomScan = NULL,
    .RestrPosCustomScan = NULL,
    .EstimateDSMCustomScan = NULL,
    .InitializeDSMCustomScan = NULL,
    .ReInitializeDSMCustomScan = NULL,
    .InitializeWorkerCustomScan = NULL,
    .ShutdownCustomScan = NULL,
    .ExplainCustomScan = DirectInsert_ExplainCustomScan,
};

static CustomScanMethods direct_insert_scan_methods = {
    .CustomName = "DuckLakeDirectInsert",
    .CreateCustomScanState = DirectInsert_CreateCustomScanState,
};

struct ValuesInsertContext {
	Oid target_table_oid;
	uint64_t table_id;
	uint64_t schema_version;
	int num_rows;
	int num_cols;
	List *target_col_names;  // List of char*
	List *inlined_col_types; // List of Oid
	List *src_col_types;     // List of Oid (user-facing PG types)
	/* One expression per cell, always non-NULL: unspecified columns get a
	 * typed NULL Const; const-foldable subexpressions are already collapsed
	 * by eval_const_expressions. */
	Node **exprs;
};

struct InsertPreconditionResult {
	Oid target_oid;
	uint64_t table_id;
	uint64_t schema_version;
	int64_t row_limit;
	Relation target_rel; // caller must close
};

static bool CheckInsertPreconditions(Query *parse, InsertPreconditionResult *result_out, DirectInsertReason *reason_out,
                                     bool *is_ducklake_out);
static bool TryMatchUnnest(Query *parse, const InsertPreconditionResult *precond, DirectInsertContext *context_out,
                           DirectInsertReason *reason_out);
static bool TryMatchValues(Query *parse, const InsertPreconditionResult *precond, ValuesInsertContext *context_out,
                           DirectInsertReason *reason_out);
static bool IsBuiltinUnnest(FuncExpr *func);
static bool IsPlainUnnestSource(Query *query);
static bool IsUnnestOfParam(Node *node, int *param_id_out, Oid *param_type_out);
static void BindUnnestParameters(DirectInsertScanState *state);
static PlannedStmt *CreateDirectInsertPlan(Query *parse, DirectInsertContext *context);
static PlannedStmt *CreateValuesInsertPlan(Query *parse, ValuesInsertContext *context);
static NativeInlineColumnStat *DirectInsertIntoInlinedTable(DirectInsertScanState *state, NativeInlineWriteBatch *batch,
                                                            uint64_t *stats_count);
static NativeInlineColumnStat *DirectInsertValuesIntoInlinedTable(DirectInsertScanState *state,
                                                                  NativeInlineWriteBatch *batch, uint64_t *stats_count);
static void CheckQueryPermissionsAndRls(Query *query, bool skip_result_relation);

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

/*
 * Map a DuckDB type string to the PG OID used in the inlined data table;
 * mirrors PostgresMetadataManager::GetColumnTypeInternal.  InvalidOid =
 * not handled by the direct insert path.
 */
static Oid
DuckDBTypeToInlinedOid(const char *duckdb_type, Oid element_type) {
	// Nested types are stored as VARCHAR and cannot appear in UNNEST($param).
	// DuckDB ToString() uses mixed case: "STRUCT(...)", "MAP(...)", "INTEGER[]".
	if (pg_strncasecmp(duckdb_type, "STRUCT", 6) == 0 || pg_strncasecmp(duckdb_type, "MAP", 3) == 0 ||
	    strchr(duckdb_type, '[') != NULL) {
		return InvalidOid;
	}

	// VARIANT and GEOMETRY do not support inlining at all
	if (pg_strcasecmp(duckdb_type, "VARIANT") == 0 || pg_strcasecmp(duckdb_type, "GEOMETRY") == 0) {
		return InvalidOid;
	}

	// VARCHAR and BLOB are stored as BYTEA
	if (pg_strcasecmp(duckdb_type, "VARCHAR") == 0 || pg_strcasecmp(duckdb_type, "BLOB") == 0) {
		return BYTEAOID;
	}

	// Scalar types with wider DuckDB range are stored as VARCHAR.  TIMESTAMPTZ
	// is excluded because timestamptz_out crashes in the VALUES path.
	if (pg_strcasecmp(duckdb_type, "TIMESTAMP WITH TIME ZONE") == 0 || pg_strcasecmp(duckdb_type, "TIMESTAMPTZ") == 0) {
		return InvalidOid;
	}
	if (pg_strcasecmp(duckdb_type, "UBIGINT") == 0 || pg_strcasecmp(duckdb_type, "HUGEINT") == 0 ||
	    pg_strcasecmp(duckdb_type, "UHUGEINT") == 0 || pg_strcasecmp(duckdb_type, "DATE") == 0 ||
	    pg_strcasecmp(duckdb_type, "TIMESTAMP") == 0 || pg_strcasecmp(duckdb_type, "TIMESTAMP_S") == 0 ||
	    pg_strcasecmp(duckdb_type, "TIMESTAMP_MS") == 0 || pg_strcasecmp(duckdb_type, "TIMESTAMP_NS") == 0) {
		return VARCHAROID;
	}

	// Natively supported -- PG element type matches the inlined column type
	return element_type;
}

/*
 * Determine the inlined-table PG type for each user column from
 * ducklake_column metadata.  element_types is the user-facing PG type per
 * column (List of Oid).  Returns false on bail-out.
 */
static bool
GetInlinedColumnTypes(uint64_t table_id, List *element_types, List **inlined_col_types_out) {
	int ret;
	int num_cols = list_length(element_types);

	// Allocate in the caller's memory context -- SPI_connect switches to a
	// private context that is freed by SPI_finish, so List nodes built inside
	// SPI would be freed too.
	Oid *oids = (Oid *)palloc(sizeof(Oid) * num_cols);

	if ((ret = SPI_connect()) < 0) {
		return false;
	}

	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query, R"(
SELECT column_type
FROM ducklake.ducklake_column
WHERE table_id = %llu
AND end_snapshot IS NULL
AND parent_column IS NULL
ORDER BY column_order)",
	                 (unsigned long long)table_id);

	ret = SPI_execute(query.data, true, 0);
	if (ret != SPI_OK_SELECT) {
		SPI_finish();
		return false;
	}

	if ((int)SPI_processed != num_cols) {
		SPI_finish();
		return false;
	}

	ListCell *lc = list_head(element_types);
	for (int i = 0; i < num_cols; i++) {
		bool isnull;
		Datum type_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull) {
			SPI_finish();
			return false;
		}
		char *duckdb_type = TextDatumGetCString(type_datum);

		Oid element_type = lfirst_oid(lc);
		Oid inlined_oid = DuckDBTypeToInlinedOid(duckdb_type, element_type);
		pfree(duckdb_type);

		if (!OidIsValid(inlined_oid)) {
			SPI_finish();
			return false;
		}
		oids[i] = inlined_oid;
		lc = lnext(element_types, lc);
	}

	SPI_finish();

	List *result = NIL;
	for (int i = 0; i < num_cols; i++) {
		result = lappend_oid(result, oids[i]);
	}
	pfree(oids);

	*inlined_col_types_out = result;
	return true;
}

void
RegisterDirectInsertNode() {
	RegisterCustomScanMethods(&direct_insert_scan_methods);
}

static bool
CheckNestedQueryWalker(Node *node, void *unused) {
	if (node == NULL)
		return false;

	if (IsA(node, Query)) {
		CheckQueryPermissionsAndRls(castNode(Query, node), false);
		return false;
	}

#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, CheckNestedQueryWalker, unused);
#else
	return expression_tree_walker(node, (bool (*)())((void *)CheckNestedQueryWalker), unused);
#endif
}

static void
CheckQueryPermissionsAndRls(Query *query, bool skip_result_relation) {
	/* Permission metadata is local to each Query level until the standard
	 * planner flattens it. DuckDB bypasses that planner, so check every level. */
#if PG_VERSION_NUM >= 160000
	ExecCheckPermissions(query->rtable, query->rteperminfos, true);
#else
	ExecCheckRTPerms(query->rtable, true);
#endif

	int rtindex = 0;
	ListCell *lc;
	foreach (lc, query->rtable) {
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
		rtindex++;
		if (rte->rtekind != RTE_RELATION || (skip_result_relation && rtindex == query->resultRelation))
			continue;

		Relation rel = relation_open(rte->relid, AccessShareLock);
		bool has_rls = rel->rd_rel->relrowsecurity;
		relation_close(rel, AccessShareLock);
		if (has_rls)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			                errmsg("row-level security is not supported for relations read by DuckLake INSERT")));
	}

#if PG_VERSION_NUM >= 160000
	query_tree_walker(query, CheckNestedQueryWalker, NULL, 0);
#else
	query_tree_walker(query, (bool (*)())((void *)CheckNestedQueryWalker), NULL, 0);
#endif
}

bool
CheckDucklakeInsertSafety(Query *parse) {
	if (parse->commandType != CMD_INSERT || parse->resultRelation == 0 ||
	    list_length(parse->rtable) < parse->resultRelation) {
		return false;
	}

	RangeTblEntry *target_rte = (RangeTblEntry *)list_nth(parse->rtable, parse->resultRelation - 1);
	if (target_rte->rtekind != RTE_RELATION) {
		return false;
	}

	static Oid ducklake_am_oid = InvalidOid;
	if (!OidIsValid(ducklake_am_oid))
		ducklake_am_oid = get_am_oid("ducklake", true);
	if (!OidIsValid(ducklake_am_oid))
		return false;

	Relation target_rel = relation_open(target_rte->relid, AccessShareLock);
	if (target_rel->rd_rel->relam != ducklake_am_oid) {
		relation_close(target_rel, AccessShareLock);
		return false;
	}

	/* DuckDB plans discard PostgreSQL's permission metadata, while the native
	 * plan bypasses ModifyTable. Repeat from ExecutorStart for cached plans. */
	CheckQueryPermissionsAndRls(parse, true);

	if (target_rel->trigdesc != NULL || target_rel->rd_rules != NULL || target_rel->rd_rel->relrowsecurity) {
		relation_close(target_rel, AccessShareLock);
		ereport(ERROR,
		        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		         errmsg("PostgreSQL triggers, rules, and row-level security are not supported for DuckLake INSERT")));
	}

	relation_close(target_rel, AccessShareLock);
	return true;
}

static bool
ContainsExternalParam(Node *node, void *unused) {
	if (node == NULL)
		return false;
	if (IsA(node, Param) && castNode(Param, node)->paramkind == PARAM_EXTERN)
		return true;

#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, ContainsExternalParam, unused);
#else
	return expression_tree_walker(node, (bool (*)())((void *)ContainsExternalParam), unused);
#endif
}

static bool
ParameterizedUnnestWalker(Node *node, void *unused) {
	if (node == NULL)
		return false;

	if (IsA(node, FuncExpr)) {
		FuncExpr *func = castNode(FuncExpr, node);
		if (IsBuiltinUnnest(func) && ContainsExternalParam((Node *)func->args, NULL))
			return true;
	}
	if (IsA(node, Query)) {
#if PG_VERSION_NUM >= 160000
		return query_tree_walker(castNode(Query, node), ParameterizedUnnestWalker, unused, 0);
#else
		return query_tree_walker(castNode(Query, node), (bool (*)())((void *)ParameterizedUnnestWalker), unused, 0);
#endif
	}

#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, ParameterizedUnnestWalker, unused);
#else
	return expression_tree_walker(node, (bool (*)())((void *)ParameterizedUnnestWalker), unused);
#endif
}

void
RejectParameterizedUnnestFallback(Query *parse) {
	if (!ParameterizedUnnestWalker((Node *)parse, NULL))
		return;

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	                errmsg("parameterized UNNEST is not supported by the DuckDB fallback for DuckLake INSERT")));
}

/*
 * is_ducklake_out separates uncounted gating failures from counted
 * "ducklake but not inlineable" rejections.  On success,
 * result_out->target_rel is open with AccessShareLock; caller must close it.
 */
static bool
CheckInsertPreconditions(Query *parse, InsertPreconditionResult *result_out, DirectInsertReason *reason_out,
                         bool *is_ducklake_out) {
	*is_ducklake_out = false;
	*reason_out = DI_R_OK;

	if (parse->commandType != CMD_INSERT) {
		return false; /* gating */
	}

	if (IsTransactionBlock() || IsolationUsesXactSnapshot()) {
		return false; /* native writer requires one READ COMMITTED statement */
	}

	if (parse->resultRelation == 0 || list_length(parse->rtable) < parse->resultRelation) {
		return false; /* gating: bad INSERT shape, can't identify target */
	}

	RangeTblEntry *target_rte = (RangeTblEntry *)list_nth(parse->rtable, parse->resultRelation - 1);
	if (target_rte->rtekind != RTE_RELATION) {
		return false; /* gating: target isn't a relation */
	}

	Oid target_oid = target_rte->relid;

	static Oid ducklake_am_oid = InvalidOid;
	if (!OidIsValid(ducklake_am_oid))
		ducklake_am_oid = get_am_oid("ducklake", true);
	if (!OidIsValid(ducklake_am_oid))
		return false; /* gating: extension not fully initialized */

	Relation target_rel = relation_open(target_oid, AccessShareLock);
	Oid am_oid = target_rel->rd_rel->relam;

	if (am_oid != ducklake_am_oid) {
		relation_close(target_rel, AccessShareLock);
		return false; /* gating: not a ducklake table */
	}

	/* Past this point target IS a ducklake table -- any further failure
	 * is a counted "unmatched" outcome. */
	*is_ducklake_out = true;

	TupleDesc target_desc = RelationGetDescr(target_rel);
	TupleConstr *constr = target_desc->constr;
	bool unsupported_relation =
	    target_rel->rd_rel->relkind != RELKIND_RELATION ||
	    (constr && (constr->has_not_null || constr->num_check > 0 || constr->has_generated_stored));
#if PG_VERSION_NUM >= 180000
	unsupported_relation = unsupported_relation || (constr && constr->has_generated_virtual);
#endif
	for (int i = 0; !unsupported_relation && i < target_desc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(target_desc, i);
		unsupported_relation = attr->attisdropped || attr->attgenerated != '\0' || attr->attidentity != '\0';
	}
	if (unsupported_relation || parse->returningList != NIL || parse->onConflict != NULL || parse->cteList != NIL ||
	    parse->hasModifyingCTE || parse->withCheckOptions != NIL || parse->override != OVERRIDING_NOT_SET ||
	    target_rte->inh) {
		relation_close(target_rel, AccessShareLock);
		*reason_out = DI_R_UNSUPPORTED_INSERT_SHAPE;
		return false;
	}

	uint64_t table_id = 0;
	uint64_t schema_version = 0;
	int64_t row_limit = 0;

	auto cache_it = inlining_info_cache.find(target_oid);
	if (cache_it != inlining_info_cache.end()) {
		table_id = cache_it->second.table_id;
		schema_version = cache_it->second.schema_version;
		row_limit = cache_it->second.row_limit;
	} else {
		TableInliningState state =
		    pgducklake::GetTableInliningState(target_oid, &table_id, &schema_version, &row_limit);
		if (state != TI_OK) {
			relation_close(target_rel, AccessShareLock);
			switch (state) {
			case TI_SCHEMA_VERSION_MISMATCH:
				*reason_out = DI_R_SCHEMA_VERSION_MISMATCH;
				break;
			case TI_NO_TABLE:
			case TI_NO_INLINED_TABLE:
			default:
				*reason_out = DI_R_NO_INLINED_TABLE;
				break;
			}
			return false;
		}
		inlining_info_cache[target_oid] = {table_id, schema_version, row_limit};
	}

	result_out->target_oid = target_oid;
	result_out->table_id = table_id;
	result_out->schema_version = schema_version;
	result_out->row_limit = row_limit;
	result_out->target_rel = target_rel;
	return true;
}

/*
 * Cached wrapper around GetInlinedColumnTypes; the cached list lives in
 * TopMemoryContext -- caller must not free it.
 */
static bool
GetCachedInlinedColumnTypes(uint64_t table_id, uint64_t schema_version, List *element_types,
                            List **inlined_col_types_out) {
	TableSchemaKey col_key = {table_id, schema_version};
	auto col_cache_it = inlined_col_types_cache.find(col_key);
	if (col_cache_it != inlined_col_types_cache.end()) {
		*inlined_col_types_out = col_cache_it->second.col_types;
		return true;
	}

	List *inlined_col_types = NIL;
	if (!GetInlinedColumnTypes(table_id, element_types, &inlined_col_types)) {
		return false;
	}

	MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);
	List *persistent = NIL;
	ListCell *lc_oid;
	foreach (lc_oid, inlined_col_types) {
		persistent = lappend_oid(persistent, lfirst_oid(lc_oid));
	}
	MemoryContextSwitchTo(old_ctx);
	inlined_col_types_cache[col_key] = {persistent};
	*inlined_col_types_out = persistent;
	return true;
}

/* Higher value = more informative / more specific. Used when both
 * detectors fail so we can surface the most useful reason. */
static int
ReasonRank(DirectInsertReason r) {
	switch (r) {
	case DI_R_COL_TYPES_UNSUPPORTED:
		return 4;
	case DI_R_GREATER_THAN_LIMIT:
		return 3;
	case DI_R_INVALID_RTE:
		return 2;
	case DI_R_UNSUPPORTED_INSERT_SHAPE:
		return 1;
	default:
		return 0;
	}
}

static DirectInsertReason
PickMoreSpecific(DirectInsertReason a, DirectInsertReason b) {
	return ReasonRank(a) >= ReasonRank(b) ? a : b;
}

PlannedStmt *
TryCreateDirectInsertPlan(Query *parse) {
	/* Gating: these outcomes are NOT counted in the stats. */
	if (parse->commandType != CMD_INSERT)
		return nullptr;
	if (IsTransactionBlock() || IsolationUsesXactSnapshot())
		return nullptr;
	if (!pgducklake::enable_direct_insert)
		return nullptr;

	bool is_ducklake = false;
	DirectInsertReason precond_reason = DI_R_OK;
	InsertPreconditionResult precond = {};
	if (!CheckInsertPreconditions(parse, &precond, &precond_reason, &is_ducklake)) {
		if (!is_ducklake)
			return nullptr; /* gating: non-ducklake target */
		DirectInsertStatsBump(DI_PAT_UNMATCHED, precond_reason);
		return nullptr;
	}

	/* precond.target_rel is now open; must close on every remaining path. */

	DirectInsertContext context = {};
	DirectInsertReason unnest_reason = DI_R_UNSUPPORTED_INSERT_SHAPE;
	if (TryMatchUnnest(parse, &precond, &context, &unnest_reason)) {
		relation_close(precond.target_rel, AccessShareLock);
		ereport(DEBUG1, (errmsg("DuckLake direct insert: optimization enabled for "
		                        "INSERT UNNEST pattern, table_id=%lu",
		                        (unsigned long)context.table_id)));
		DirectInsertStatsBump(DI_PAT_MATCHED_UNNEST, DI_R_OK);
		return CreateDirectInsertPlan(parse, &context);
	}

	ValuesInsertContext values_ctx = {};
	DirectInsertReason values_reason = DI_R_UNSUPPORTED_INSERT_SHAPE;
	if (TryMatchValues(parse, &precond, &values_ctx, &values_reason)) {
		relation_close(precond.target_rel, AccessShareLock);
		ereport(DEBUG1, (errmsg("DuckLake direct insert: optimization enabled for "
		                        "INSERT VALUES pattern, "
		                        "table_id=%lu, rows=%d",
		                        (unsigned long)values_ctx.table_id, values_ctx.num_rows)));
		DirectInsertStatsBump(DI_PAT_MATCHED_VALUES, DI_R_OK);
		return CreateValuesInsertPlan(parse, &values_ctx);
	}

	relation_close(precond.target_rel, AccessShareLock);
	DirectInsertStatsBump(DI_PAT_UNMATCHED, PickMoreSpecific(unnest_reason, values_reason));
	return nullptr;
}

/*
 * Caller opens precond->target_rel and closes it after this call
 * returns (on both success and failure paths).  This function must not
 * release the lock.
 */
static bool
TryMatchUnnest(Query *parse, const InsertPreconditionResult *precond, DirectInsertContext *context_out,
               DirectInsertReason *reason_out) {
	*reason_out = DI_R_UNSUPPORTED_INSERT_SHAPE;

	Relation target_rel = precond->target_rel;

	if (!parse->jointree || parse->jointree->quals != NULL || list_length(parse->jointree->fromlist) != 1) {
		return false;
	}

	Node *from_node = (Node *)linitial(parse->jointree->fromlist);
	if (!IsA(from_node, RangeTblRef)) {
		return false;
	}

	int from_rtindex = ((RangeTblRef *)from_node)->rtindex;
	if (from_rtindex < 1 || from_rtindex > list_length(parse->rtable)) {
		*reason_out = DI_R_INVALID_RTE;
		return false;
	}

	RangeTblEntry *from_rte = (RangeTblEntry *)list_nth(parse->rtable, from_rtindex - 1);
	if (from_rte->rtekind != RTE_SUBQUERY) {
		*reason_out = DI_R_INVALID_RTE;
		return false;
	}
	Query *subquery = from_rte->subquery;
	if (!IsPlainUnnestSource(subquery)) {
		return false;
	}

	TupleDesc tupdesc = RelationGetDescr(target_rel);
	int live_natts = 0;
	for (int i = 0; i < tupdesc->natts; i++) {
		if (!TupleDescAttr(tupdesc, i)->attisdropped) {
			live_natts++;
		}
	}
	ParamInfo **params_by_column = (ParamInfo **)palloc0(sizeof(ParamInfo *) * live_natts);

	ListCell *lc;
	foreach (lc, parse->targetList) {
		TargetEntry *target_tle = (TargetEntry *)lfirst(lc);
		if (target_tle->resjunk) {
			continue;
		}
		if (target_tle->resno < 1 || target_tle->resno > tupdesc->natts) {
			return false;
		}

		Var *source_var = IsA(target_tle->expr, Var) ? (Var *)target_tle->expr : NULL;
		if (!source_var || source_var->varno != from_rtindex || source_var->varattno < 1) {
			return false;
		}
		TargetEntry *source_tle = NULL;
		ListCell *source_lc;
		foreach (source_lc, subquery->targetList) {
			TargetEntry *candidate = (TargetEntry *)lfirst(source_lc);
			if (!candidate->resjunk && candidate->resno == source_var->varattno) {
				source_tle = candidate;
				break;
			}
		}
		if (!source_tle) {
			return false;
		}

		int param_id;
		Oid param_type;
		if (!IsUnnestOfParam((Node *)source_tle->expr, &param_id, &param_type)) {
			return false;
		}
		Oid element_type = get_element_type(param_type);
		if (!OidIsValid(element_type)) {
			return false;
		}

		int live_column = 0;
		for (int i = 0; i < target_tle->resno - 1; i++) {
			if (!TupleDescAttr(tupdesc, i)->attisdropped) {
				live_column++;
			}
		}
		Form_pg_attribute attr = TupleDescAttr(tupdesc, target_tle->resno - 1);
		if (attr->attisdropped || params_by_column[live_column]) {
			return false;
		}

		ParamInfo *pinfo = (ParamInfo *)palloc(sizeof(ParamInfo));
		pinfo->param_id = param_id;
		pinfo->param_type = param_type;
		pinfo->element_type = element_type;
		params_by_column[live_column] = pinfo;
	}

	List *param_infos = NIL;
	List *target_col_names = NIL;
	for (int i = 0, live_column = 0; i < tupdesc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (attr->attisdropped) {
			continue;
		}
		if (!params_by_column[live_column]) {
			return false;
		}
		param_infos = lappend(param_infos, params_by_column[live_column]);
		target_col_names = lappend(target_col_names, pstrdup(NameStr(attr->attname)));
		live_column++;
	}
	pfree(params_by_column);

	List *element_types = NIL;
	foreach (lc, param_infos) {
		ParamInfo *pinfo = (ParamInfo *)lfirst(lc);
		element_types = lappend_oid(element_types, pinfo->element_type);
	}

	List *inlined_col_types = NIL;
	if (!GetCachedInlinedColumnTypes(precond->table_id, precond->schema_version, element_types, &inlined_col_types)) {
		*reason_out = DI_R_COL_TYPES_UNSUPPORTED;
		return false;
	}

	context_out->target_table_oid = precond->target_oid;
	context_out->table_id = precond->table_id;
	context_out->schema_version = precond->schema_version;
	context_out->param_infos = param_infos;
	context_out->target_col_names = target_col_names;
	context_out->target_col_types = inlined_col_types;

	*reason_out = DI_R_OK;
	return true;
}

static bool
IsBuiltinUnnest(FuncExpr *func) {
	if (func->funcid != F_UNNEST_ANYARRAY || !func->funcretset || list_length(func->args) != 1) {
		return false;
	}

	Oid array_type = exprType((Node *)linitial(func->args));
	Oid element_type = get_element_type(array_type);
	return OidIsValid(element_type) && func->funcresulttype == element_type;
}

/* The native writer implements only a bare SELECT list of built-in UNNEST
 * calls. Any other source-query clause can filter, duplicate, regroup, or
 * otherwise change when and how often those calls are evaluated. */
static bool
IsPlainUnnestSource(Query *query) {
	if (!query || query->commandType != CMD_SELECT || query->resultRelation != 0 || query->utilityStmt != NULL ||
	    query->hasAggs || query->hasWindowFuncs || !query->hasTargetSRFs || query->hasSubLinks ||
	    query->hasDistinctOn || query->hasRecursive || query->hasModifyingCTE || query->hasForUpdate ||
	    query->hasRowSecurity || query->isReturn || query->cteList != NIL || query->rtable != NIL || !query->jointree ||
	    query->jointree->fromlist != NIL || query->jointree->quals != NULL || query->override != OVERRIDING_NOT_SET ||
	    query->onConflict != NULL || query->returningList != NIL || query->groupClause != NIL || query->groupDistinct ||
	    query->groupingSets != NIL || query->havingQual != NULL || query->windowClause != NIL ||
	    query->distinctClause != NIL || query->sortClause != NIL || query->limitOffset != NULL ||
	    query->limitCount != NULL || query->rowMarks != NIL || query->setOperations != NULL ||
	    query->withCheckOptions != NIL || query->targetList == NIL) {
		return false;
	}

	ListCell *lc;
	foreach (lc, query->targetList) {
		if (((TargetEntry *)lfirst(lc))->resjunk) {
			return false;
		}
	}
	return true;
}

static bool
IsUnnestOfParam(Node *node, int *param_id_out, Oid *param_type_out) {
	if (!IsA(node, FuncExpr)) {
		return false;
	}

	FuncExpr *funcexpr = (FuncExpr *)node;
	if (!IsBuiltinUnnest(funcexpr)) {
		return false;
	}

	Node *arg = (Node *)linitial(funcexpr->args);
	if (!IsA(arg, Param)) {
		return false;
	}

	Param *param = (Param *)arg;
	if (param->paramkind != PARAM_EXTERN) {
		return false;
	}

	*param_id_out = param->paramid;
	*param_type_out = param->paramtype;
	return true;
}

static void
BindUnnestParameters(DirectInsertScanState *state) {
	int num_params = list_length(state->param_ids);
	state->unnest_arrays = (ArrayType **)palloc0(sizeof(ArrayType *) * num_params);
	state->unnest_array_lengths = (int *)palloc0(sizeof(int) * num_params);
	state->expected_row_count = 0;

	ListCell *id_lc = list_head(state->param_ids);
	ListCell *type_lc = list_head(state->element_types);
	for (int i = 0; i < num_params; i++) {
		int param_id = lfirst_int(id_lc);
		Oid expected_element_type = lfirst_oid(type_lc);
		id_lc = lnext(state->param_ids, id_lc);
		type_lc = lnext(state->element_types, type_lc);

		if (param_id < 1 || param_id > state->bound_params->numParams) {
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PARAMETER),
			                errmsg("direct UNNEST parameter $%d is not bound", param_id)));
		}

		ParamExternData workspace;
		ParamExternData *pdata = state->bound_params->paramFetch
		                             ? state->bound_params->paramFetch(state->bound_params, param_id, false, &workspace)
		                             : &state->bound_params->params[param_id - 1];
		if (!pdata || !OidIsValid(pdata->ptype)) {
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PARAMETER),
			                errmsg("direct UNNEST parameter $%d is not bound", param_id)));
		}
		if (get_element_type(pdata->ptype) != expected_element_type) {
			ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
			                errmsg("direct UNNEST parameter $%d has unexpected type", param_id)));
		}
		if (pdata->isnull) {
			continue;
		}

		ArrayType *array = DatumGetArrayTypeP(pdata->value);
		if (ARR_ELEMTYPE(array) != expected_element_type) {
			ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
			                errmsg("direct UNNEST parameter $%d has unexpected type", param_id)));
		}
		int length = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
		state->unnest_arrays[i] = array;
		state->unnest_array_lengths[i] = length;
		state->expected_row_count = Max(state->expected_row_count, length);
	}
}

/*
 * Reject expressions that need a tuple stream, outer query, or aggregate
 * context; also VOLATILE functions, so direct insert stays observably
 * equivalent to the DuckDB path.
 */
static bool
ValuesExprUnsafeWalker(Node *node, void *unused) {
	if (node == NULL)
		return false;

	switch (nodeTag(node)) {
	case T_Var:
	case T_Param:
	case T_Aggref:
	case T_GroupingFunc:
	case T_WindowFunc:
	case T_SubLink:
	case T_SubPlan:
	case T_AlternativeSubPlan:
	case T_CurrentOfExpr:
		return true;
	default:
		break;
	}

	if (IsA(node, FuncExpr)) {
		FuncExpr *fe = (FuncExpr *)node;
		if (func_volatile(fe->funcid) == PROVOLATILE_VOLATILE)
			return true;
	} else if (IsA(node, OpExpr) || IsA(node, DistinctExpr) || IsA(node, NullIfExpr)) {
		/* DistinctExpr and NullIfExpr share OpExpr's struct prefix;
		 * an unresolved opfuncid is treated as volatile. */
		OpExpr *oe = (OpExpr *)node;
		if (!OidIsValid(oe->opfuncid))
			return true;
		if (func_volatile(oe->opfuncid) == PROVOLATILE_VOLATILE)
			return true;
	}

#if PG_VERSION_NUM >= 160000
	return expression_tree_walker(node, ValuesExprUnsafeWalker, unused);
#else
	return expression_tree_walker(node, (bool (*)())((void *)ValuesExprUnsafeWalker), unused);
#endif
}

static bool
IsAcceptableValuesExpr(Node *expr) {
	return !ValuesExprUnsafeWalker(expr, NULL);
}

/*
 * Caller opens precond->target_rel and closes it after this call
 * returns.  This function does not touch the relation lock.
 */
static bool
TryMatchValues(Query *parse, const InsertPreconditionResult *precond, ValuesInsertContext *context_out,
               DirectInsertReason *reason_out) {
	*reason_out = DI_R_UNSUPPORTED_INSERT_SHAPE;

	Relation target_rel = precond->target_rel;

	/* Find the VALUES source.  PG 18 creates an RTE_VALUES entry for
	 * multi-row VALUES but inlines single-row VALUES directly into the
	 * targetList.  Handle both cases. */
	RangeTblEntry *values_rte = NULL;
	int values_rte_index = 0;
	{
		ListCell *rtlc;
		int idx = 1;
		foreach (rtlc, parse->rtable) {
			RangeTblEntry *rte = (RangeTblEntry *)lfirst(rtlc);
			if (rte->rtekind == RTE_VALUES) {
				values_rte = rte;
				values_rte_index = idx;
				break;
			}
			idx++;
		}
	}

	/* Require a pure VALUES shape: a WHERE clause or a FROM entry other
	 * than the values_rte means INSERT ... SELECT, which can produce a
	 * different row count than the targetList suggests. */
	if (parse->jointree && parse->jointree->quals != NULL) {
		return false;
	}
	{
		List *fl = parse->jointree ? parse->jointree->fromlist : NIL;
		if (values_rte == NULL) {
			if (fl != NIL)
				return false;
		} else {
			if (list_length(fl) != 1)
				return false;
			Node *fn = (Node *)linitial(fl);
			if (!IsA(fn, RangeTblRef) || ((RangeTblRef *)fn)->rtindex != values_rte_index)
				return false;
		}
	}

	List *values_lists = NIL;

	if (values_rte) {
		/* Multi-row VALUES: expressions in values_rte->values_lists */
		values_lists = values_rte->values_lists;
	} else {
		/* Single-row VALUES: expressions are in parse->targetList directly.
		 * Build a synthetic single-row values_lists from targetList exprs. */
		if (!parse->targetList) {
			return false;
		}

		List *row = NIL;
		ListCell *tlc;
		foreach (tlc, parse->targetList) {
			TargetEntry *tle = (TargetEntry *)lfirst(tlc);
			if (tle->resjunk) {
				continue;
			}
			row = lappend(row, tle->expr);
		}
		values_lists = list_make1(row);
	}
	int num_rows = list_length(values_lists);
	if (num_rows == 0) {
		return false;
	}

	/* Batch size check -- skip direct insert when the INSERT would
	 * overflow data_inlining_row_limit in one shot. */
	if (precond->row_limit > 0 && (int64_t)num_rows > precond->row_limit) {
		*reason_out = DI_R_GREATER_THAN_LIMIT;
		return false;
	}

	TupleDesc tupdesc = RelationGetDescr(target_rel);
	int num_table_cols = tupdesc->natts;

	/* Per table column: values_col_idx[col] >= 0 selects the per-row VALUES
	 * cell (a targetList Var pointing at the RTE_VALUES); target_expr[col]
	 * is a row-independent default/const; neither set -> typed NULL filler. */
	int *values_col_idx = (int *)palloc(sizeof(int) * num_table_cols);
	Node **target_expr = (Node **)palloc0(sizeof(Node *) * num_table_cols);
	for (int i = 0; i < num_table_cols; i++)
		values_col_idx[i] = -1;

	int values_rte_varno = 0;
	if (values_rte) {
		int idx = 1;
		ListCell *rtlc;
		foreach (rtlc, parse->rtable) {
			if (lfirst(rtlc) == values_rte) {
				values_rte_varno = idx;
				break;
			}
			idx++;
		}
	}

	int single_row_seq = 0;
	ListCell *lc;
	foreach (lc, parse->targetList) {
		TargetEntry *tle = (TargetEntry *)lfirst(lc);
		if (tle->resjunk)
			continue;
		if (tle->resno < 1 || tle->resno > num_table_cols) {
			pfree(values_col_idx);
			pfree(target_expr);
			return false;
		}
		int col = tle->resno - 1;
		if (values_rte) {
			Var *v = IsA(tle->expr, Var) ? (Var *)tle->expr : NULL;
			if (v && v->varno == values_rte_varno) {
				values_col_idx[col] = v->varattno - 1;
			} else {
				target_expr[col] = (Node *)tle->expr;
			}
		} else {
			values_col_idx[col] = single_row_seq++;
		}
	}

	/* Resolve every cell to a Node*.  eval_const_expressions folds what
	 * it can (IMMUTABLE only); everything else flows through to
	 * ExecInitExpr at executor start. */
	Node **exprs = (Node **)palloc(sizeof(Node *) * num_rows * num_table_cols);

	int row_idx = 0;
	foreach (lc, values_lists) {
		List *row_exprs = (List *)lfirst(lc);

		for (int col = 0; col < num_table_cols; col++) {
			int flat = row_idx * num_table_cols + col;
			Node *expr = NULL;
			if (values_col_idx[col] >= 0) {
				if (values_col_idx[col] >= list_length(row_exprs)) {
					pfree(values_col_idx);
					pfree(target_expr);
					pfree(exprs);
					return false;
				}
				expr = (Node *)list_nth(row_exprs, values_col_idx[col]);
			} else if (target_expr[col]) {
				expr = target_expr[col];
			}
			if (expr == NULL) {
				/* Unspecified column with no default: typed NULL Const. */
				Form_pg_attribute attr = TupleDescAttr(tupdesc, col);
				expr = (Node *)makeConst(attr->atttypid, attr->atttypmod, attr->attcollation, attr->attlen, (Datum)0,
				                         true /* isnull */, attr->attbyval);
			} else {
				expr = eval_const_expressions(NULL, expr);
				if (!IsAcceptableValuesExpr(expr)) {
					pfree(values_col_idx);
					pfree(target_expr);
					pfree(exprs);
					return false;
				}
			}
			exprs[flat] = expr;
		}
		row_idx++;
	}

	pfree(values_col_idx);
	pfree(target_expr);

	List *src_col_types = NIL;
	List *target_col_names = NIL;
	for (int i = 0; i < num_table_cols; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		src_col_types = lappend_oid(src_col_types, attr->atttypid);
		target_col_names = lappend(target_col_names, pstrdup(NameStr(attr->attname)));
	}

	List *inlined_col_types = NIL;
	if (!GetCachedInlinedColumnTypes(precond->table_id, precond->schema_version, src_col_types, &inlined_col_types)) {
		pfree(exprs);
		*reason_out = DI_R_COL_TYPES_UNSUPPORTED;
		return false;
	}

	context_out->target_table_oid = precond->target_oid;
	context_out->table_id = precond->table_id;
	context_out->schema_version = precond->schema_version;
	context_out->num_rows = num_rows;
	context_out->num_cols = num_table_cols;
	context_out->target_col_names = target_col_names;
	context_out->inlined_col_types = inlined_col_types;
	context_out->src_col_types = src_col_types;
	context_out->exprs = exprs;

	*reason_out = DI_R_OK;
	return true;
}

static PlannedStmt *
MakeDirectInsertPlannedStmt(Query *parse, List *custom_private) {
	PlannedStmt *pstmt = makeNode(PlannedStmt);
	pstmt->commandType = CMD_INSERT;
	pstmt->hasReturning = false;
	pstmt->hasModifyingCTE = false;
	pstmt->canSetTag = true;
	pstmt->transientPlan = false;
	pstmt->dependsOnRole = true;
	pstmt->parallelModeNeeded = false;
#if PG_VERSION_NUM >= 190000
	pstmt->resultRelationRelids = bms_make_singleton(parse->resultRelation);
	pstmt->unprunableRelids = bms_make_singleton(parse->resultRelation);
#else
	pstmt->resultRelations = list_make1_int(parse->resultRelation);
#endif
	pstmt->rtable = parse->rtable;
#if PG_VERSION_NUM >= 160000
	pstmt->permInfos = parse->rteperminfos;
#endif

	CustomScan *cscan = makeNode(CustomScan);
	cscan->scan.plan.targetlist = NIL;
	cscan->scan.plan.qual = NIL;
	cscan->scan.plan.lefttree = NULL;
	cscan->scan.plan.righttree = NULL;
	cscan->flags = 0;
	cscan->methods = &direct_insert_scan_methods;
	cscan->custom_private = custom_private;

	pstmt->planTree = (Plan *)cscan;
	return pstmt;
}

static PlannedStmt *
CreateDirectInsertPlan(Query *parse, DirectInsertContext *context) {
	List *custom_private = NIL;
	custom_private = lappend(custom_private, makeInteger(DIRECT_INSERT_UNNEST));
	custom_private = lappend(custom_private, makeInteger((int)context->target_table_oid));
	custom_private = lappend(custom_private, makeInteger((int)(context->table_id & 0xFFFFFFFF)));
	custom_private = lappend(custom_private, makeInteger((int)((context->table_id >> 32) & 0xFFFFFFFF)));
	custom_private = lappend(custom_private, makeInteger((int)(context->schema_version & 0xFFFFFFFF)));
	custom_private = lappend(custom_private, makeInteger((int)((context->schema_version >> 32) & 0xFFFFFFFF)));

	custom_private = lappend(custom_private, makeInteger(list_length(context->param_infos)));
	ListCell *lc;
	foreach (lc, context->param_infos) {
		ParamInfo *pinfo = (ParamInfo *)lfirst(lc);
		custom_private = lappend(custom_private, makeInteger(pinfo->param_id));
		custom_private = lappend(custom_private, makeInteger((int)pinfo->element_type));
	}

	custom_private = lappend(custom_private, makeInteger(list_length(context->target_col_names)));
	foreach (lc, context->target_col_names) {
		char *colname = (char *)lfirst(lc);
		custom_private = lappend(custom_private, makeString(pstrdup(colname)));
	}

	custom_private = lappend(custom_private, makeInteger(list_length(context->target_col_types)));
	foreach (lc, context->target_col_types) {
		Oid coltype = lfirst_oid(lc);
		custom_private = lappend(custom_private, makeInteger((int)coltype));
	}

	custom_private = lappend(custom_private, copyObjectImpl(parse));
	return MakeDirectInsertPlannedStmt(parse, custom_private);
}

static PlannedStmt *
CreateValuesInsertPlan(Query *parse, ValuesInsertContext *context) {
	List *custom_private = NIL;
	custom_private = lappend(custom_private, makeInteger(DIRECT_INSERT_VALUES));
	custom_private = lappend(custom_private, makeInteger((int)context->target_table_oid));
	custom_private = lappend(custom_private, makeInteger((int)(context->table_id & 0xFFFFFFFF)));
	custom_private = lappend(custom_private, makeInteger((int)((context->table_id >> 32) & 0xFFFFFFFF)));
	custom_private = lappend(custom_private, makeInteger((int)(context->schema_version & 0xFFFFFFFF)));
	custom_private = lappend(custom_private, makeInteger((int)((context->schema_version >> 32) & 0xFFFFFFFF)));
	custom_private = lappend(custom_private, makeInteger(context->num_rows));
	custom_private = lappend(custom_private, makeInteger(context->num_cols));

	custom_private = lappend(custom_private, makeInteger(list_length(context->target_col_names)));
	ListCell *lc;
	foreach (lc, context->target_col_names) {
		custom_private = lappend(custom_private, makeString(pstrdup((char *)lfirst(lc))));
	}

	custom_private = lappend(custom_private, makeInteger(list_length(context->inlined_col_types)));
	foreach (lc, context->inlined_col_types) {
		custom_private = lappend(custom_private, makeInteger((int)lfirst_oid(lc)));
	}

	custom_private = lappend(custom_private, makeInteger(list_length(context->src_col_types)));
	foreach (lc, context->src_col_types) {
		custom_private = lappend(custom_private, makeInteger((int)lfirst_oid(lc)));
	}

	/* Cell expressions: num_rows * num_cols Node*.  Const cells are still
	 * Const; STABLE-coercion cells are FuncExpr/RelabelType/etc.  PG's
	 * out/read funcs handle either. */
	int total = context->num_rows * context->num_cols;
	for (int i = 0; i < total; i++) {
		custom_private = lappend(custom_private, context->exprs[i]);
	}

	custom_private = lappend(custom_private, copyObjectImpl(parse));
	return MakeDirectInsertPlannedStmt(parse, custom_private);
}

static inline Node *
NextPrivate(List *priv, ListCell **lc) {
	Node *n = (Node *)lfirst(*lc);
	*lc = lnext(priv, *lc);
	return n;
}

static Node *
DirectInsert_CreateCustomScanState(CustomScan *cscan) {
	DirectInsertScanState *state = (DirectInsertScanState *)palloc0(sizeof(DirectInsertScanState));
	NodeSetTag(state, T_CustomScanState);
	state->css.methods = &direct_insert_exec_methods;

	List *priv = cscan->custom_private;
	ListCell *lc = list_head(priv);

	state->mode = (DirectInsertMode)intVal(NextPrivate(priv, &lc));
	state->target_table_oid = (Oid)intVal(NextPrivate(priv, &lc));

	uint32_t tlo = (uint32_t)intVal(NextPrivate(priv, &lc));
	uint32_t thi = (uint32_t)intVal(NextPrivate(priv, &lc));
	state->table_id = ((uint64_t)thi << 32) | tlo;

	uint32_t slo = (uint32_t)intVal(NextPrivate(priv, &lc));
	uint32_t shi = (uint32_t)intVal(NextPrivate(priv, &lc));
	state->schema_version = ((uint64_t)shi << 32) | slo;

	if (state->mode == DIRECT_INSERT_UNNEST) {
		state->expected_row_count = -1;

		int num_params = intVal(NextPrivate(priv, &lc));
		state->param_ids = NIL;
		state->element_types = NIL;
		for (int i = 0; i < num_params; i++) {
			state->param_ids = lappend_int(state->param_ids, intVal(NextPrivate(priv, &lc)));
			state->element_types = lappend_oid(state->element_types, (Oid)intVal(NextPrivate(priv, &lc)));
		}

		int num_cols = intVal(NextPrivate(priv, &lc));
		state->column_names = NIL;
		for (int i = 0; i < num_cols; i++) {
			state->column_names = lappend(state->column_names, makeString(pstrdup(strVal(NextPrivate(priv, &lc)))));
		}

		int num_types = intVal(NextPrivate(priv, &lc));
		state->column_types = NIL;
		for (int i = 0; i < num_types; i++) {
			state->column_types = lappend_oid(state->column_types, (Oid)intVal(NextPrivate(priv, &lc)));
		}
	} else {
		state->values_num_rows = intVal(NextPrivate(priv, &lc));
		state->values_num_cols = intVal(NextPrivate(priv, &lc));

		int num_names = intVal(NextPrivate(priv, &lc));
		state->column_names = NIL;
		for (int i = 0; i < num_names; i++) {
			state->column_names = lappend(state->column_names, makeString(pstrdup(strVal(NextPrivate(priv, &lc)))));
		}

		int num_inl_types = intVal(NextPrivate(priv, &lc));
		state->column_types = NIL;
		for (int i = 0; i < num_inl_types; i++) {
			state->column_types = lappend_oid(state->column_types, (Oid)intVal(NextPrivate(priv, &lc)));
		}

		int num_src_types = intVal(NextPrivate(priv, &lc));
		state->values_src_types = (Oid *)palloc(sizeof(Oid) * num_src_types);
		for (int i = 0; i < num_src_types; i++) {
			state->values_src_types[i] = (Oid)intVal(NextPrivate(priv, &lc));
		}

		/* Decode cell expressions; ExprStates are built lazily in
		 * DirectInsert_BeginCustomScan once the executor's PlanState is
		 * available. */
		int total = state->values_num_rows * state->values_num_cols;
		state->values_exprs = (Node **)palloc(sizeof(Node *) * total);
		state->values_estates = NULL;
		for (int i = 0; i < total; i++) {
			state->values_exprs[i] = NextPrivate(priv, &lc);
		}
	}

	state->finished = false;
	state->rows_inserted = 0;

	return (Node *)state;
}

static void
DirectInsert_BeginCustomScan(CustomScanState *node, EState *estate, int eflags) {
	DirectInsertScanState *state = (DirectInsertScanState *)node;

	if (state->mode == DIRECT_INSERT_UNNEST) {
		state->bound_params = estate->es_param_list_info;
		if (!state->bound_params) {
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("direct insert: no bound parameters found")));
		}
	}

	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf, "ducklake.ducklake_inlined_data_%llu_%llu", (unsigned long long)state->table_id,
	                 (unsigned long long)state->schema_version);
	state->inlined_table_name = buf.data;

	if (state->mode == DIRECT_INSERT_VALUES) {
		/* One ExprState per cell -- needed for STABLE coercions and other
		 * non-Const expressions that survived eval_const_expressions. */
		int total = state->values_num_rows * state->values_num_cols;
		state->values_estates = (ExprState **)palloc(sizeof(ExprState *) * total);
		MemoryContext old_ctx = MemoryContextSwitchTo(estate->es_query_cxt);
		for (int i = 0; i < total; i++) {
			state->values_estates[i] = ExecInitExpr((Expr *)state->values_exprs[i], &node->ss.ps);
		}
		MemoryContextSwitchTo(old_ctx);
	}

	/* Candidate system values are assigned immediately before the one-time
	 * payload prewrite in ExecCustomScan. */
}

static TupleTableSlot *
DirectInsert_ExecCustomScan(CustomScanState *node) {
	DirectInsertScanState *state = (DirectInsertScanState *)node;

	if (state->finished) {
		return NULL;
	}

	if (state->mode == DIRECT_INSERT_UNNEST) {
		BindUnnestParameters(state);
		if (state->expected_row_count == 0) {
			state->finished = true;
			node->ss.ps.state->es_processed = 0;
			pgducklake::ResetDirectInsertCaches();
			return NULL;
		}
	}

	uint64_t expected_rows = state->mode == DIRECT_INSERT_UNNEST ? state->expected_row_count : state->values_num_rows;
	NativeInlineWriteBatch batch =
	    PrepareNativeInlineWrite(state->target_table_oid, state->table_id, state->schema_version, expected_rows);
	state->begin_snapshot = batch.candidate_snapshot_id;
	state->next_row_id = batch.candidate_row_id;
	state->rows_inserted = 0;

	/* The producer runs exactly once. PublishNativeInlineWrite retains these
	 * parent-transaction rows across snapshot-claim retries. */
	if (state->mode == DIRECT_INSERT_UNNEST) {
		batch.column_stats = DirectInsertIntoInlinedTable(state, &batch, &batch.column_stats_count);
	} else {
		batch.column_stats = DirectInsertValuesIntoInlinedTable(state, &batch, &batch.column_stats_count);
	}

	batch.rows_inserted = state->rows_inserted;
	PublishNativeInlineWrite(batch);

	state->finished = true;
	node->ss.ps.state->es_processed = state->rows_inserted;

	pgducklake::ResetDirectInsertCaches();

	return NULL;
}

static void
DirectInsert_EndCustomScan(CustomScanState *node) {
}

static void
DirectInsert_ReScanCustomScan(CustomScanState *node) {
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("direct insert does not support rescan")));
}

static void
DirectInsert_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es) {
	DirectInsertScanState *state = (DirectInsertScanState *)node;

	ExplainPropertyText("Custom Scan", "DuckLakeDirectInsert", es);
	const char *pattern = (state->mode == DIRECT_INSERT_UNNEST) ? "UNNEST" : "VALUES";
	ExplainPropertyText("Pattern", pattern, es);
	if (state->mode == DIRECT_INSERT_UNNEST && state->expected_row_count < 0) {
		ExplainPropertyText("Expected Rows", "bound at execution", es);
	} else {
		int nrows = (state->mode == DIRECT_INSERT_UNNEST) ? state->expected_row_count : state->values_num_rows;
		ExplainPropertyInteger("Expected Rows", NULL, nrows, es);
	}

	if (es->verbose) {
		ExplainPropertyText("Inlined Table", state->inlined_table_name, es);
	}
}

/* System columns prepended to the inlined data table: row_id,
 * begin_snapshot, end_snapshot. */
#define INLINED_SYSTEM_COLS 3

#define MAX_BUFFERED_TUPLES 1000

static NativeInlineColumnStat *
DirectInsertIntoInlinedTable(DirectInsertScanState *state, NativeInlineWriteBatch *batch, uint64_t *stats_count) {
	int num_params = list_length(state->param_ids);
	ArrayType **arrays = state->unnest_arrays;
	Oid *element_types = (Oid *)palloc(sizeof(Oid) * num_params);
	Oid *inlined_types = (Oid *)palloc(sizeof(Oid) * num_params);
	ArrayIterator *iterators = (ArrayIterator *)palloc0(sizeof(ArrayIterator) * num_params);
	FmgrInfo *typoutput = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * num_params);
	bool *needs_text_conv = (bool *)palloc0(sizeof(bool) * num_params);

	ListCell *lc;
	int param_idx = 0;
	foreach (lc, state->element_types) {
		element_types[param_idx++] = lfirst_oid(lc);
	}
	int arr_length = state->expected_row_count;

	int idx = 0;
	foreach (lc, state->column_types) {
		inlined_types[idx++] = lfirst_oid(lc);
	}
	/* Iterators return pointers into the parameter arrays. Unlike
	 * deconstruct_array(), they do not allocate a Datum/null pair per input
	 * element, so only converted values in the current insert batch are kept. */
	for (int i = 0; i < num_params; i++) {
		if (arrays[i]) {
			iterators[i] = array_create_iterator(arrays[i], 0, NULL);
		}
		if ((inlined_types[i] == TEXTOID || inlined_types[i] == VARCHAROID) && inlined_types[i] != element_types[i]) {
			Oid output_oid;
			bool typisvarlena;
			getTypeOutputInfo(element_types[i], &output_oid, &typisvarlena);
			fmgr_info(output_oid, &typoutput[i]);
			needs_text_conv[i] = true;
		}
	}

	SPI_connect();
	InlineColStats *col_stats = CreateInlineColStats(state->table_id, num_params);
	SPI_finish();
	for (int i = 0; i < num_params; i++) {
		SetupInlineColStatsColumn(col_stats, i, element_types[i]);
	}

	char relname[NAMEDATALEN];
	snprintf(relname, sizeof(relname), "ducklake_inlined_data_%llu_%llu", (unsigned long long)state->table_id,
	         (unsigned long long)state->schema_version);
	Oid ducklake_nsp = get_namespace_oid("ducklake", false);
	Oid relid = get_relname_relid(relname, ducklake_nsp);
	if (!OidIsValid(relid)) {
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("inlined data table \"%s\" does not exist", relname)));
	}
	Relation inlined_rel = table_open(relid, RowExclusiveLock);
	BindNativeInlineWriteRelation(batch, inlined_rel);
	TupleDesc inlined_tupdesc = RelationGetDescr(inlined_rel);
	if (inlined_tupdesc->natts != num_params + INLINED_SYSTEM_COLS) {
		ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
		                errmsg("inlined data table column count mismatch: expected %d, got %d",
		                       num_params + INLINED_SYSTEM_COLS, inlined_tupdesc->natts)));
	}

	int batch_size = Min(arr_length, MAX_BUFFERED_TUPLES);
	TupleTableSlot **slots = (TupleTableSlot **)palloc(sizeof(TupleTableSlot *) * batch_size);
	for (int i = 0; i < batch_size; i++) {
		slots[i] = MakeSingleTupleTableSlot(inlined_tupdesc, &TTSOpsVirtual);
	}
	BulkInsertState bistate = GetBulkInsertState();
	CommandId cid = GetCurrentCommandId(true);
	MemoryContext batch_context =
	    AllocSetContextCreate(CurrentMemoryContext, "DirectInsertUnnestBatch", ALLOCSET_DEFAULT_SIZES);

	int nslots = 0;
	uint64_t current_row_id = state->next_row_id;
	for (int row = 0; row < arr_length; row++) {
		TupleTableSlot *slot = slots[nslots];
		ExecClearTuple(slot);
		Datum *values = slot->tts_values;
		bool *nulls = slot->tts_isnull;
		values[0] = Int64GetDatum((int64)current_row_id++);
		nulls[0] = false;
		values[1] = Int64GetDatum((int64)state->begin_snapshot);
		nulls[1] = false;
		values[2] = (Datum)0;
		nulls[2] = true;

		for (int i = 0; i < num_params; i++) {
			int dst = i + INLINED_SYSTEM_COLS;
			Datum element = (Datum)0;
			bool isnull = row >= state->unnest_array_lengths[i];
			if (!isnull && !array_iterate(iterators[i], &element, &isnull)) {
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("direct UNNEST array ended unexpectedly")));
			}
			if (isnull) {
				ObserveInlineColStatsNull(col_stats, i);
				values[dst] = (Datum)0;
				nulls[dst] = true;
			} else if (needs_text_conv[i]) {
				ObserveInlineColStatsDatum(col_stats, i, element);
				MemoryContext old_context = MemoryContextSwitchTo(batch_context);
				char *str = OutputFunctionCallIso(&typoutput[i], element);
				values[dst] = CStringGetTextDatum(str);
				pfree(str);
				MemoryContextSwitchTo(old_context);
				nulls[dst] = false;
			} else {
				ObserveInlineColStatsDatum(col_stats, i, element);
				values[dst] = element;
				nulls[dst] = false;
			}
		}
		ExecStoreVirtualTuple(slot);
		nslots++;

		if (nslots == batch_size) {
			table_multi_insert(inlined_rel, slots, nslots, cid, 0, bistate);
			RecordNativeInlineWriteRows(batch, slots, nslots);
			state->rows_inserted += nslots;
			NativeWriterStatsAdd(NW_PAYLOAD_ROWS, nslots);
			for (int i = 0; i < nslots; i++) {
				ExecClearTuple(slots[i]);
			}
			nslots = 0;
			MemoryContextReset(batch_context);
		}
	}
	if (nslots > 0) {
		table_multi_insert(inlined_rel, slots, nslots, cid, 0, bistate);
		RecordNativeInlineWriteRows(batch, slots, nslots);
		state->rows_inserted += nslots;
		NativeWriterStatsAdd(NW_PAYLOAD_ROWS, nslots);
	}

	table_finish_bulk_insert(inlined_rel, 0);
	FreeBulkInsertState(bistate);
	for (int i = 0; i < batch_size; i++) {
		ExecDropSingleTupleTableSlot(slots[i]);
	}
	for (int i = 0; i < num_params; i++) {
		if (iterators[i]) {
			array_free_iterator(iterators[i]);
		}
	}
	MemoryContextDelete(batch_context);
	/* Publication owns the retained RowExclusiveLock through any retag. */
	table_close(inlined_rel, NoLock);

	NativeInlineColumnStat *result = FinalizeInlineColStats(col_stats, stats_count);
	ereport(DEBUG1, (errmsg("DuckLake direct insert: successfully inserted %lld rows into %s",
	                        (long long)state->rows_inserted, state->inlined_table_name)));
	return result;
}

struct ValuesColumnConvInfo {
	bool needs_text_conv;
	FmgrInfo typoutput_finfo; /* cached output function */
};

static NativeInlineColumnStat *
DirectInsertValuesIntoInlinedTable(DirectInsertScanState *state, NativeInlineWriteBatch *batch, uint64_t *stats_count) {
	int num_rows = state->values_num_rows;
	int num_cols = state->values_num_cols;
	ExprContext *econtext = state->css.ss.ps.ps_ExprContext;

	SPI_connect();
	InlineColStats *col_stats = CreateInlineColStats(state->table_id, num_cols);
	SPI_finish();

	char relname[NAMEDATALEN];
	snprintf(relname, sizeof(relname), "ducklake_inlined_data_%llu_%llu", (unsigned long long)state->table_id,
	         (unsigned long long)state->schema_version);

	Oid ducklake_nsp = get_namespace_oid("ducklake", false);
	Oid relid = get_relname_relid(relname, ducklake_nsp);
	if (!OidIsValid(relid)) {
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("inlined data table \"%s\" does not exist", relname)));
	}

	Relation inlined_rel = table_open(relid, RowExclusiveLock);
	BindNativeInlineWriteRelation(batch, inlined_rel);
	TupleDesc inlined_tupdesc = RelationGetDescr(inlined_rel);

	ValuesColumnConvInfo *conv = (ValuesColumnConvInfo *)palloc0(sizeof(ValuesColumnConvInfo) * num_cols);
	ListCell *inl_lc = list_head(state->column_types);

	for (int i = 0; i < num_cols; i++) {
		Oid src_type = state->values_src_types[i];
		Oid inl_type = lfirst_oid(inl_lc);
		inl_lc = lnext(state->column_types, inl_lc);

		if (src_type == inl_type) {
			conv[i].needs_text_conv = false;
		} else if (inl_type == VARCHAROID || inl_type == TEXTOID) {
			/* Scalar types (DATE, TIMESTAMP, etc.) stored as VARCHAR in
			 * the inlined table: convert via PG output function. */
			Oid typoutput;
			bool typisvarlena;
			getTypeOutputInfo(src_type, &typoutput, &typisvarlena);
			fmgr_info(typoutput, &conv[i].typoutput_finfo);
			conv[i].needs_text_conv = true;
		} else if (inl_type == BYTEAOID) {
			/* DuckDB VARCHAR/BLOB inline as BYTEA; text/varchar and bytea
			 * share the same varlena layout, so store the Datum as-is. */
			conv[i].needs_text_conv = false;
		} else {
			conv[i].needs_text_conv = false;
		}
		SetupInlineColStatsColumn(col_stats, i, src_type);
	}

	int batch_size = (num_rows < MAX_BUFFERED_TUPLES) ? num_rows : MAX_BUFFERED_TUPLES;
	TupleTableSlot **slots = (TupleTableSlot **)palloc(sizeof(TupleTableSlot *) * batch_size);
	for (int i = 0; i < batch_size; i++) {
		slots[i] = MakeSingleTupleTableSlot(inlined_tupdesc, &TTSOpsVirtual);
	}

	BulkInsertState bistate = GetBulkInsertState();
	CommandId cid = GetCurrentCommandId(true);

	int nslots = 0;
	uint64_t current_row_id = state->next_row_id;

	for (int row = 0; row < num_rows; row++) {
		TupleTableSlot *slot = slots[nslots];
		ExecClearTuple(slot);

		Datum *sv = slot->tts_values;
		bool *sn = slot->tts_isnull;

		/* System columns */
		sv[0] = Int64GetDatum((int64)current_row_id++);
		sn[0] = false;
		sv[1] = Int64GetDatum((int64)state->begin_snapshot);
		sn[1] = false;
		sv[2] = (Datum)0; /* end_snapshot = NULL */
		sn[2] = true;

		/* User columns */
		for (int col = 0; col < num_cols; col++) {
			int flat = row * num_cols + col;
			int dst = col + INLINED_SYSTEM_COLS;

			bool isnull;
			Datum d = ExecEvalExprSwitchContext(state->values_estates[flat], econtext, &isnull);

			if (isnull) {
				ObserveInlineColStatsNull(col_stats, col);
				sv[dst] = (Datum)0;
				sn[dst] = true;
			} else if (conv[col].needs_text_conv) {
				ObserveInlineColStatsDatum(col_stats, col, d);
				char *str = OutputFunctionCallIso(&conv[col].typoutput_finfo, d);
				sv[dst] = CStringGetTextDatum(str);
				sn[dst] = false;
				pfree(str);
			} else {
				ObserveInlineColStatsDatum(col_stats, col, d);
				sv[dst] = d;
				sn[dst] = false;
			}
		}

		ExecStoreVirtualTuple(slot);
		nslots++;

		if (nslots >= batch_size) {
			table_multi_insert(inlined_rel, slots, nslots, cid, 0, bistate);
			RecordNativeInlineWriteRows(batch, slots, nslots);
			state->rows_inserted += nslots;
			NativeWriterStatsAdd(NW_PAYLOAD_ROWS, nslots);
			for (int i = 0; i < nslots; i++) {
				ExecClearTuple(slots[i]);
			}
			nslots = 0;
			ResetExprContext(econtext);
		}
	}

	if (nslots > 0) {
		table_multi_insert(inlined_rel, slots, nslots, cid, 0, bistate);
		RecordNativeInlineWriteRows(batch, slots, nslots);
		state->rows_inserted += nslots;
		NativeWriterStatsAdd(NW_PAYLOAD_ROWS, nslots);
	}

	table_finish_bulk_insert(inlined_rel, 0);
	FreeBulkInsertState(bistate);

	for (int i = 0; i < batch_size; i++) {
		ExecDropSingleTupleTableSlot(slots[i]);
	}
	pfree(slots);
	pfree(conv);

	/* Publication owns the retained RowExclusiveLock through any retag. */
	table_close(inlined_rel, NoLock);

	NativeInlineColumnStat *result = FinalizeInlineColStats(col_stats, stats_count);

	ereport(DEBUG1, (errmsg("DuckLake direct insert (VALUES): inserted %d rows into %s", num_rows, relname)));
	return result;
}

/* Direct-insert outcome counters in shared memory. */

namespace {

struct DirectInsertStatsShmemStruct {
	slock_t lock;
	uint64_t counters[DI_PAT_NUM][DI_R_NUM];
};

DirectInsertStatsShmemStruct *StatsShmem = nullptr;

#if PG_VERSION_NUM >= 150000
shmem_request_hook_type prev_shmem_request_hook = nullptr;
#endif
shmem_startup_hook_type prev_shmem_startup_hook = nullptr;

void
ShmemRequest() {
#if PG_VERSION_NUM >= 150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif
	RequestAddinShmemSpace(sizeof(DirectInsertStatsShmemStruct));
}

void
ShmemStartup() {
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	bool found;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	StatsShmem = (DirectInsertStatsShmemStruct *)ShmemInitStruct("DuckLakeDirectInsertStats",
	                                                             sizeof(DirectInsertStatsShmemStruct), &found);
	if (!found) {
		MemSet(StatsShmem, 0, sizeof(DirectInsertStatsShmemStruct));
		SpinLockInit(&StatsShmem->lock);
	}
	LWLockRelease(AddinShmemInitLock);
}

} // namespace

void
InitDirectInsertStatsShmem() {
#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = ShmemRequest;
#else
	ShmemRequest();
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ShmemStartup;
}

void
DirectInsertStatsBump(DirectInsertPattern pattern, DirectInsertReason reason) {
	if (!StatsShmem)
		return;
	if (pattern < 0 || pattern >= DI_PAT_NUM)
		return;
	if (reason < 0 || reason >= DI_R_NUM)
		return;
	SpinLockAcquire(&StatsShmem->lock);
	StatsShmem->counters[pattern][reason]++;
	SpinLockRelease(&StatsShmem->lock);
}

void
DirectInsertStatsReset() {
	if (!StatsShmem)
		return;
	SpinLockAcquire(&StatsShmem->lock);
	memset(StatsShmem->counters, 0, sizeof(StatsShmem->counters));
	SpinLockRelease(&StatsShmem->lock);
}

uint64_t
DirectInsertStatsRead(DirectInsertPattern pattern, DirectInsertReason reason) {
	if (!StatsShmem)
		return 0;
	if (pattern < 0 || pattern >= DI_PAT_NUM)
		return 0;
	if (reason < 0 || reason >= DI_R_NUM)
		return 0;
	SpinLockAcquire(&StatsShmem->lock);
	uint64_t value = StatsShmem->counters[pattern][reason];
	SpinLockRelease(&StatsShmem->lock);
	return value;
}

void
DirectInsertStatsReadAll(uint64_t out[DI_PAT_NUM][DI_R_NUM]) {
	if (!StatsShmem) {
		memset(out, 0, sizeof(uint64_t) * DI_PAT_NUM * DI_R_NUM);
		return;
	}
	SpinLockAcquire(&StatsShmem->lock);
	memcpy(out, StatsShmem->counters, sizeof(uint64_t) * DI_PAT_NUM * DI_R_NUM);
	SpinLockRelease(&StatsShmem->lock);
}

const char *
DirectInsertPatternName(DirectInsertPattern pattern) {
	switch (pattern) {
	case DI_PAT_MATCHED_UNNEST:
		return "matched_unnest";
	case DI_PAT_MATCHED_VALUES:
		return "matched_values";
	case DI_PAT_UNMATCHED:
		return "unmatched";
	default:
		return "unknown";
	}
}

const char *
DirectInsertReasonName(DirectInsertReason reason) {
	switch (reason) {
	case DI_R_OK:
		return "ok";
	case DI_R_INVALID_RTE:
		return "invalid_rte";
	case DI_R_NO_INLINED_TABLE:
		return "no_inlined_table";
	case DI_R_SCHEMA_VERSION_MISMATCH:
		return "schema_version_mismatch";
	case DI_R_COL_TYPES_UNSUPPORTED:
		return "col_types_unsupported";
	case DI_R_GREATER_THAN_LIMIT:
		return "greater_than_limit";
	case DI_R_UNSUPPORTED_INSERT_SHAPE:
		return "unsupported_insert_shape";
	case DI_R_RETRY:
		return "retry";
	default:
		return "unknown";
	}
}

} // namespace pgducklake

extern "C" {

/*
 * Emits a fixed row set (matched_unnest/matched_values + ok, unmatched x
 * every non-ok reason) so the shape is stable across resets.
 */
PG_FUNCTION_INFO_V1(ducklake_direct_insert_stats);
Datum
ducklake_direct_insert_stats(PG_FUNCTION_ARGS) {
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		                errmsg("materialize mode required, but it is not allowed in this context")));

	TupleDesc tupdesc;
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("return type must be a row type")));

	MemoryContext per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	MemoryContext old_ctx = MemoryContextSwitchTo(per_query_ctx);

	Tuplestorestate *tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = BlessTupleDesc(tupdesc);

	MemoryContextSwitchTo(old_ctx);

	Datum values[3];
	bool nulls[3] = {false, false, false};

	/* Snapshot the matrix once under the spinlock to keep emitted rows
	 * consistent and avoid N lock round-trips. */
	uint64_t snap[pgducklake::DI_PAT_NUM][pgducklake::DI_R_NUM];
	pgducklake::DirectInsertStatsReadAll(snap);

	/* Matched rows */
	for (int p = pgducklake::DI_PAT_MATCHED_UNNEST; p <= pgducklake::DI_PAT_MATCHED_VALUES; p++) {
		values[0] = CStringGetTextDatum(pgducklake::DirectInsertPatternName((pgducklake::DirectInsertPattern)p));
		values[1] = CStringGetTextDatum(pgducklake::DirectInsertReasonName(pgducklake::DI_R_OK));
		values[2] = Int64GetDatum((int64_t)snap[p][pgducklake::DI_R_OK]);
		tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);
	}

	/* Unmatched rows for every non-ok reason */
	for (int r = pgducklake::DI_R_OK + 1; r < pgducklake::DI_R_NUM; r++) {
		values[0] = CStringGetTextDatum(pgducklake::DirectInsertPatternName(pgducklake::DI_PAT_UNMATCHED));
		values[1] = CStringGetTextDatum(pgducklake::DirectInsertReasonName((pgducklake::DirectInsertReason)r));
		values[2] = Int64GetDatum((int64_t)snap[pgducklake::DI_PAT_UNMATCHED][r]);
		tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);
	}

	return (Datum)0;
}

PG_FUNCTION_INFO_V1(ducklake_reset_direct_insert_stats);
Datum
ducklake_reset_direct_insert_stats(PG_FUNCTION_ARGS) {
	pgducklake::DirectInsertStatsReset();
	PG_RETURN_VOID();
}

} // extern "C"
