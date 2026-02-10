/*
 * Inline Bypass for DuckLake INSERT...SELECT UNNEST pattern
 *
 * This module optimizes the common ETL pattern:
 *   INSERT INTO ducklake_table SELECT UNNEST($1::type[]), UNNEST($2::type[]), ...
 *
 * Instead of going through DuckDB execution (which involves converting parameters
 * to DuckDB values, executing UNNEST, then converting back to SQL literals for
 * the inlined data table), we directly insert into the PostgreSQL inlined data
 * table using SPI with the original array Datums.
 *
 * Constraints:
 *   - Only single-statement mode (no BEGIN/transaction block)
 *   - Uses sub-transactions for retry on snapshot conflict
 */

/* DuckDB headers must come BEFORE postgres headers to avoid macro conflicts */
#include "pgduckdb/ducklake/pgducklake_inline_bypass.hpp"
#include "pgduckdb/ducklake/pgducklake_defs.hpp"
#include "pgduckdb/pg/explain.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_metadata_cache.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"

extern "C" {
#include "postgres.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
}

namespace pgduckdb {

/* DuckLake metadata schema in PostgreSQL */
static const char *DUCKLAKE_METADATA_SCHEMA = "ducklake";
static const int MAX_BYPASS_RETRY_COUNT = 5;

/* Custom scan methods for inline bypass */
static CustomScanMethods inline_bypass_scan_methods;
static CustomExecMethods inline_bypass_exec_methods;

/* State for the inline bypass custom scan */
struct InlineBypassState {
	CustomScanState css; /* must be first field */
	InlineBypassInfo info;
	bool executed;
	int64_t rows_inserted;
};

/*
 * Check if an expression is UNNEST(Param) where Param is an array type.
 * Returns the param index (0-based) if it matches, -1 otherwise.
 */
static int
GetUnnestParamIndex(Node *node, Oid *array_type_out, Oid *elem_type_out) {
	/* We're looking for a FuncExpr that is UNNEST */
	if (!IsA(node, FuncExpr)) {
		return -1;
	}

	FuncExpr *func = (FuncExpr *)node;

	/* Check if this is an UNNEST function - it should have a single array argument */
	char *funcname = get_func_name(func->funcid);
	if (!funcname || strcmp(funcname, "unnest") != 0) {
		pfree(funcname);
		return -1;
	}
	pfree(funcname);

	/* UNNEST should have exactly one argument */
	if (list_length(func->args) != 1) {
		return -1;
	}

	Node *arg = (Node *)linitial(func->args);

	/* The argument should be a Param (possibly with a type coercion) */
	Param *param = nullptr;

	if (IsA(arg, Param)) {
		param = (Param *)arg;
	} else if (IsA(arg, CoerceViaIO) || IsA(arg, ArrayCoerceExpr) || IsA(arg, RelabelType)) {
		/* Handle type coercions - extract the underlying Param */
		Node *inner = arg;
		while (inner) {
			if (IsA(inner, Param)) {
				param = (Param *)inner;
				break;
			} else if (IsA(inner, CoerceViaIO)) {
				inner = (Node *)((CoerceViaIO *)inner)->arg;
			} else if (IsA(inner, ArrayCoerceExpr)) {
				inner = (Node *)((ArrayCoerceExpr *)inner)->arg;
			} else if (IsA(inner, RelabelType)) {
				inner = (Node *)((RelabelType *)inner)->arg;
			} else {
				break;
			}
		}
	}

	if (!param || param->paramkind != PARAM_EXTERN) {
		return -1;
	}

	/* Verify the parameter is an array type */
	Oid param_type = param->paramtype;
	Oid elem_type = get_element_type(param_type);
	if (!OidIsValid(elem_type)) {
		/* Not an array type */
		return -1;
	}

	*array_type_out = param_type;
	*elem_type_out = elem_type;

	/* paramid is 1-based, return 0-based index */
	return param->paramid - 1;
}

/*
 * Get the data_inlining_row_limit for a DuckLake table.
 * Returns 0 if inlining is disabled or the table is not a DuckLake table.
 *
 * This runs at planning time when DuckDB connection is available.
 */
static uint64_t
GetDataInliningRowLimit(Oid /*relid*/, const char *schema_name, const char *table_name) {
	auto connection = DuckDBManager::GetConnection();

	std::string scope_entry = std::string(schema_name) + "." + table_name;
	std::string query =
	    duckdb::StringUtil::Format("SELECT value FROM %s.options() "
	                               "WHERE option_name = 'data_inlining_row_limit' "
	                               "  AND ((scope = 'TABLE' AND scope_entry = '%s') OR scope = 'GLOBAL') "
	                               "ORDER BY CASE scope WHEN 'TABLE' THEN 0 ELSE 1 END "
	                               "LIMIT 1",
	                               PGDUCKLAKE_DB_NAME, scope_entry.c_str());

	auto result = connection->context->Query(query, false);
	if (result->HasError()) {
		return 0;
	}

	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return 0;
	}

	try {
		return chunk->GetValue(0, 0).GetValue<uint64_t>();
	} catch (...) {
		return 0;
	}
}

std::optional<InlineBypassInfo>
DetectInlineBypassPattern(Query *query) {
	/* Must be an INSERT command */
	if (query->commandType != CMD_INSERT) {
		return std::nullopt;
	}

	/* Only support single-statement mode (no transaction block) */
	if (IsTransactionBlock()) {
		return std::nullopt;
	}

	/* Must have a result relation */
	if (query->resultRelation == 0) {
		return std::nullopt;
	}

	/* Get the target table RTE */
	RangeTblEntry *result_rte = rt_fetch(query->resultRelation, query->rtable);
	if (!result_rte || result_rte->rtekind != RTE_RELATION) {
		return std::nullopt;
	}

	Oid target_relid = result_rte->relid;

	/* Check if it's a DuckLake table */
	Relation rel = RelationIdGetRelation(target_relid);
	if (!rel) {
		return std::nullopt;
	}

	bool is_ducklake = IsDucklakeTable(rel);
	const char *table_name = pstrdup(RelationGetRelationName(rel));
	Oid namespace_oid = RelationGetNamespace(rel);
	RelationClose(rel);

	if (!is_ducklake) {
		return std::nullopt;
	}

	const char *schema_name = get_namespace_name(namespace_oid);
	if (!schema_name) {
		return std::nullopt;
	}

	/* Check if data inlining is enabled for this table */
	uint64_t row_limit = GetDataInliningRowLimit(target_relid, schema_name, table_name);
	if (row_limit == 0) {
		return std::nullopt;
	}

	/* Now analyze the target list to see if it's all UNNEST(Param) expressions */
	InlineBypassInfo info;
	info.target_table_oid = target_relid;
	info.data_inlining_row_limit = row_limit;
	info.schema_name = schema_name;
	info.table_name = table_name;

	ListCell *lc;
	foreach (lc, query->targetList) {
		TargetEntry *te = (TargetEntry *)lfirst(lc);

		/* Skip junk columns (e.g., ctid for UPDATE) */
		if (te->resjunk) {
			continue;
		}

		Oid array_type, elem_type;
		int param_idx = GetUnnestParamIndex((Node *)te->expr, &array_type, &elem_type);

		if (param_idx < 0) {
			/* Not an UNNEST(Param) expression - try to resolve through Var -> RTE */
			if (IsA(te->expr, Var)) {
				Var *var = (Var *)te->expr;
				if (var->varno > 0 && var->varno <= (Index)list_length(query->rtable)) {
					RangeTblEntry *rte = rt_fetch(var->varno, query->rtable);
					if (rte->rtekind == RTE_FUNCTION) {
						RangeTblFunction *rtfunc = (RangeTblFunction *)linitial(rte->functions);
						param_idx = GetUnnestParamIndex(rtfunc->funcexpr, &array_type, &elem_type);
					} else if (rte->rtekind == RTE_SUBQUERY && rte->subquery) {
						TargetEntry *sub_te = list_nth_node(TargetEntry, rte->subquery->targetList, var->varattno - 1);
						param_idx = GetUnnestParamIndex((Node *)sub_te->expr, &array_type, &elem_type);
					}
				}
			}
			if (param_idx < 0) {
				return std::nullopt;
			}
		}

		info.param_indices.push_back(param_idx);
		info.array_types.push_back(array_type);
		info.array_elem_types.push_back(elem_type);
	}

	/* Must have at least one column */
	if (info.param_indices.empty()) {
		return std::nullopt;
	}

	return info;
}

/* Forward declarations for custom scan callbacks */
static Node *InlineBypass_CreateCustomScanState(CustomScan *cscan);
static void InlineBypass_BeginCustomScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *InlineBypass_ExecCustomScan(CustomScanState *node);
static void InlineBypass_EndCustomScan(CustomScanState *node);
static void InlineBypass_ReScanCustomScan(CustomScanState *node);
static void InlineBypass_ExplainCustomScan(CustomScanState *node, List *ancestors, ExplainState *es);

static Node *
InlineBypass_CreateCustomScanState(CustomScan *cscan) {
	InlineBypassState *state = (InlineBypassState *)newNode(sizeof(InlineBypassState), T_CustomScanState);
	state->css.methods = &inline_bypass_exec_methods;
	state->executed = false;
	state->rows_inserted = 0;

	/* Deserialize the InlineBypassInfo from custom_private */
	List *private_data = cscan->custom_private;
	if (list_length(private_data) >= 5) {
		state->info.target_table_oid = intVal(linitial(private_data));
		state->info.data_inlining_row_limit = intVal(lsecond(private_data));
		state->info.schema_name = strVal(lthird(private_data));
		state->info.table_name = strVal(lfourth(private_data));

		List *param_list = (List *)list_nth(private_data, 4);
		List *array_type_list = (List *)list_nth(private_data, 5);
		List *elem_type_list = (List *)list_nth(private_data, 6);

		ListCell *lc1, *lc2, *lc3;
		forthree(lc1, param_list, lc2, array_type_list, lc3, elem_type_list) {
			state->info.param_indices.push_back(intVal(lfirst(lc1)));
			state->info.array_types.push_back(intVal(lfirst(lc2)));
			state->info.array_elem_types.push_back(intVal(lfirst(lc3)));
		}
	}

	return (Node *)state;
}

/* ----------------------------------------------------------------
 * SPI-based executor helpers
 * ---------------------------------------------------------------- */

/*
 * Build the INSERT query string for the inlined data table.
 * Returns a palloc'd string.
 *
 * The query looks like:
 *   INSERT INTO ducklake.<inlined_table> (row_id, begin_snapshot, end_snapshot, col1, col2, ...)
 *   SELECT row_number() OVER () + $1 - 1, $2, NULL, UNNEST($3), UNNEST($4), ...
 */
static char *
BuildInsertQuery(const InlineBypassInfo &info, const char *inlined_table_name) {
	StringInfoData buf;
	initStringInfo(&buf);

	appendStringInfo(&buf, "INSERT INTO %s.%s (row_id, begin_snapshot, end_snapshot", DUCKLAKE_METADATA_SCHEMA,
	                 quote_identifier(inlined_table_name));

	/* Add column names from the target table */
	Relation target_rel = RelationIdGetRelation(info.target_table_oid);
	TupleDesc tupdesc = RelationGetDescr(target_rel);
	for (int i = 0; i < tupdesc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (attr->attisdropped) {
			continue;
		}
		appendStringInfo(&buf, ", %s", quote_identifier(NameStr(attr->attname)));
	}
	RelationClose(target_rel);

	appendStringInfo(&buf, ") SELECT row_number() OVER () + $1 - 1, $2, NULL");

	int num_cols = info.param_indices.size();
	for (int i = 0; i < num_cols; i++) {
		appendStringInfo(&buf, ", unnest($%d)", i + 3);
	}

	return buf.data;
}

/*
 * Build the column definition string for CREATE TABLE of the inlined data table.
 * Uses the target table's column types.
 */
static char *
BuildInlinedTableColumns(const InlineBypassInfo &info) {
	StringInfoData buf;
	initStringInfo(&buf);

	Relation target_rel = RelationIdGetRelation(info.target_table_oid);
	TupleDesc tupdesc = RelationGetDescr(target_rel);
	bool first = true;
	for (int i = 0; i < tupdesc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (attr->attisdropped) {
			continue;
		}
		if (!first) {
			appendStringInfoString(&buf, ", ");
		}
		appendStringInfo(&buf, "%s %s", quote_identifier(NameStr(attr->attname)), format_type_be(attr->atttypid));
		first = false;
	}
	RelationClose(target_rel);

	return buf.data;
}

/*
 * Read metadata needed for the bypass insert via SPI.
 * SPI must already be connected.
 *
 * Returns: table_id (or -1 on failure)
 * Fills: inlined_table_name, schema_version, next_row_id
 */
static int64_t
ReadTableId(const char *schema_name, const char *table_name) {
	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
	                 "SELECT t.table_id FROM %s.ducklake_table t "
	                 "JOIN %s.ducklake_schema s USING (schema_id) "
	                 "WHERE s.schema_name = '%s' AND t.table_name = '%s' "
	                 "AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL",
	                 DUCKLAKE_METADATA_SCHEMA, DUCKLAKE_METADATA_SCHEMA, schema_name, table_name);

	int ret = SPI_execute(query.data, true, 1);
	pfree(query.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0) {
		return -1;
	}

	bool isnull;
	Datum val = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull) {
		return -1;
	}
	return DatumGetInt64(val);
}

/*
 * Read latest snapshot info via SPI.
 * Returns snapshot_id (or -1 on failure).
 */
static int64_t
ReadLatestSnapshot(int64_t *schema_version_out, int64_t *next_catalog_id_out, int64_t *next_file_id_out) {
	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
	                 "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id "
	                 "FROM %s.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1",
	                 DUCKLAKE_METADATA_SCHEMA);

	int ret = SPI_execute(query.data, true, 1);
	pfree(query.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0) {
		return -1;
	}

	bool isnull;
	int64_t snapshot_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
	*schema_version_out = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
	*next_catalog_id_out = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isnull));
	*next_file_id_out = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4, &isnull));

	return snapshot_id;
}

/*
 * Read next_row_id and record_count from ducklake_table_stats via SPI.
 */
static int64_t
ReadNextRowId(int64_t table_id, int64_t *record_count_out) {
	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
	                 "SELECT next_row_id, record_count FROM %s.ducklake_table_stats "
	                 "WHERE table_id = " INT64_FORMAT,
	                 DUCKLAKE_METADATA_SCHEMA, table_id);

	int ret = SPI_execute(query.data, true, 1);
	pfree(query.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0) {
		*record_count_out = 0;
		return 1; /* default start */
	}

	bool isnull;
	int64_t next_row_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
	*record_count_out = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
	return next_row_id;
}

/*
 * Find existing inlined data table name via SPI.
 * Returns a palloc'd string or NULL if none exists.
 */
static char *
FindInlinedDataTable(int64_t table_id) {
	StringInfoData query;
	initStringInfo(&query);
	appendStringInfo(&query,
	                 "SELECT table_name FROM %s.ducklake_inlined_data_tables "
	                 "WHERE table_id = " INT64_FORMAT " ORDER BY schema_version DESC LIMIT 1",
	                 DUCKLAKE_METADATA_SCHEMA, table_id);

	int ret = SPI_execute(query.data, true, 1);
	pfree(query.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0) {
		return NULL;
	}

	bool isnull;
	Datum val = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	if (isnull) {
		return NULL;
	}
	return pstrdup(TextDatumGetCString(val));
}

/*
 * Create the inlined data table and register it in ducklake_inlined_data_tables.
 * SPI must be connected.
 * Returns a palloc'd table name or NULL on failure.
 */
static char *
CreateInlinedDataTable(const InlineBypassInfo &info, int64_t table_id, int64_t schema_version) {
	/* Table name format: ducklake_inlined_data_{table_id}_{schema_version} */
	char *inlined_table_name =
	    psprintf("ducklake_inlined_data_" INT64_FORMAT "_" INT64_FORMAT, table_id, schema_version);

	/* Build column definitions from the target table */
	char *col_defs = BuildInlinedTableColumns(info);

	/* CREATE TABLE IF NOT EXISTS */
	StringInfoData create_query;
	initStringInfo(&create_query);
	appendStringInfo(&create_query,
	                 "CREATE TABLE IF NOT EXISTS %s.%s ("
	                 "row_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, %s)",
	                 DUCKLAKE_METADATA_SCHEMA, quote_identifier(inlined_table_name), col_defs);

	int ret = SPI_execute(create_query.data, false, 0);
	pfree(create_query.data);
	pfree(col_defs);

	if (ret != SPI_OK_UTILITY) {
		pfree(inlined_table_name);
		return NULL;
	}

	/* Register in ducklake_inlined_data_tables (check for duplicates first) */
	StringInfoData check_query;
	initStringInfo(&check_query);
	appendStringInfo(&check_query,
	                 "SELECT 1 FROM %s.ducklake_inlined_data_tables "
	                 "WHERE table_id = " INT64_FORMAT " AND table_name = '%s'",
	                 DUCKLAKE_METADATA_SCHEMA, table_id, inlined_table_name);

	ret = SPI_execute(check_query.data, true, 1);
	pfree(check_query.data);

	if (ret == SPI_OK_SELECT && SPI_processed == 0) {
		/* Not yet registered — insert */
		StringInfoData reg_query;
		initStringInfo(&reg_query);
		appendStringInfo(
		    &reg_query, "INSERT INTO %s.ducklake_inlined_data_tables VALUES (" INT64_FORMAT ", '%s', " INT64_FORMAT ")",
		    DUCKLAKE_METADATA_SCHEMA, table_id, inlined_table_name, schema_version);

		ret = SPI_execute(reg_query.data, false, 0);
		pfree(reg_query.data);

		if (ret != SPI_OK_INSERT) {
			pfree(inlined_table_name);
			return NULL;
		}
	}

	return inlined_table_name;
}

/*
 * Execute the inline bypass insert.
 * Returns the number of rows inserted, or -1 if fallback to DuckDB is needed.
 */
static int64_t
ExecuteInlineBypass(InlineBypassState *state, ParamListInfo params) {
	InlineBypassInfo &info = state->info;
	MemoryContext caller_ctx = CurrentMemoryContext;

	if (!params || params->numParams == 0) {
		elog(WARNING, "[PGDuckLake] Inline bypass: no parameters provided");
		return -1;
	}

	/* ----------------------------------------------------------------
	 * Phase 0: Extract and validate array parameters
	 * ---------------------------------------------------------------- */
	int num_cols = info.param_indices.size();
	int64_t num_rows = -1;

	/* Allocate in current memory context (executor context, survives sub-txn rollback) */
	Datum *array_datums = (Datum *)palloc(sizeof(Datum) * num_cols);
	bool *array_nulls = (bool *)palloc0(sizeof(bool) * num_cols);

	for (int i = 0; i < num_cols; i++) {
		int param_idx = info.param_indices[i];
		if (param_idx >= params->numParams) {
			elog(ERROR, "[PGDuckLake] Inline bypass: parameter $%d not provided", param_idx + 1);
		}

		ParamExternData *param;
		ParamExternData tmp;
		if (params->paramFetch) {
			param = params->paramFetch(params, param_idx + 1, false, &tmp);
		} else {
			param = &params->params[param_idx];
		}

		if (param->isnull) {
			array_nulls[i] = true;
			continue;
		}

		array_datums[i] = param->value;
		array_nulls[i] = false;

		/* Validate array length */
		ArrayType *arr = DatumGetArrayTypeP(param->value);
		int arr_len = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

		if (num_rows < 0) {
			num_rows = arr_len;
		} else if (arr_len != num_rows) {
			elog(ERROR,
			     "[PGDuckLake] Inline bypass: array length mismatch "
			     "(column %d has %d rows, expected " INT64_FORMAT ")",
			     i + 1, arr_len, num_rows);
		}
	}

	if (num_rows <= 0) {
		return 0;
	}

	/* Check row limit — fall back to DuckDB if exceeded */
	if ((uint64_t)num_rows > info.data_inlining_row_limit) {
		return -1;
	}

	/* ----------------------------------------------------------------
	 * Phase 1: Setup — read table_id and ensure inlined data table exists
	 * These persist across retries (outside sub-transaction).
	 * ---------------------------------------------------------------- */
	/* Save old GUC value to restore later */
	const char *val = GetConfigOption("duckdb.force_execution", true, false);
	char *old_force_exec = val ? pstrdup(val) : NULL;
	SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);

	SPI_connect();

	int64_t table_id = ReadTableId(info.schema_name, info.table_name);
	if (table_id < 0) {
		SPI_finish();
		if (old_force_exec) {
			SetConfigOption("duckdb.force_execution", old_force_exec, PGC_USERSET, PGC_S_SESSION);
			pfree(old_force_exec);
		}
		elog(ERROR, "[PGDuckLake] Inline bypass: cannot find table_id for %s.%s", info.schema_name, info.table_name);
	}

	/* Read snapshot info for schema_version (needed for inlined table name) */
	int64_t schema_version, next_catalog_id, next_file_id;
	int64_t last_snapshot_id = ReadLatestSnapshot(&schema_version, &next_catalog_id, &next_file_id);
	if (last_snapshot_id < 0) {
		SPI_finish();
		if (old_force_exec) {
			SetConfigOption("duckdb.force_execution", old_force_exec, PGC_USERSET, PGC_S_SESSION);
			pfree(old_force_exec);
		}
		elog(ERROR, "[PGDuckLake] Inline bypass: cannot read snapshot info");
	}

	/* Find or create the inlined data table */
	char *inlined_table_name_spi = FindInlinedDataTable(table_id);
	if (!inlined_table_name_spi) {
		inlined_table_name_spi = CreateInlinedDataTable(info, table_id, schema_version);
		if (!inlined_table_name_spi) {
			SPI_finish();
			if (old_force_exec) {
				SetConfigOption("duckdb.force_execution", old_force_exec, PGC_USERSET, PGC_S_SESSION);
				pfree(old_force_exec);
			}
			elog(ERROR, "[PGDuckLake] Inline bypass: cannot create inlined data table");
		}
	}

	/* Copy inlined_table_name to caller context so it survives SPI_finish */
	MemoryContext spi_ctx = MemoryContextSwitchTo(caller_ctx);
	char *inlined_table_name = pstrdup(inlined_table_name_spi);
	MemoryContextSwitchTo(spi_ctx);

	SPI_finish();

	/* Build the INSERT query template (stable across retries) */
	/* We build this after SPI_finish to ensure relation access uses the parent resource owner */
	char *insert_query = BuildInsertQuery(info, inlined_table_name);

	/* ----------------------------------------------------------------
	 * Phase 2: Retry loop with sub-transactions
	 * Each attempt: read fresh snapshot/stats, insert data, create snapshot
	 * ---------------------------------------------------------------- */
	int64_t rows_inserted = 0;
	// bool success = false;
	// MemoryContext old_ctx = CurrentMemoryContext;

	/* Build SPI argument arrays (stable across retries, only values[0..1] change) */
	int nargs = 2 + num_cols;
	Oid *argtypes = (Oid *)palloc(sizeof(Oid) * nargs);
	Datum *values = (Datum *)palloc(sizeof(Datum) * nargs);
	char *nulls = (char *)palloc(sizeof(char) * nargs);

	argtypes[0] = INT8OID; /* start_row_id */
	argtypes[1] = INT8OID; /* begin_snapshot */
	nulls[0] = ' ';
	nulls[1] = ' ';
	for (int i = 0; i < num_cols; i++) {
		argtypes[i + 2] = info.array_types[i];
		values[i + 2] = array_datums[i];
		nulls[i + 2] = array_nulls[i] ? 'n' : ' ';
	}

	SPI_connect();

	// Single attempt without subtransaction
	{
		/* Read fresh snapshot and stats for this attempt */
		int64_t sv, nci, nfi;
		int64_t snap_id = ReadLatestSnapshot(&sv, &nci, &nfi);
		int64_t new_snapshot_id = snap_id + 1;

		int64_t record_count;
		int64_t next_row_id = ReadNextRowId(table_id, &record_count);

		/* Set dynamic SPI args */
		values[0] = Int64GetDatum(next_row_id);
		values[1] = Int64GetDatum(new_snapshot_id);

		/* Execute data insert */
		int ret = SPI_execute_with_args(insert_query, nargs, argtypes, values, nulls, false, 0);
		if (ret != SPI_OK_INSERT) {
			elog(ERROR, "[PGDuckLake] Inline bypass: data insert failed: %s", SPI_result_code_string(ret));
		}
		rows_inserted = SPI_processed;

		/* Create new snapshot */
		{
			StringInfoData snap_query;
			initStringInfo(&snap_query);
			appendStringInfo(&snap_query,
			                 "INSERT INTO %s.ducklake_snapshot VALUES (" INT64_FORMAT ", NOW(), " INT64_FORMAT
			                 ", " INT64_FORMAT ", " INT64_FORMAT ")",
			                 DUCKLAKE_METADATA_SCHEMA, new_snapshot_id, sv, nci, nfi);
			ret = SPI_execute(snap_query.data, false, 0);
			pfree(snap_query.data);
			if (ret != SPI_OK_INSERT) {
				elog(ERROR, "[PGDuckLake] Inline bypass: snapshot insert failed");
			}
		}

		/* Update table stats */
		{
			StringInfoData stats_query;
			initStringInfo(&stats_query);
			appendStringInfo(&stats_query,
			                 "UPDATE %s.ducklake_table_stats "
			                 "SET record_count = record_count + " INT64_FORMAT ", "
			                 "next_row_id = next_row_id + " INT64_FORMAT " "
			                 "WHERE table_id = " INT64_FORMAT,
			                 DUCKLAKE_METADATA_SCHEMA, rows_inserted, rows_inserted, table_id);
			ret = SPI_execute(stats_query.data, false, 0);
			pfree(stats_query.data);
			if (ret != SPI_OK_UPDATE) {
				elog(ERROR, "[PGDuckLake] Inline bypass: stats update failed");
			}
		}

		/* Record snapshot changes */
		{
			StringInfoData changes_query;
			initStringInfo(&changes_query);
			appendStringInfo(&changes_query,
			                 "INSERT INTO %s.ducklake_snapshot_changes "
			                 "VALUES (" INT64_FORMAT ", 'inlined_insert:" INT64_FORMAT "', NULL, NULL, NULL)",
			                 DUCKLAKE_METADATA_SCHEMA, new_snapshot_id, table_id);
			ret = SPI_execute(changes_query.data, false, 0);
			pfree(changes_query.data);
			if (ret != SPI_OK_INSERT) {
				elog(ERROR, "[PGDuckLake] Inline bypass: snapshot changes insert failed");
			}
		}
	}

	SPI_finish();

	if (old_force_exec) {
		SetConfigOption("duckdb.force_execution", old_force_exec, PGC_USERSET, PGC_S_SESSION);
		pfree(old_force_exec);
	}

	pfree(insert_query);
	pfree(argtypes);
	pfree(values);
	pfree(nulls);
	pfree(array_datums);
	pfree(array_nulls);

	return rows_inserted;
}

static void
InlineBypass_BeginCustomScan(CustomScanState *node, EState * /*estate*/, int /*eflags*/) {
	InlineBypassState *state = (InlineBypassState *)node;
	state->executed = false;
	state->rows_inserted = 0;
}

static TupleTableSlot *
InlineBypass_ExecCustomScan_Cpp(CustomScanState *node) {
	InlineBypassState *state = (InlineBypassState *)node;

	if (state->executed) {
		return nullptr;
	}

	state->executed = true;

	/* Get parameters from the executor state */
	ParamListInfo params = node->ss.ps.state->es_param_list_info;

	int64_t result = ExecuteInlineBypass(state, params);

	if (result < 0) {
		/* Fallback needed — row count exceeded limit.
		 * This shouldn't happen because we check at detection time,
		 * but the actual array length is only known at execution time. */
		elog(ERROR, "[PGDuckLake] Inline bypass: cannot fall back to DuckDB execution "
		            "from within the executor; row count may have exceeded the inlining limit");
	}

	state->rows_inserted = result;
	node->ss.ps.state->es_processed = result;

	return nullptr;
}

static TupleTableSlot *
InlineBypass_ExecCustomScan(CustomScanState *node) {
	return InvokeCPPFunc(InlineBypass_ExecCustomScan_Cpp, node);
}

static void
InlineBypass_EndCustomScan(CustomScanState * /*node*/) {
	/* Nothing to clean up */
}

static void
InlineBypass_ReScanCustomScan(CustomScanState *node) {
	InlineBypassState *state = (InlineBypassState *)node;
	state->executed = false;
}

static void
InlineBypass_ExplainCustomScan(CustomScanState *node, List * /*ancestors*/, ExplainState *es) {
	InlineBypassState *state = (InlineBypassState *)node;
	pg::ExplainPropertyText("DuckLake Inline Bypass", "true", es);
	pg::ExplainPropertyText("Target Table",
	                                  psprintf("%s.%s", state->info.schema_name, state->info.table_name), es);
}

PlannedStmt *
CreateInlineBypassPlan(const InlineBypassInfo &info, Query *query) {
	CustomScan *cscan = makeNode(CustomScan);

	/* Store the bypass info in custom_private */
	List *param_list = NIL;
	List *array_type_list = NIL;
	List *elem_type_list = NIL;

	for (size_t i = 0; i < info.param_indices.size(); i++) {
		param_list = lappend(param_list, makeInteger(info.param_indices[i]));
		array_type_list = lappend(array_type_list, makeInteger(info.array_types[i]));
		elem_type_list = lappend(elem_type_list, makeInteger(info.array_elem_types[i]));
	}

	cscan->custom_private =
	    list_make5(makeInteger(info.target_table_oid), makeInteger(info.data_inlining_row_limit),
	               makeString(pstrdup(info.schema_name)), makeString(pstrdup(info.table_name)), param_list);
	cscan->custom_private = lappend(cscan->custom_private, array_type_list);
	cscan->custom_private = lappend(cscan->custom_private, elem_type_list);

	cscan->methods = &inline_bypass_scan_methods;
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = NIL;

	/* Create a dummy RTE (like DuckDB scan does) to avoid perminfoindex issues */
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_NAMEDTUPLESTORE;
	rte->eref = makeAlias("ducklake_inline_bypass", NIL);
	rte->inFromCl = true;

	/* Create the PlannedStmt */
	PlannedStmt *result = makeNode(PlannedStmt);
	result->commandType = query->commandType;
	result->queryId = query->queryId;
	result->hasReturning = false;
	result->hasModifyingCTE = false;
	result->canSetTag = query->canSetTag;
	result->transientPlan = false;
	result->dependsOnRole = false;
	result->parallelModeNeeded = false;
	result->planTree = (Plan *)cscan;
	result->rtable = list_make1(rte);
#if PG_VERSION_NUM >= 160000
	result->permInfos = NULL;
#endif
	result->resultRelations = NULL;
	result->appendRelations = NULL;
	result->subplans = NIL;
	result->rewindPlanIDs = NULL;
	result->rowMarks = NIL;
	result->relationOids = NIL;
	result->invalItems = NIL;
	result->paramExecTypes = NIL;
	result->utilityStmt = query->utilityStmt;

	return result;
}

void
InitInlineBypassNode(void) {
	/* Setup scan methods */
	memset(&inline_bypass_scan_methods, 0, sizeof(inline_bypass_scan_methods));
	inline_bypass_scan_methods.CustomName = "DuckLakeInlineBypass";
	inline_bypass_scan_methods.CreateCustomScanState = InlineBypass_CreateCustomScanState;
	RegisterCustomScanMethods(&inline_bypass_scan_methods);

	/* Setup exec methods */
	memset(&inline_bypass_exec_methods, 0, sizeof(inline_bypass_exec_methods));
	inline_bypass_exec_methods.CustomName = "DuckLakeInlineBypass";
	inline_bypass_exec_methods.BeginCustomScan = InlineBypass_BeginCustomScan;
	inline_bypass_exec_methods.ExecCustomScan = InlineBypass_ExecCustomScan;
	inline_bypass_exec_methods.EndCustomScan = InlineBypass_EndCustomScan;
	inline_bypass_exec_methods.ReScanCustomScan = InlineBypass_ReScanCustomScan;
	inline_bypass_exec_methods.ExplainCustomScan = InlineBypass_ExplainCustomScan;
}

} // namespace pgduckdb
