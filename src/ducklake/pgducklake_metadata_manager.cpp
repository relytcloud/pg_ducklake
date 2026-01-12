#include "pgduckdb/ducklake/pgducklake_metadata_manager.hpp"

#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/allocator.hpp"

#include "common/ducklake_util.hpp"

#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pg/string_utils.hpp"
#include "pgduckdb/pgduckdb_detoast.hpp"
#include <duckdb/common/assert.hpp>

extern "C" {
#include "postgres.h"
#include "catalog/pg_namespace.h"
#include "access/table.h"
#include "utils/fmgroids.h"
#include "access/skey.h"
#include "catalog/pg_class_d.h"
#include "utils/syscache.h"
#include "access/genam.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "executor/spi.h"
}

/* Referenced from pgduckdb_xact.cpp */
namespace pgduckdb {
namespace pg {
CommandId GetCurrentCommandId(bool used = false);
}
void IncrementDuckLakeCommandId(CommandId inc);
} // namespace pgduckdb

namespace pgduckdb {
using namespace duckdb;

static StatementType
ConvertSPIResultToDuckStatementType(int result) {
	switch (result) {
	case SPI_OK_UTILITY:
		return StatementType::EXECUTE_STATEMENT;
	case SPI_OK_SELECT:
	case SPI_OK_SELINTO:
		return StatementType::SELECT_STATEMENT;
	case SPI_OK_INSERT:
	case SPI_OK_INSERT_RETURNING:
		return StatementType::INSERT_STATEMENT;
	case SPI_OK_DELETE:
	case SPI_OK_DELETE_RETURNING:
		return StatementType::DELETE_STATEMENT;
	case SPI_OK_UPDATE:
	case SPI_OK_UPDATE_RETURNING:
		return StatementType::UPDATE_STATEMENT;
	default:
		// For now, we should not use other types query in SPI.
		return StatementType::INVALID_STATEMENT;
	}
}

static void
InsertSPITupleTableIntoChunk(DataChunk &output, SPITupleTable *tuptable, idx_t start_idx, int num_tuples) {
	D_ASSERT(tuptable);
	D_ASSERT(start_idx + num_tuples <= tuptable->numvals);

	if (num_tuples == 0) {
		return;
	}

	int natts = tuptable->tupdesc->natts;

	for (int duckdb_output_index = 0; duckdb_output_index < natts; duckdb_output_index++) {
		auto &result = output.data[duckdb_output_index];
		auto attr = TupleDescAttr(tuptable->tupdesc, duckdb_output_index);

		for (int row = 0; row < num_tuples; row++) {
			HeapTuple tuple = tuptable->vals[start_idx + row];
			bool isnull = false;
			Datum datum = SPI_getbinval(tuple, tuptable->tupdesc, duckdb_output_index + 1, &isnull);
			if (isnull) {
				auto &array_mask = duckdb::FlatVector::Validity(result);
				array_mask.SetInvalid(start_idx + row);
			} else {
				if (attr->attlen == -1) {
					bool should_free = false;
					Datum detoasted_value = DetoastPostgresDatum(reinterpret_cast<varlena *>(datum), &should_free);
					ConvertPostgresToDuckValue(attr->atttypid, detoasted_value, result, start_idx + row);
					if (should_free) {
						duckdb_free(reinterpret_cast<void *>(detoasted_value));
					}
				} else {
					ConvertPostgresToDuckValue(attr->atttypid, datum, result, start_idx + row);
				}
			}
		}
	}
}

static unique_ptr<QueryResult>
CreateSPIResult(const string &query) {
	elog(DEBUG1, "Creating SPI result for query: %s", query.c_str());

	CommandId cid_before_commit = pg::GetCurrentCommandId();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	auto save_nestlevel = NewGUCNestLevel();
	SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);

	// Execute the query in read-only mode
	int ret = SPI_execute(query.c_str(), false, 0);

	if (ret < 0) {
		elog(ERROR, "SPI_execute failed: %s", SPI_result_code_string(ret));
	}

	// Get the result table
	SPITupleTable *tuptable = SPI_tuptable;
	if (!tuptable) {
		AtEOXact_GUC(false, save_nestlevel);
		PopActiveSnapshot();
		SPI_finish();
		IncrementDuckLakeCommandId(pg::GetCurrentCommandId() - cid_before_commit);

		// Return an empty result
		vector<string> names;
		StatementProperties properties;
		ClientProperties client_properties;

		// Create an empty ColumnDataCollection instead of passing nullptr
		auto &allocator = Allocator::DefaultAllocator();
		auto empty_collection = make_uniq<ColumnDataCollection>(allocator);

		return make_uniq<MaterializedQueryResult>(ConvertSPIResultToDuckStatementType(ret), properties, names,
		                                          std::move(empty_collection), client_properties);
	}

	TupleDesc tupdesc = tuptable->tupdesc;
	int num_columns = tupdesc->natts;
	uint64 num_rows = tuptable->numvals;

	// Convert column types and names
	vector<LogicalType> types;
	vector<string> names;

	for (int i = 0; i < num_columns; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		D_ASSERT(!attr->attisdropped);

		// Get column name
		names.push_back(NameStr(attr->attname));

		// Convert Postgres type to DuckDB type
		types.push_back(ConvertPostgresToDuckColumnType(attr));
	}

	// Create a ColumnDataCollection to store the results
	ClientProperties client_properties;
	auto &allocator = Allocator::DefaultAllocator();
	auto collection_p = make_uniq<ColumnDataCollection>(allocator, types);

	// Convert SPI rows to DuckDB DataChunks and append them
	for (idx_t row_idx = 0; row_idx < num_rows; row_idx += STANDARD_VECTOR_SIZE) {
		idx_t chunk_size = MinValue<int>(STANDARD_VECTOR_SIZE, num_rows - row_idx);
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(allocator, types, chunk_size);
		InsertSPITupleTableIntoChunk(*chunk, tuptable, row_idx, chunk_size);

		chunk->SetCardinality(chunk_size);
		collection_p->Append(*chunk);
	}

	AtEOXact_GUC(false, save_nestlevel);
	PopActiveSnapshot();
	SPI_finish();
	IncrementDuckLakeCommandId(pg::GetCurrentCommandId() - cid_before_commit);

	// Create and return the MaterializedQueryResult
	StatementProperties properties;
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, properties, names,
	                                          std::move(collection_p), client_properties);
}

PgDuckLakeMetadataManager::PgDuckLakeMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

PgDuckLakeMetadataManager::~PgDuckLakeMetadataManager() {
}

unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Query(string query) {
	DuckLakeMetadataManager::FillCatalogArgs(query, transaction.GetCatalog());
	DuckLakeMetadataManager::FillSnapshotCommitArgs(query, transaction.GetCommitInfo());
	// Execute the query using SPI and wrap the result
	return CreateSPIResult(query);
}

unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Query(DuckLakeSnapshot snapshot, duckdb::string query) {
	// Fill snapshot args into the query
	DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
	return Query(query);
}

unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Execute(DuckLakeSnapshot snapshot, duckdb::string &query) {
	// Fill snapshot args into the query
	DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
	return Query(query);
}

bool
PgDuckLakeMetadataManager::IsInitialized(duckdb::DuckLakeOptions & /* options */) {

	auto tup = SearchSysCache1(NAMESPACENAME, CStringGetDatum("ducklake"));

	if (!HeapTupleIsValid(tup))
		return false;

	auto nspoid = ((Form_pg_namespace)GETSTRUCT(tup))->oid;
	ReleaseSysCache(tup);

	auto rel = table_open(RelationRelationId, AccessShareLock);

	ScanKeyData scankey;

	ScanKeyInit(&scankey, Anum_pg_class_relnamespace, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(nspoid));

	auto scan = systable_beginscan(rel, ClassNameNspIndexId, /* pg_class_relname_nsp_index */
	                               true, NULL, 1, &scankey);

	bool found = false;

	while (HeapTupleIsValid(tup = systable_getnext(scan))) {
		Form_pg_class classForm = (Form_pg_class)GETSTRUCT(tup);
		const char *relname = NameStr(classForm->relname);

		/* Match LIKE 'ducklake_%' */
		if (StringHasPrefix(relname, "ducklake_") && classForm->relkind == RELKIND_RELATION) {
			found = true;
			break;
		}
	}

	systable_endscan(scan);
	table_close(rel, AccessShareLock);

	return found;
}

static bool
AddChildColumn(vector<duckdb::DuckLakeColumnInfo> &columns, duckdb::FieldIndex parent_id,
               DuckLakeColumnInfo &column_info) {
	for (auto &col : columns) {
		if (col.id == parent_id) {
			col.children.push_back(std::move(column_info));
			return true;
		}
		if (AddChildColumn(col.children, parent_id, column_info)) {
			return true;
		}
	}
	return false;
}

static vector<duckdb::DuckLakeTag>
LoadTags(const Value &tag_map) {

	static const LogicalType tags_type =
	    LogicalType::STRUCT({{"key", duckdb::LogicalType::VARCHAR}, {"value", duckdb::LogicalType::VARCHAR}});

	vector<duckdb::DuckLakeTag> result;
	for (auto &tag : ListValue::GetChildren(tag_map)) {
		auto tag_struct = tag.DefaultCastAs(tags_type);
		auto &struct_children = StructValue::GetChildren(tag);
		if (struct_children[1].IsNull()) {
			continue;
		}
		DuckLakeTag tag_info;
		tag_info.key = struct_children[0].ToString();
		tag_info.value = struct_children[1].ToString();
		result.push_back(std::move(tag_info));
	}
	return result;
}

static vector<duckdb::DuckLakeInlinedTableInfo>
LoadInlinedDataTables(const Value &list) {

	static const LogicalType val_type =
	    duckdb::LogicalType::STRUCT({{"name", LogicalType::VARCHAR}, {"schema_version", duckdb::LogicalType::BIGINT}});

	vector<duckdb::DuckLakeInlinedTableInfo> result;
	for (auto &val : ListValue::GetChildren(list)) {
		auto val_struct = val.DefaultCastAs(val_type);
		auto &struct_children = StructValue::GetChildren(val_struct);
		DuckLakeInlinedTableInfo inlined_data_table;
		inlined_data_table.table_name = StringValue::Get(struct_children[0]);
		inlined_data_table.schema_version = struct_children[1].GetValue<idx_t>();
		result.push_back(std::move(inlined_data_table));
	}
	return result;
}

duckdb::string
PgDuckLakeMetadataManager::CastStatsToTarget(const duckdb::string &stats, const duckdb::LogicalType &type) {
	if (type.IsNumeric()) {
		return "CAST(" + stats + " AS " + type.ToString() + ")";
	}
	return stats;
}

DuckLakeCatalogInfo
PgDuckLakeMetadataManager::GetCatalogForSnapshot(DuckLakeSnapshot snapshot) {
	auto &ducklake_catalog = transaction.GetCatalog();
	auto &base_data_path = ducklake_catalog.DataPath();
	DuckLakeCatalogInfo catalog;
	// load the schema information
	auto result = Query(snapshot, R"(
SELECT schema_id, schema_uuid::VARCHAR, schema_name, path, path_is_relative
FROM {METADATA_CATALOG}.ducklake_schema
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get schema information from DuckLake: ");
	}
	map<duckdb::SchemaIndex, idx_t> schema_map;
	for (auto &row : *result) {
		DuckLakeSchemaInfo schema;
		schema.id = SchemaIndex(row.GetValue<uint64_t>(0));
		schema.uuid = row.GetValue<string>(1);
		schema.name = row.GetValue<string>(2);
		if (row.IsNull(3)) {
			// no path provided - fallback to base data path
			schema.path = base_data_path;
		} else {
			// path is provided - load it
			DuckLakePath path;
			path.path = row.GetValue<string>(3);
			path.path_is_relative = row.GetValue<bool>(4);

			schema.path = FromRelativePath(path);
		}
		schema_map[schema.id] = catalog.schemas.size();
		catalog.schemas.push_back(std::move(schema));
	}

	// load the table information
	result = Query(snapshot, R"(
SELECT schema_id, tbl.table_id, table_uuid::VARCHAR, table_name,
	(
		SELECT array_agg(jsonb_build_object('key', key, 'value', value))
		FROM {METADATA_CATALOG}.ducklake_tag tag
		WHERE object_id=table_id AND
		      {SNAPSHOT_ID} >= tag.begin_snapshot AND ({SNAPSHOT_ID} < tag.end_snapshot OR tag.end_snapshot IS NULL)
	) AS tag,
	(
		SELECT array_agg(jsonb_build_object('name', table_name, 'schema_version', schema_version))
		FROM {METADATA_CATALOG}.ducklake_inlined_data_tables inlined_data_tables
		WHERE inlined_data_tables.table_id = tbl.table_id
	) AS inlined_data_tables,
	path, path_is_relative,
	col.column_id, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column,
	(
		SELECT array_agg(jsonb_build_object('key', key, 'value', value))
		FROM {METADATA_CATALOG}.ducklake_column_tag col_tag
		WHERE col_tag.table_id=tbl.table_id AND col_tag.column_id=col.column_id AND
		      {SNAPSHOT_ID} >= col_tag.begin_snapshot AND ({SNAPSHOT_ID} < col_tag.end_snapshot OR col_tag.end_snapshot IS NULL)
	) AS column_tags
FROM {METADATA_CATALOG}.ducklake_table tbl
LEFT JOIN {METADATA_CATALOG}.ducklake_column col USING (table_id)
WHERE {SNAPSHOT_ID} >= tbl.begin_snapshot AND ({SNAPSHOT_ID} < tbl.end_snapshot OR tbl.end_snapshot IS NULL)
  AND (({SNAPSHOT_ID} >= col.begin_snapshot AND ({SNAPSHOT_ID} < col.end_snapshot OR col.end_snapshot IS NULL)) OR column_id IS NULL)
ORDER BY table_id, parent_column NULLS FIRST, column_order
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get table information from DuckLake: ");
	}
	const idx_t COLUMN_INDEX_START = 8;
	auto &tables = catalog.tables;
	for (auto &row : *result) {
		auto table_id = TableIndex(row.GetValue<uint64_t>(1));

		// check if this column belongs to the current table or not
		if (tables.empty() || tables.back().id != table_id) {
			// new table
			DuckLakeTableInfo table_info;
			table_info.id = table_id;
			table_info.schema_id = SchemaIndex(row.GetValue<uint64_t>(0));
			table_info.uuid = row.GetValue<string>(2);
			table_info.name = row.GetValue<string>(3);
			if (!row.IsNull(4)) {
				auto tags = row.GetValue<Value>(4);
				table_info.tags = LoadTags(tags);
			}
			if (!row.IsNull(5)) {
				auto inlined_data_tables = row.GetValue<Value>(5);
				table_info.inlined_data_tables = LoadInlinedDataTables(inlined_data_tables);
			}
			// find the schema
			auto schema_entry = schema_map.find(table_info.schema_id);
			if (schema_entry == schema_map.end()) {
				throw InvalidInputException(
				    "Failed to load DuckLake - table with id %d references schema id %d that does not exist",
				    table_info.id.index, table_info.schema_id.index);
			}
			auto &schema = catalog.schemas[schema_entry->second];
			if (row.IsNull(6)) {
				// no path provided - fallback to schema path
				table_info.path = schema.path;
			} else {
				// path is provided - load it
				DuckLakePath path;
				path.path = row.GetValue<string>(6);
				path.path_is_relative = row.GetValue<bool>(7);

				table_info.path = FromRelativePath(path, schema.path);
			}
			tables.push_back(std::move(table_info));
		}
		auto &table_entry = tables.back();
		if (row.GetValue<Value>(COLUMN_INDEX_START).IsNull()) {
			throw InvalidInputException("Failed to load DuckLake - Table entry \"%s\" does not have any columns",
			                            table_entry.name);
		}
		DuckLakeColumnInfo column_info;
		column_info.id = FieldIndex(row.GetValue<uint64_t>(COLUMN_INDEX_START));
		column_info.name = row.GetValue<string>(COLUMN_INDEX_START + 1);
		column_info.type = row.GetValue<string>(COLUMN_INDEX_START + 2);
		if (!row.IsNull(COLUMN_INDEX_START + 3)) {
			column_info.initial_default = Value(row.GetValue<duckdb::string>(COLUMN_INDEX_START + 3));
		}
		if (!row.IsNull(COLUMN_INDEX_START + 4)) {
			column_info.default_value = Value(row.GetValue<duckdb::string>(COLUMN_INDEX_START + 4));
		}
		column_info.nulls_allowed = row.GetValue<bool>(COLUMN_INDEX_START + 5);
		if (!row.IsNull(COLUMN_INDEX_START + 7)) {
			auto tags = row.GetValue<Value>(COLUMN_INDEX_START + 7);
			column_info.tags = LoadTags(tags);
		}

		if (row.IsNull(COLUMN_INDEX_START + 6)) {
			// base column - add the column to this table
			table_entry.columns.push_back(std::move(column_info));
		} else {
			auto parent_id = FieldIndex(row.GetValue<idx_t>(COLUMN_INDEX_START + 6));
			if (!AddChildColumn(table_entry.columns, parent_id, column_info)) {
				throw InvalidInputException("Failed to load DuckLake - Could not find parent column for column %s",
				                            column_info.name);
			}
		}
	}
	// load view information
	result = Query(snapshot, R"(
SELECT view_id, view_uuid, schema_id, view_name, dialect, sql, column_aliases,
	(
		SELECT ARRAY_AGG(JSONB_BUILD_OBJECT('key', key, 'value', value))
		FROM {METADATA_CATALOG}.ducklake_tag tag
		WHERE object_id=view_id AND
		      {SNAPSHOT_ID} >= tag.begin_snapshot AND ({SNAPSHOT_ID} < tag.end_snapshot OR tag.end_snapshot IS NULL)
	) AS tag
FROM {METADATA_CATALOG}.ducklake_view view
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < view.end_snapshot OR view.end_snapshot IS NULL)
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get partition information from DuckLake: ");
	}
	auto &views = catalog.views;
	for (auto &row : *result) {
		DuckLakeViewInfo view_info;
		view_info.id = TableIndex(row.GetValue<uint64_t>(0));
		view_info.uuid = row.GetValue<string>(1);
		view_info.schema_id = SchemaIndex(row.GetValue<uint64_t>(2));
		view_info.name = row.GetValue<string>(3);
		view_info.dialect = row.GetValue<string>(4);
		view_info.sql = row.GetValue<string>(5);
		view_info.column_aliases = DuckLakeUtil::ParseQuotedList(row.GetValue<duckdb::string>(6));
		if (!row.IsNull(7)) {
			auto tags = row.GetValue<Value>(7);
			view_info.tags = LoadTags(tags);
		}
		views.push_back(std::move(view_info));
	}

	// load partition information
	result = Query(snapshot, R"(
SELECT partition_id, part.table_id, partition_key_index, column_id, transform
FROM {METADATA_CATALOG}.ducklake_partition_info part
JOIN {METADATA_CATALOG}.ducklake_partition_column part_col USING (partition_id)
WHERE {SNAPSHOT_ID} >= part.begin_snapshot AND ({SNAPSHOT_ID} < part.end_snapshot OR part.end_snapshot IS NULL)
ORDER BY part.table_id, partition_id, partition_key_index
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get partition information from DuckLake: ");
	}
	auto &partitions = catalog.partitions;
	for (auto &row : *result) {
		auto partition_id = row.GetValue<uint64_t>(0);
		auto table_id = TableIndex(row.GetValue<uint64_t>(1));

		if (partitions.empty() || partitions.back().table_id != table_id) {
			DuckLakePartitionInfo partition_info;
			partition_info.id = partition_id;
			partition_info.table_id = table_id;
			partitions.push_back(std::move(partition_info));
		}
		auto &partition_entry = partitions.back();

		DuckLakePartitionFieldInfo partition_field;
		partition_field.partition_key_index = row.GetValue<uint64_t>(2);
		partition_field.field_id = FieldIndex(row.GetValue<uint64_t>(3));
		partition_field.transform = row.GetValue<string>(4);
		partition_entry.fields.push_back(std::move(partition_field));
	}
	return catalog;
}

} // namespace pgduckdb
