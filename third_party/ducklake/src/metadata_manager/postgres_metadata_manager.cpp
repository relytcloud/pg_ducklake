#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "yyjson.hpp"

namespace duckdb {

PostgresMetadataManager::PostgresMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

bool PostgresMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	// Unnamed composite types are not supported.
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	// Postgres timestamp/date ranges are narrower than DuckDB's
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	// Postgres bytea input format differs from DuckDB's blob text format
	case LogicalTypeId::BLOB:
	// Postgres cannot store null bytes in VARCHAR/TEXT columns
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::VARIANT:
	// If we knew that the Postgres installation has PostGIS installed, we could support GEOMETRY in the future.
	case LogicalTypeId::GEOMETRY:
		return false;
	default:
		return true;
	}
}

bool PostgresMetadataManager::SupportsInlining(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARIANT) {
		return false;
	}
	return DuckLakeMetadataManager::SupportsInlining(type);
}

string PostgresMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::TINYINT:
		return "SMALLINT";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return "INTEGER";
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return "BYTEA";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return "VARCHAR";
	default:
		return column_type.ToString();
	}
}

bool PostgresMetadataManager::InlinedDeletionTableExists(const string &table_name, DuckLakeSnapshot snapshot) {
	// The base class checks existence with "SELECT NULL FROM table LIMIT 1",
	// which aborts the Postgres transaction when the table doesn't exist.
	// Use an information_schema query instead so the transaction stays valid.
	auto query = StringUtil::Format(R"(
SELECT 1 FROM information_schema.tables
WHERE table_schema = {METADATA_SCHEMA_NAME_LITERAL} AND table_name = '%s'
)",
	                                table_name);
	auto result = Query(snapshot, query);
	if (result->HasError()) {
		return false;
	}
	for (auto &row : *result) {
		(void)row;
		return true;
	}
	return false;
}

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteQuery(DuckLakeSnapshot snapshot, string &query,
                                                              string command) {
	FillSnapshotArgs(query, snapshot);
	FillSnapshotCommitArgs(query, transaction.GetCommitInfo());

	auto &connection = transaction.GetConnection();
	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataDatabaseName());
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	auto schema_identifier_escaped = StringUtil::Replace(schema_identifier, "'", "''");
	auto schema_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataSchemaName());
	auto metadata_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataPath());
	auto data_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.DataPath());

	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}", catalog_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}", catalog_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}", schema_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}", schema_identifier_escaped);
	query = StringUtil::Replace(query, "{METADATA_PATH}", metadata_path);
	query = StringUtil::Replace(query, "{DATA_PATH}", data_path);

	return connection.Query(StringUtil::Format("CALL %s(%s, %s)", command, catalog_literal, SQLString(query)));
}

unique_ptr<QueryResult> PostgresMetadataManager::Execute(DuckLakeSnapshot snapshot, string query) {
	return ExecuteQuery(snapshot, query, "postgres_execute");
}

unique_ptr<QueryResult> PostgresMetadataManager::Query(DuckLakeSnapshot snapshot, string query) {
	return ExecuteQuery(snapshot, query, "postgres_query");
}

string PostgresMetadataManager::GetLatestSnapshotQuery() const {
	return R"(
		SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
		FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot WHERE snapshot_id = (
		    SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
		);
	)";
}

string PostgresMetadataManager::ListAggregation(const vector<pair<string, string>> &fields) const {
	string fields_part;
	for (auto const &entry : fields) {
		if (!fields_part.empty()) {
			fields_part += ", ";
		}
		fields_part += "'" + entry.first + "', " + entry.second;
	}
	return "json_agg(json_build_object(" + fields_part + "))";
}

string PostgresMetadataManager::CastStatsToTarget(const string &stats, const LogicalType &type) {
	// PostgreSQL doesn't have TRY_CAST, use regular CAST with :: operator
	// For numeric types, we cast directly; stats should be valid numeric values
	if (type.IsNumeric()) {
		return "(" + stats + " :: " + GetColumnTypeInternal(type) + ")";
	}
	return stats;
}

string PostgresMetadataManager::CastColumnToTarget(const string &stats, const LogicalType &type) {
	// If not a pg native type, defer type cast to TransformInlinedData.
	if (!TypeIsNativelySupported(type)) {
		return stats;
	}
	// Also for nested types
	if (type.id() == LogicalTypeId ::ARRAY) {
		LogicalType child_type = ArrayType::GetChildType(type);

		while (child_type.id() == LogicalTypeId::ARRAY) {
			child_type = ArrayType::GetChildType(child_type);
		}
		if (!TypeIsNativelySupported(child_type)) {
			return stats;
		}
	}
	return stats + "::" + GetColumnTypeInternal(type);
}

vector<DuckLakeTag> PostgresMetadataManager::LoadTags(const Value &tag_map) const {
	using namespace duckdb_yyjson; // NOLINT

	vector<DuckLakeTag> result;

	auto tags_json = tag_map.GetValue<string>();
	auto doc = yyjson_read(tags_json.c_str(), tags_json.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse tags JSON");
	}
	auto root = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(root)) {
		yyjson_doc_free(doc);
		throw InvalidInputException("Invalid tags JSON: expected array");
	}

	idx_t idx, n_tag;
	yyjson_val *tag_json;
	yyjson_arr_foreach(root, idx, n_tag, tag_json) {
		if (!yyjson_is_obj(tag_json)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid tags JSON: expected object");
		}

		DuckLakeTag tag;

		auto key = yyjson_obj_get(tag_json, "key");
		auto value = yyjson_obj_get(tag_json, "value");
		if (!yyjson_is_str(key)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid tags JSON: missing required fields");
		}

		tag.key = yyjson_get_str(key);

		// Value can be null or string - skip null values
		if (yyjson_is_null(value)) {
			continue;
		}
		if (!yyjson_is_str(value)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid tags JSON: value must be string or null");
		}
		tag.value = yyjson_get_str(value);

		result.push_back(std::move(tag));
	}
	yyjson_doc_free(doc);
	return result;
}

vector<DuckLakeInlinedTableInfo> PostgresMetadataManager::LoadInlinedDataTables(const Value &list) const {
	using namespace duckdb_yyjson; // NOLINT

	vector<DuckLakeInlinedTableInfo> result;

	auto json_str = list.GetValue<string>();
	auto doc = yyjson_read(json_str.c_str(), json_str.size(), 0);
	if (!doc) {
		throw InvalidInputException("Failed to parse inlined data tables JSON");
	}
	auto root = yyjson_doc_get_root(doc);
	if (!yyjson_is_arr(root)) {
		yyjson_doc_free(doc);
		throw InvalidInputException("Invalid inlined data tables JSON: expected array");
	}

	idx_t idx, n_tables;
	yyjson_val *table_json;
	yyjson_arr_foreach(root, idx, n_tables, table_json) {
		if (!yyjson_is_obj(table_json)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid inlined data tables JSON: expected object");
		}

		auto table_name = yyjson_obj_get(table_json, "name");
		auto schema_version = yyjson_obj_get(table_json, "schema_version");
		if (!yyjson_is_str(table_name) || !yyjson_is_num(schema_version)) {
			yyjson_doc_free(doc);
			throw InvalidInputException("Invalid inlined data tables JSON: missing required fields");
		}

		DuckLakeInlinedTableInfo info;
		info.table_name = yyjson_get_str(table_name);
		info.schema_version = yyjson_get_uint(schema_version);
		result.push_back(std::move(info));
	}
	yyjson_doc_free(doc);
	return result;
}

// Postgres metadata manager needs specialized TransformInlinedData:
// 1. BLOB->VARCHAR reinterpret (Postgres stores VARCHAR as BYTEA)
// 2. VARCHAR->DuckDB type cast (non-native types stored as VARCHAR in Postgres)
shared_ptr<DuckLakeInlinedData>
PostgresMetadataManager::TransformInlinedData(QueryResult &result, const vector<LogicalType> &expected_types) {
	bool needs_transform = false;
	if (!expected_types.empty()) {
		D_ASSERT(expected_types.size() == result.types.size());
		for (idx_t i = 0; i < expected_types.size(); i++) {
			if (result.types[i] != expected_types[i]) {
				needs_transform = true;
				break;
			}
		}
	}
	if (!needs_transform) {
		return DuckLakeMetadataManager::TransformInlinedData(result, expected_types);
	}

	if (result.HasError()) {
		result.GetErrorObject().Throw("Failed to read inlined data from DuckLake: ");
	}

	// Transform the result by casting vectors to their expected types
	auto context = transaction.context.lock();
	auto data = make_uniq<ColumnDataCollection>(*context, expected_types);

	while (true) {
		auto chunk = result.Fetch();
		if (!chunk) {
			break;
		}

		// Create a new chunk with the expected types
		DataChunk casted_chunk;
		casted_chunk.Initialize(*context, expected_types, chunk->size());
		casted_chunk.SetCardinality(chunk->size());

		// Cast each column to its expected type
		for (idx_t col_idx = 0; col_idx < chunk->ColumnCount(); col_idx++) {
			auto &source_vector = chunk->data[col_idx];
			auto &target_vector = casted_chunk.data[col_idx];

			if (source_vector.GetType() == target_vector.GetType()) {
				// No casting needed, just copy
				target_vector.Reference(source_vector);
			} else if (source_vector.GetType().id() == LogicalTypeId::BLOB &&
			           target_vector.GetType().id() == LogicalTypeId::VARCHAR) {
				// BLOB->VARCHAR: same binary representation, use Reinterpret
				target_vector.Reinterpret(source_vector);
			} else {
				// General case: cast from source to expected type
				VectorOperations::Cast(*context, source_vector, target_vector, chunk->size());
			}
		}

		data->Append(casted_chunk);
	}

	auto inlined_data = make_shared_ptr<DuckLakeInlinedData>();
	inlined_data->data = std::move(data);
	return inlined_data;
}

} // namespace duckdb
