#include "pgduckdb/ducklake/pgducklake_metadata_manager.hpp"
#include "pgduckdb/pg/string_utils.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include <catalog/pg_class_d.h>
#include <duckdb/common/enums/statement_type.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/materialized_query_result.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/allocator.hpp>
#include <utils/fmgroids.h>

extern "C" {
#include "postgres.h"
#include <catalog/pg_namespace.h>
#include <access/table.h>
#include <access/skey.h>
#include "utils/syscache.h"
#include "access/genam.h"
#include "utils/elog.h"
#include "executor/spi.h"
#include "utils/tuplesort.h"
#include "access/tupdesc_details.h"
}

namespace pgduckdb {

namespace {

static duckdb::StatementType
ConvertSPIStatementTypeToDuckDB(int result) {
	switch (result) {
	case SPI_OK_UTILITY:
		return duckdb::StatementType::EXECUTE_STATEMENT;
	case SPI_OK_SELECT:
	case SPI_OK_SELINTO:
		return duckdb::StatementType::SELECT_STATEMENT;
	case SPI_OK_INSERT:
	case SPI_OK_INSERT_RETURNING:
		return duckdb::StatementType::INSERT_STATEMENT;
	case SPI_OK_DELETE:
	case SPI_OK_DELETE_RETURNING:
		return duckdb::StatementType::DELETE_STATEMENT;
	case SPI_OK_UPDATE:
	case SPI_OK_UPDATE_RETURNING:
		return duckdb::StatementType::UPDATE_STATEMENT;
	default:
		// For now, we should not use other types query in SPI.
		return duckdb::StatementType::INVALID_STATEMENT;
	}
}

// Helper class to create SPI result
class SPI_Result_Helper {
public:
	static duckdb::unique_ptr<duckdb::QueryResult>
	Create(const duckdb::string &query) {
		elog(INFO, "Creating SPI result for query: %s", query.c_str());
		// Connect to SPI
		int SPI_connected = SPI_connect();
		if (SPI_connected != SPI_OK_CONNECT) {
			return duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::ErrorData("Failed to connect to SPI"));
		}

		// Execute the query in read-only mode
		int ret = SPI_execute(query.c_str(), false, 0);

		if (ret < 0) {
			SPI_finish();
			return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
			    duckdb::ErrorData(duckdb::StringUtil::Format("SPI_execute failed: %s", SPI_result_code_string(ret))));
		}

		// Get the result table
		SPITupleTable *tuptable = SPI_tuptable;
		if (!tuptable) {
			// No results returned (e.g., from a utility command)
			SPI_finish();

			// Return an empty result
			duckdb::vector<duckdb::LogicalType> types;
			duckdb::vector<duckdb::string> names;
			duckdb::StatementProperties properties;
			duckdb::ClientProperties client_properties;

			return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
			    ConvertSPIStatementTypeToDuckDB(ret), properties, names,
			    duckdb::unique_ptr<duckdb::ColumnDataCollection>(), client_properties);
		}

		TupleDesc tupdesc = tuptable->tupdesc;
		int num_columns = tupdesc->natts;
		uint64 num_rows = tuptable->numvals;

		// Convert column types and names
		duckdb::vector<duckdb::LogicalType> types;
		duckdb::vector<duckdb::string> names;

		for (int i = 0; i < num_columns; i++) {
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped) {
				continue;
			}

			// Get column name
			names.push_back(NameStr(attr->attname));

			// Convert Postgres type to DuckDB type
			types.push_back(ConvertPostgresToDuckColumnType(attr));
		}

		// Create a ColumnDataCollection to store the results
		duckdb::ClientProperties client_properties;
		auto &allocator = duckdb::Allocator::DefaultAllocator();
		auto collection_p = duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator, types);

		// Convert SPI rows to DuckDB DataChunks and append them
		for (duckdb::idx_t row_idx = 0; row_idx < num_rows; row_idx += STANDARD_VECTOR_SIZE) {
			duckdb::idx_t chunk_size = duckdb::MinValue<duckdb::idx_t>(STANDARD_VECTOR_SIZE, num_rows - row_idx);
			auto chunk = ConvertSPIRowToDataChunk(tuptable, row_idx, chunk_size, types);

			if (chunk) {
				collection_p->Append(*chunk);
			}
		}

		// Disconnect from SPI
		SPI_finish();

		// Create and return the MaterializedQueryResult
		duckdb::StatementProperties properties;
		return duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::StatementType::SELECT_STATEMENT, properties,
		                                                          names, std::move(collection_p), client_properties);
	}

private:
	static duckdb::unique_ptr<duckdb::DataChunk>
	ConvertSPIRowToDataChunk(SPITupleTable *tuptable, duckdb::idx_t start_idx, duckdb::idx_t count,
	                         const duckdb::vector<duckdb::LogicalType> &types);
	static duckdb::LogicalType ConvertPostgresTypeToDuckDB(Oid postgres_type);
};

duckdb::unique_ptr<duckdb::DataChunk>
SPI_Result_Helper::ConvertSPIRowToDataChunk(SPITupleTable *tuptable, duckdb::idx_t start_idx, duckdb::idx_t count,
                                            const duckdb::vector<duckdb::LogicalType> &types) {
	if (!tuptable || !tuptable->tupdesc) {
		return nullptr;
	}

	TupleDesc tupdesc = tuptable->tupdesc;
	int num_columns = tupdesc->natts;

	// Determine the number of valid (non-dropped) columns
	duckdb::vector<int> valid_columns;
	for (int i = 0; i < num_columns; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (!attr->attisdropped) {
			valid_columns.push_back(i);
		}
	}

	if (valid_columns.empty()) {
		return nullptr;
	}

	// Create a DataChunk
	auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
	auto &allocator = duckdb::Allocator::DefaultAllocator();
	chunk->Initialize(allocator, types, count);

	// Fill the DataChunk with data
	duckdb::idx_t output_col = 0;
	for (int col_idx : valid_columns) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, col_idx);
		auto &result_vector = chunk->data[output_col];

		for (duckdb::idx_t row = 0; row < count; row++) {
			HeapTuple tuple = tuptable->vals[start_idx + row];
			bool isnull = false;
			Datum datum = SPI_getbinval(tuple, tupdesc, col_idx + 1, &isnull);

			if (isnull) {
				auto &validity = duckdb::FlatVector::Validity(result_vector);
				validity.SetInvalid(row);
			} else {
				// Convert Postgres Datum to DuckDB Value
				ConvertPostgresToDuckValue(attr->atttypid, datum, result_vector, row);
			}
		}
		output_col++;
	}

	chunk->Verify();
	return chunk;
}

} // anonymous namespace

PgDuckLakeMetadataManager::PgDuckLakeMetadataManager(duckdb::DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

PgDuckLakeMetadataManager::~PgDuckLakeMetadataManager() {
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Query(duckdb::string query) {
	DuckLakeMetadataManager::FillCatalogArgs(query, transaction.GetCatalog());
	DuckLakeMetadataManager::FillSnapshotCommitArgs(query, transaction.GetCommitInfo());
	// Execute the query using SPI and wrap the result
	return SPI_Result_Helper::Create(query);
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Query(duckdb::DuckLakeSnapshot snapshot, duckdb::string query) {
	// Fill snapshot args into the query
	DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
	return Query(query);
}

duckdb::unique_ptr<duckdb::QueryResult>
PgDuckLakeMetadataManager::Execute(duckdb::DuckLakeSnapshot snapshot, duckdb::string &query) {
	// Fill snapshot args into the query
	DuckLakeMetadataManager::FillSnapshotArgs(query, snapshot);
	return Query(query);
}

bool
PgDuckLakeMetadataManager::IsInitialized() {

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

} // namespace pgduckdb
