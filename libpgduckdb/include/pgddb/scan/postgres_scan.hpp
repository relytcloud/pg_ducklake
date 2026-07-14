#pragma once

#include "duckdb.hpp"

#include "duckdb/common/enums/order_type.hpp"

#include "pgddb/catalog/relation_desc.hpp"
#include "pgddb/pg/declarations.hpp"
#include "pgddb/utility/allocator.hpp"

#include "pgddb/scan/postgres_table_reader.hpp"
#include "pgddb/worker/worker_dispatch.hpp"

#include "pgddb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgddb {

struct PostgresScanGlobalState : public duckdb::GlobalTableFunctionState {
	PostgresScanGlobalState(duckdb::ClientContext &context, Snapshot, const RelationDesc &desc,
	                        const duckdb::TableFunctionInitInput &input);
	~PostgresScanGlobalState();
	idx_t
	MaxThreads() const override {
		return max_threads;
	}
	void ConstructTableScanQuery(const duckdb::TableFunctionInitInput &input);
	bool RegisterLocalState();
	void UnregisterLocalState();

private:
	int ExtractQueryFilters(duckdb::TableFilter *filter, const char *column_name, duckdb::string &filters,
	                        bool is_optional_filter_parent);
	PostgresScanGlobalState(const PostgresScanGlobalState &) = delete;
	PostgresScanGlobalState &operator=(const PostgresScanGlobalState &) = delete;

public:
	Snapshot snapshot;
	const RelationDesc &desc;
	bool count_tuples_only;
	duckdb::vector<AttrNumber> output_columns;
	std::atomic<std::uint32_t> total_row_count;
	std::atomic<std::int32_t> registered_local_states;
	std::ostringstream scan_query;
	// Byte offset in scan_query right after "FROM <relation>", where a remote scan-pool
	// producer splices a CTID range predicate. 0 until ConstructTableScanQuery runs.
	uint32_t remote_where_off = 0;
	duckdb::shared_ptr<PostgresTableReader> table_reader_global_state;
	MemoryContext duckdb_scan_memory_ctx;
	idx_t max_threads;
	// When set (worker session), the scan runs on the requesting backend and yields
	// DataChunks; table_reader_global_state stays null and no PG is touched here.
	duckdb::unique_ptr<RemoteScanStream> remote_scan;
};

#define LOCAL_STATE_SLOT_BATCH_SIZE 32
struct PostgresScanLocalState : public duckdb::LocalTableFunctionState {
	PostgresScanLocalState(PostgresScanGlobalState *global_state);
	~PostgresScanLocalState() override;

	PostgresScanGlobalState *global_state;
	TupleTableSlot *slots[LOCAL_STATE_SLOT_BATCH_SIZE];
	std::vector<uint8_t> minimal_tuple_buffer[LOCAL_STATE_SLOT_BATCH_SIZE];

	size_t output_vector_size;
	bool exhausted_scan;

private:
	PostgresScanLocalState(const PostgresScanLocalState &) = delete;
	PostgresScanLocalState &operator=(const PostgresScanLocalState &) = delete;
};

struct PostgresOrderBySpec {
	PostgresOrderBySpec()
	    : column_index(0), order_type(duckdb::OrderType::ASCENDING), null_order(duckdb::OrderByNullType::ORDER_DEFAULT),
	      column_name() {
	}
	duckdb::idx_t column_index;
	duckdb::OrderType order_type;
	duckdb::OrderByNullType null_order;
	duckdb::string column_name;
};

struct PostgresScanFunctionData : public duckdb::TableFunctionData {
	PostgresScanFunctionData(RelationDesc desc, Snapshot snapshot);
	~PostgresScanFunctionData() override;
	duckdb::vector<duckdb::string> complex_filters;
	duckdb::vector<PostgresOrderBySpec> order_bys;
	// Set when a Top-N (ORDER BY + LIMIT) is pushed: emit LIMIT/OFFSET in the scan query.
	duckdb::optional_idx limit;
	duckdb::idx_t offset;
	RelationDesc desc;
	Snapshot snapshot;

private:
	PostgresScanFunctionData(const PostgresScanFunctionData &) = delete;
	PostgresScanFunctionData &operator=(const PostgresScanFunctionData &) = delete;
};

struct PostgresScanTableFunction : public duckdb::TableFunction {
	PostgresScanTableFunction();

	static duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
	PostgresScanInitGlobal(duckdb::ClientContext &context, duckdb::TableFunctionInitInput &input);

	static duckdb::unique_ptr<duckdb::LocalTableFunctionState>
	PostgresScanInitLocal(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
	                      duckdb::GlobalTableFunctionState *gstate);

	static void PostgresScanFunction(duckdb::ClientContext &context, duckdb::TableFunctionInput &data,
	                                 duckdb::DataChunk &output);

	static duckdb::unique_ptr<duckdb::NodeStatistics> PostgresScanCardinality(duckdb::ClientContext &context,
	                                                                          const duckdb::FunctionData *data);
	static duckdb::InsertionOrderPreservingMap<duckdb::string> ToString(duckdb::TableFunctionToStringInput &input);
};

extern int duckdb_threads_for_postgres_scan;

} // namespace pgddb
