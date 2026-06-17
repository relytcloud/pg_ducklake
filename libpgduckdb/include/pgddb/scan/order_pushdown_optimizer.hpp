#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace pgddb {

// Pushes ORDER BY / Top-N into the Postgres scan when a btree index can produce
// the requested ordering. Registered on the kernel DuckDB instance, so every
// extension built on libpgduckdb gets it. OptimizerExtension is a by-value
// struct of callbacks; this subclass just wires optimize_function in its ctor.
class PostgresOrderPushdownOptimizer : public duckdb::OptimizerExtension {
public:
	PostgresOrderPushdownOptimizer();
};

} // namespace pgddb
