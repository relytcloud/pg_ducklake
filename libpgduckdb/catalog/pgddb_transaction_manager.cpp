#include "pgddb/catalog/pgddb_transaction_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "pgddb/catalog/pgddb_transaction.hpp"
#include "pgddb/pg/snapshots.hpp"
#include "pgddb/pgddb_process_lock.hpp"
#include "pgddb/worker/duckdb_worker.hpp"

#include "duckdb/main/attached_database.hpp"

#include "pgddb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgddb {

PostgresTransactionManager::PostgresTransactionManager(duckdb::AttachedDatabase &_db_p, PostgresCatalog &_catalog)
    : TransactionManager(_db_p), catalog(_catalog), transaction_lock(), transactions() {
}

duckdb::Transaction &
PostgresTransactionManager::StartTransaction(duckdb::ClientContext &context) {
	// In the PG-free worker (a worker session is active) there is no active snapshot; the
	// transaction's snapshot is unused there (catalog is RPC'd, heap scans are inverted),
	// so avoid GetActiveSnapshot() which asserts without one. Context-resolved because a
	// nested query can start this transaction on a DuckDB scheduler thread.
	Snapshot snap = pgddb::worker::EffectiveWorkerSession(&context) ? nullptr : GetActiveSnapshot();
	auto transaction = duckdb::make_uniq<PostgresTransaction>(*this, context, catalog, snap);
	auto &result = *transaction;
	duckdb::lock_guard<duckdb::mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

duckdb::ErrorData
PostgresTransactionManager::CommitTransaction(duckdb::ClientContext &context, duckdb::Transaction &transaction) {
	duckdb::lock_guard<duckdb::mutex> l(transaction_lock);
	ClosePostgresRelations(context);
	transactions.erase(transaction);
	return duckdb::ErrorData();
}

void
PostgresTransactionManager::RollbackTransaction(duckdb::Transaction &transaction) {
	duckdb::lock_guard<duckdb::mutex> l(transaction_lock);
	duckdb::shared_ptr<duckdb::ClientContext> context = transaction.context.lock();
	if (context) {
		ClosePostgresRelations(*context);
	}
	transactions.erase(transaction);
}

void
PostgresTransactionManager::Checkpoint(duckdb::ClientContext &, bool /*force*/) {
}

} // namespace pgddb
