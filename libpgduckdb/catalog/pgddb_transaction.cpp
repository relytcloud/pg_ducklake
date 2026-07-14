#include "pgddb/catalog/pgddb_catalog.hpp"
#include "pgddb/catalog/pgddb_schema.hpp"
#include "pgddb/catalog/pgddb_transaction.hpp"
#include "pgddb/catalog/pgddb_table.hpp"
#include "pgddb/catalog/relation_desc.hpp"
#include "pgddb/scan/postgres_scan.hpp"
#include "pgddb/pg/relations.hpp"
#include "pgddb/worker/duckdb_worker.hpp"
#include "pgddb/worker/transport/session_channel.hpp"
#include "pgddb/worker/transport/session_protocol.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"

#include "pgddb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgddb {
void
ClosePostgresRelations(duckdb::ClientContext &context) {
	auto context_state = context.registered_state->GetOrCreate<PostgresContextState>("pgduckdb");
	context_state->QueryEnd();
}

PostgresTransaction::PostgresTransaction(duckdb::TransactionManager &_manager, duckdb::ClientContext &_context,
                                         PostgresCatalog &_catalog, Snapshot _snapshot)
    : duckdb::Transaction(_manager, _context), catalog(_catalog), snapshot(_snapshot) {
}

PostgresTransaction::~PostgresTransaction() {
}

SchemaItems::SchemaItems(duckdb::unique_ptr<PostgresSchema> &&_schema, const duckdb::string &_name)
    : name(_name), schema(std::move(_schema)), tables() {
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SchemaItems::GetTable(const duckdb::string &entry_name, duckdb::ClientContext *context) {
	auto it = tables.find(entry_name);
	if (it != tables.end()) {
		return it->second.get();
	}

	// In a duckdb worker session, the relation lives in the requesting backend; RPC it to
	// describe the relation rather than opening it here. Resolved through the context as
	// well: a nested query (e.g. DuckLake inlined-data reads) binds on a DuckDB scheduler
	// thread, where the session thread-local is invisible.
	if (auto *session = pgddb::worker::EffectiveWorkerSession(context)) {
		RelationDesc desc = pgddb::WorkerDescribeRelation(*session, name, entry_name);
		if (desc.oid == 0 /* InvalidOid */) {
			return nullptr; // Table could not be found
		}
		duckdb::CreateTableInfo info;
		info.table = entry_name;
		PostgresTable::SetTableInfo(info, desc);
		tables.emplace(entry_name, duckdb::make_uniq<PostgresTable>(schema->catalog, *schema, info, std::move(desc),
		                                                            schema->snapshot));
		return tables[entry_name].get();
	}

	Oid rel_oid = pgddb::GetRelidFromSchemaAndTable(name.c_str(), entry_name.c_str());

	if (!pgddb::IsValidOid(rel_oid)) {
		return nullptr; // Table could not be found
	}

	Relation rel = PostgresTable::OpenRelation(rel_oid);
	RelationDesc desc = pgddb::BuildRelationDesc(rel);
	PostgresTable::CloseRelation(rel);

	duckdb::CreateTableInfo info;
	info.table = entry_name;
	PostgresTable::SetTableInfo(info, desc);

	tables.emplace(entry_name,
	               duckdb::make_uniq<PostgresTable>(schema->catalog, *schema, info, std::move(desc), schema->snapshot));
	return tables[entry_name].get();
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SchemaItems::GetSchema() const {
	return schema.get();
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresTransaction::GetSchema(const duckdb::string &name) {
	auto context_state = context.lock()->registered_state->GetOrCreate<PostgresContextState>("pgduckdb");
	auto schemas = &context_state->schemas;
	auto it = schemas->find(name);
	if (it != schemas->end()) {
		return it->second.GetSchema();
	}

	duckdb::CreateSchemaInfo create_schema;
	create_schema.schema = name;
	auto pg_schema = duckdb::make_uniq<PostgresSchema>(catalog, create_schema, snapshot);
	schemas->emplace(std::make_pair(name, SchemaItems(std::move(pg_schema), name)));
	return schemas->at(name).GetSchema();
}

PostgresContextState::PostgresContextState() : duckdb::ClientContextState(), schemas() {
}

void
PostgresContextState::QueryEnd() {
	schemas.clear();
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PostgresTransaction::GetCatalogEntry(duckdb::CatalogType type, const duckdb::string &schema,
                                     const duckdb::string &name) {
	switch (type) {
	case duckdb::CatalogType::TABLE_ENTRY: {
		auto ctx = context.lock();
		auto context_state = ctx->registered_state->GetOrCreate<PostgresContextState>("pgduckdb");
		auto schemas = &context_state->schemas;
		auto it = schemas->find(schema);
		if (it == schemas->end()) {
			return nullptr;
		}

		auto &schema_entry = it->second;
		return schema_entry.GetTable(name, ctx.get());
	}
	case duckdb::CatalogType::SCHEMA_ENTRY: {
		return GetSchema(schema);
	}
	default:
		return nullptr;
	}
}

} // namespace pgddb
