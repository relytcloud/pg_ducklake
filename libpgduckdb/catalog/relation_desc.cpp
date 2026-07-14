#include "pgddb/catalog/relation_desc.hpp"

#include "pgddb/pg/relations.hpp"
#include "pgddb/pgddb_process_lock.hpp"
#include "pgddb/pgddb_types.hpp" // ConvertPostgresToDuckColumnType
#include "pgddb/worker/transport/session_channel.hpp"
#include "pgddb/worker/transport/session_protocol.hpp"

#include <cstring>
#include <exception>
#include <string>

#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types.hpp>

extern "C" {
#include "postgres.h"

#include "utils/memutils.h" // TopMemoryContext
#include "utils/rel.h"      // RelationGetRelid
}

namespace pgddb {

#undef RelationGetDescr

RelationDesc
BuildRelationDesc(Relation rel) {
	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());

	RelationDesc desc;
	desc.oid = RelationGetRelid(rel);
	desc.qualified_name = pgddb::GenerateQualifiedRelationName(rel);
	desc.name = pgddb::GetRelationName(rel);
	desc.is_temporary = pgddb::RelationIsTemporary(rel);
	desc.cardinality = pgddb::EstimateRelSize(rel);
	// Only relations with physical storage have a main-fork block count; a partitioned
	// parent (or other storage-less relkind) would crash RelationGetNumberOfBlocksInFork.
	if (RELKIND_HAS_TABLE_AM(rel->rd_rel->relkind)) {
		desc.nblocks = pgddb::RelationBlockCount(rel);
	}

	auto tuple_desc = pgddb::RelationGetDescr(rel);
	const auto natts = pgddb::GetTupleDescNatts(tuple_desc);
	for (int i = 0; i < natts; ++i) {
		Form_pg_attribute attr = pgddb::GetAttr(tuple_desc, i);
		if (pgddb::AttIsDropped(attr)) {
			continue;
		}
		RelationColumn column;
		column.attno = static_cast<AttrNumber>(i + 1);
		column.name = pgddb::GetAttName(attr);
		column.type = ConvertPostgresToDuckColumnType(attr);
		desc.columns.emplace_back(std::move(column));
	}

	return desc;
}

void
ServeDescribeRelation(SessionChannel &channel, const char *data, std::size_t len) {
	std::string payload(data, len);
	auto nul = payload.find('\0');
	std::string schema = (nul == std::string::npos) ? payload : payload.substr(0, nul);
	std::string table = (nul == std::string::npos) ? std::string() : payload.substr(nul + 1);

	std::string error_text;
	volatile bool found = false;
	RelationDesc desc;
	PG_TRY();
	{
		try {
			Oid rel_oid = pgddb::GetRelidFromSchemaAndTable(schema.c_str(), table.c_str());
			if (pgddb::IsValidOid(rel_oid)) {
				Relation rel = pgddb::OpenRelation(rel_oid);
				desc = BuildRelationDesc(rel);
				pgddb::CloseRelation(rel);
				found = true;
			}
		} catch (const std::exception &e) {
			error_text = e.what();
		}
	}
	PG_CATCH();
	{
		MemoryContext ctx = MemoryContextSwitchTo(TopMemoryContext);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();
		MemoryContextSwitchTo(ctx);
		error_text = edata->message ? edata->message : "describe-relation error";
		FreeErrorData(edata);
	}
	PG_END_TRY();

	if (!error_text.empty()) {
		channel.SendControl(FrameTag::DescribeRelError, error_text.c_str(), error_text.size());
		return;
	}
	if (!found) {
		const char *msg = "relation not found";
		channel.SendControl(FrameTag::DescribeRelError, msg, std::strlen(msg));
		return;
	}
	duckdb::MemoryStream stream;
	SerializeRelationDesc(desc, stream);
	channel.SendControl(FrameTag::DescribeRelResult, reinterpret_cast<const char *>(stream.GetData()),
	                    stream.GetPosition());
}

} // namespace pgddb
