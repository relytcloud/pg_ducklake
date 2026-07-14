#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "pgddb/pg/declarations.hpp"

#include <duckdb/common/types.hpp>

namespace pgddb {

// One non-dropped column of a relation, materialized as plain data.
struct RelationColumn {
	AttrNumber attno = 0;                                      // 1-based PG attribute number
	std::string name = {};                                     // attname
	duckdb::LogicalType type = duckdb::LogicalTypeId::SQLNULL; // ConvertPostgresToDuckColumnType result
};

// Snapshot of everything the DuckDB catalog + scan path needs from a PG relation,
// built once from a live Relation; holds no live handle afterwards.
struct RelationDesc {
	Oid oid = 0;                     // InvalidOid
	std::string qualified_name = {}; // GenerateQualifiedRelationName (schema."table")
	std::string name = {};           // RelationGetRelationName (unqualified)
	bool is_temporary = false;
	double cardinality = 0;
	uint32_t nblocks = 0;                     // RelationBlockCount (main fork)
	std::vector<RelationColumn> columns = {}; // non-dropped, in attno order
};

// Fill a RelationDesc from a live relation. Takes the GlobalProcessLock internally.
RelationDesc BuildRelationDesc(Relation rel);

class SessionChannel;

// Backend side: handle a worker's DescribeRel request. `data`/`len` is the payload
// `schema\0table`; opens the relation under the backend's catalog, builds a RelationDesc,
// and ships it back as DescribeRelResult (or DescribeRelError on a missing relation /
// error).
void ServeDescribeRelation(SessionChannel &channel, const char *data, std::size_t len);

} // namespace pgddb
