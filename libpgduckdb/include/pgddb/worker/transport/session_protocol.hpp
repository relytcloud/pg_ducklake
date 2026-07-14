#pragma once

#include <cstddef>
#include <string>

#include <duckdb/main/query_result.hpp>

#include "pgddb/catalog/relation_desc.hpp"

namespace duckdb {
class DataChunk;
class MemoryStream;
} // namespace duckdb

namespace pgddb {

class SessionChannel;

// Serialize `chunk` into `out`; the resulting bytes (out.GetData(),
// out.GetPosition()) are position-independent and safe to ship across a process
// boundary to another process linking the same DuckDB build. DeserializeDataChunk
// reconstructs the chunk from those bytes.
void SerializeDataChunk(duckdb::DataChunk &chunk, duckdb::MemoryStream &out);
void DeserializeDataChunk(const char *data, std::size_t len, duckdb::DataChunk &out);

// Serialize a whole materialized query result (column names + types + all rows)
// into `out`, and rebuild it as a MaterializedQueryResult on the other side. Used
// to ship metadata-query results from a backend to the PG-free duckdb worker.
// SerializeQueryResult drains `result`.
void SerializeQueryResult(duckdb::QueryResult &result, duckdb::MemoryStream &out);
duckdb::unique_ptr<duckdb::QueryResult> DeserializeQueryResult(const char *data, std::size_t len);

// Worker side: send `sql` to the requesting backend as a MetaQuery (read) or MetaExec
// (write, run in a subtransaction on the backend) and wait for the backend's MetaResult,
// rebuilt as a MaterializedQueryResult (or an error result if the backend reported one /
// the channel detached). Session-thread-safe (serialized transport). The backend answers
// with SendMetadataReply.
duckdb::unique_ptr<duckdb::QueryResult> WorkerMetadataQuery(SessionChannel &channel, const std::string &sql);
duckdb::unique_ptr<duckdb::QueryResult> WorkerMetadataExec(SessionChannel &channel, const std::string &sql);

// Backend side: ship `result` back as MetaResult, or -- when it carries an error -- as
// a MetaError that preserves the duckdb exception type (so e.g. a TransactionException
// from a metadata write conflict is rethrown as one in the worker).
void SendMetadataReply(SessionChannel &channel, duckdb::QueryResult &result);
void SendMetadataError(SessionChannel &channel, duckdb::ExceptionType type, const std::string &msg);

// Serialize a RelationDesc into `out` / rebuild one from those bytes, so a relation
// descriptor can be shipped from a backend to the PG-free duckdb worker.
void SerializeRelationDesc(const RelationDesc &desc, duckdb::MemoryStream &out);
RelationDesc DeserializeRelationDesc(const char *data, std::size_t len);

// Worker side: ask the requesting backend to describe `schema`.`table` and block for
// the backend's DescribeRelResult, rebuilt as a RelationDesc. Throws on a backend error
// (e.g. relation not found) or a detached channel.
RelationDesc WorkerDescribeRelation(SessionChannel &channel, const std::string &schema, const std::string &table);

} // namespace pgddb
