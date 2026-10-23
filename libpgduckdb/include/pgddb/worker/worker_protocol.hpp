#pragma once
// Wire format between a dispatching backend and the duckdb worker: every shm_mq
// message is one frame, a FrameTag byte followed by a payload written with
// DuckDB's BinarySerializer, so values, types and chunks reuse DuckDB's codecs.

#include "duckdb.hpp"

#include <string>
#include <utility>
#include <vector>

#include "pgddb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgddb {

enum class FrameTag : uint8_t {
	// backend -> worker
	QUERY = 1,      // the query to run: sql, parameters, session settings
	RPC_REPLY = 2,  // reply to an RPC frame; payload is extension-defined
	CHUNK_FREE = 3, // the backend is done with an arena page
	// worker -> backend
	SCHEMA = 10,      // result column types, sent once before any chunk
	CHUNK = 11,       // one serialized result DataChunk (nested types, oversized rows)
	DONE = 12,        // the result is complete
	QUERY_ERROR = 13, // the query failed: exception type + message
	RPC = 14,         // the worker needs the backend; payload is extension-defined
	CHUNK_REF = 15,   // one result chunk, laid out by reference in an arena page
};

struct QueryFrame {
	std::string sql;
	// PG $n parameters, named "n", already converted to DuckDB values.
	std::vector<std::pair<std::string, duckdb::Value>> params;
	// DuckDB settings applied to the session connection as SET name = value.
	std::vector<std::pair<std::string, duckdb::Value>> settings;
};

struct ErrorFrame {
	duckdb::ExceptionType type;
	std::string message;
};

// Encoders return the complete frame (tag + payload).
std::string EncodeQueryFrame(const QueryFrame &frame);
std::string EncodeRpcReplyFrame(const std::string &payload);
std::string EncodeSchemaFrame(const duckdb::vector<duckdb::LogicalType> &types);
std::string EncodeChunkFrame(duckdb::DataChunk &chunk);
std::string EncodeDoneFrame();
std::string EncodeErrorFrame(duckdb::ExceptionType type, const std::string &message);
std::string EncodeRpcFrame(const std::string &payload);

// Decoders take a complete frame; the tag must match the decoder.
FrameTag DecodeFrameTag(const char *data, size_t len);
QueryFrame DecodeQueryFrame(const char *data, size_t len);
std::string DecodePayloadFrame(const char *data, size_t len); // RPC and RPC_REPLY
duckdb::vector<duckdb::LogicalType> DecodeSchemaFrame(const char *data, size_t len);
duckdb::unique_ptr<duckdb::DataChunk> DecodeChunkFrame(const char *data, size_t len);
ErrorFrame DecodeErrorFrame(const char *data, size_t len);

// A materialized query result as an RPC payload: either an error (exception
// type + message) or names, types and rows. Encoding consumes the result.
std::string EncodeQueryResult(duckdb::QueryResult &result);
duckdb::unique_ptr<duckdb::QueryResult> DecodeQueryResult(const std::string &payload);

// Arena pages: a result chunk written once into shared memory by the worker
// and read in place by the backend. Fixed-width columns are stored as their
// flat vector data, text and blob columns as offsets plus bytes, each with
// an optional validity mask; nested types are not supported and travel as
// serialized CHUNK frames instead.
std::string EncodeChunkRefFrame(uint32_t page);
uint32_t DecodeChunkRefFrame(const char *data, size_t len);
std::string EncodeChunkFreeFrame(uint32_t page);
uint32_t DecodeChunkFreeFrame(const char *data, size_t len);

bool PageLayoutSupported(const duckdb::vector<duckdb::LogicalType> &types);
// Rows of the flat `chunk`, starting at `start`, that fit into `capacity`
// bytes; 0 when even the first row does not fit.
duckdb::idx_t RowsThatFitPage(duckdb::DataChunk &chunk, duckdb::idx_t start, size_t capacity);
void WritePage(duckdb::DataChunk &chunk, duckdb::idx_t start, duckdb::idx_t count, char *page);
// The chunk references `page` and `strings` (text/blob pointers into the page);
// both must outlive it.
duckdb::unique_ptr<duckdb::DataChunk> ReadPage(const char *page, size_t capacity,
                                               const duckdb::vector<duckdb::LogicalType> &types,
                                               duckdb::vector<duckdb::unsafe_unique_array<duckdb::string_t>> &strings);

} // namespace pgddb
