#include "pgddb/worker/transport/session_protocol.hpp"
#include "pgddb/worker/transport/session_channel.hpp"

#include <cstdint>
#include <string>

#include <duckdb/common/allocator.hpp>
#include <duckdb/common/enums/statement_type.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/materialized_query_result.hpp>

namespace pgddb {

void
SerializeDataChunk(duckdb::DataChunk &chunk, duckdb::MemoryStream &out) {
	duckdb::BinarySerializer serializer(out);
	serializer.Begin();
	chunk.Serialize(serializer);
	serializer.End();
}

void
DeserializeDataChunk(const char *data, std::size_t len, duckdb::DataChunk &out) {
	duckdb::MemoryStream stream(reinterpret_cast<duckdb::data_ptr_t>(const_cast<char *>(data)),
	                            static_cast<duckdb::idx_t>(len));
	duckdb::BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	out.Deserialize(deserializer);
	deserializer.End();
}

static void
WriteU32(duckdb::MemoryStream &s, uint32_t v) {
	s.WriteData(reinterpret_cast<duckdb::const_data_ptr_t>(&v), sizeof(v));
}

static uint32_t
ReadU32(duckdb::MemoryStream &s) {
	uint32_t v = 0;
	s.ReadData(reinterpret_cast<duckdb::data_ptr_t>(&v), sizeof(v));
	return v;
}

static void
WriteString(duckdb::MemoryStream &s, const std::string &str) {
	WriteU32(s, static_cast<uint32_t>(str.size()));
	if (!str.empty())
		s.WriteData(reinterpret_cast<duckdb::const_data_ptr_t>(str.data()), str.size());
}

static std::string
ReadString(duckdb::MemoryStream &s) {
	uint32_t len = ReadU32(s);
	std::string str;
	str.resize(len);
	if (len)
		s.ReadData(reinterpret_cast<duckdb::data_ptr_t>(&str[0]), len);
	return str;
}

// Length-prefixed serialized DataChunk, so chunks can be read back sequentially.
static void
WriteChunk(duckdb::MemoryStream &out, duckdb::DataChunk &chunk) {
	duckdb::MemoryStream tmp;
	SerializeDataChunk(chunk, tmp);
	WriteU32(out, static_cast<uint32_t>(tmp.GetPosition()));
	out.WriteData(tmp.GetData(), tmp.GetPosition());
}

static void
ReadChunk(duckdb::MemoryStream &in, duckdb::DataChunk &chunk) {
	uint32_t len = ReadU32(in);
	auto pos = in.GetPosition();
	DeserializeDataChunk(reinterpret_cast<const char *>(in.GetData() + pos), len, chunk);
	in.SetPosition(pos + len);
}

void
SerializeQueryResult(duckdb::QueryResult &result, duckdb::MemoryStream &out) {
	WriteU32(out, static_cast<uint32_t>(result.names.size()));
	for (auto &name : result.names) {
		WriteU32(out, static_cast<uint32_t>(name.size()));
		if (!name.empty())
			out.WriteData(reinterpret_cast<duckdb::const_data_ptr_t>(name.data()), name.size());
	}

	// Empty chunk carrying the column types, so 0-row results still convey their schema.
	duckdb::DataChunk schema_chunk;
	schema_chunk.Initialize(duckdb::Allocator::DefaultAllocator(), result.types);
	schema_chunk.SetCardinality(0);
	WriteChunk(out, schema_chunk);

	duckdb::vector<duckdb::unique_ptr<duckdb::DataChunk>> chunks;
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk || chunk->size() == 0)
			break;
		chunks.push_back(std::move(chunk));
	}
	WriteU32(out, static_cast<uint32_t>(chunks.size()));
	for (auto &chunk : chunks)
		WriteChunk(out, *chunk);
}

duckdb::unique_ptr<duckdb::QueryResult>
DeserializeQueryResult(const char *data, std::size_t len) {
	duckdb::MemoryStream in(reinterpret_cast<duckdb::data_ptr_t>(const_cast<char *>(data)),
	                        static_cast<duckdb::idx_t>(len));

	uint32_t ncols = ReadU32(in);
	duckdb::vector<duckdb::string> names;
	names.reserve(ncols);
	for (uint32_t i = 0; i < ncols; i++) {
		uint32_t nlen = ReadU32(in);
		duckdb::string name;
		name.resize(nlen);
		if (nlen)
			in.ReadData(reinterpret_cast<duckdb::data_ptr_t>(&name[0]), nlen);
		names.push_back(std::move(name));
	}

	duckdb::DataChunk schema_chunk;
	ReadChunk(in, schema_chunk);
	auto types = schema_chunk.GetTypes();

	uint32_t nchunks = ReadU32(in);
	auto &allocator = duckdb::Allocator::DefaultAllocator();
	auto collection = duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator, types);
	duckdb::ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);
	for (uint32_t i = 0; i < nchunks; i++) {
		duckdb::DataChunk chunk;
		ReadChunk(in, chunk);
		collection->Append(append_state, chunk);
	}

	duckdb::StatementProperties properties;
	duckdb::ClientProperties client_properties;
	return duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::StatementType::SELECT_STATEMENT, properties,
	                                                          std::move(names), std::move(collection),
	                                                          client_properties);
}

void
SerializeRelationDesc(const RelationDesc &desc, duckdb::MemoryStream &out) {
	WriteU32(out, static_cast<uint32_t>(desc.oid));
	WriteString(out, desc.qualified_name);
	WriteString(out, desc.name);
	uint8_t is_temp = desc.is_temporary ? 1 : 0;
	out.WriteData(reinterpret_cast<duckdb::const_data_ptr_t>(&is_temp), sizeof(is_temp));
	out.WriteData(reinterpret_cast<duckdb::const_data_ptr_t>(&desc.cardinality), sizeof(desc.cardinality));
	WriteU32(out, desc.nblocks);
	WriteU32(out, static_cast<uint32_t>(desc.columns.size()));
	for (const auto &col : desc.columns) {
		int32_t attno = static_cast<int32_t>(col.attno);
		out.WriteData(reinterpret_cast<duckdb::const_data_ptr_t>(&attno), sizeof(attno));
		WriteString(out, col.name);
		duckdb::BinarySerializer serializer(out);
		serializer.Begin();
		col.type.Serialize(serializer);
		serializer.End();
	}
}

RelationDesc
DeserializeRelationDesc(const char *data, std::size_t len) {
	duckdb::MemoryStream in(reinterpret_cast<duckdb::data_ptr_t>(const_cast<char *>(data)),
	                        static_cast<duckdb::idx_t>(len));
	RelationDesc desc;
	desc.oid = static_cast<Oid>(ReadU32(in));
	desc.qualified_name = ReadString(in);
	desc.name = ReadString(in);
	uint8_t is_temp = 0;
	in.ReadData(reinterpret_cast<duckdb::data_ptr_t>(&is_temp), sizeof(is_temp));
	desc.is_temporary = is_temp != 0;
	in.ReadData(reinterpret_cast<duckdb::data_ptr_t>(&desc.cardinality), sizeof(desc.cardinality));
	desc.nblocks = ReadU32(in);
	uint32_t ncols = ReadU32(in);
	desc.columns.reserve(ncols);
	for (uint32_t i = 0; i < ncols; i++) {
		RelationColumn col;
		int32_t attno = 0;
		in.ReadData(reinterpret_cast<duckdb::data_ptr_t>(&attno), sizeof(attno));
		col.attno = static_cast<AttrNumber>(attno);
		col.name = ReadString(in);
		duckdb::BinaryDeserializer deserializer(in);
		deserializer.Begin();
		col.type = duckdb::LogicalType::Deserialize(deserializer);
		deserializer.End();
		desc.columns.push_back(std::move(col));
	}
	return desc;
}

RelationDesc
WorkerDescribeRelation(SessionChannel &channel, const std::string &schema, const std::string &table) {
	/* One metadata-lane round-trip at a time per channel: replies carry no request id. */
	std::lock_guard<std::mutex> request(channel.MetaRequestMutex());
	std::string payload = schema;
	payload.push_back('\0');
	payload += table;
	if (!channel.SerializedSendResult(FrameTag::DescribeRel, payload.data(), payload.size()))
		throw duckdb::IOException("duckdb worker: describe-relation channel detached");
	FrameTag tag = FrameTag::Error;
	std::string reply;
	if (channel.RecvMetaReply(&tag, &reply) != FrameResult::Ok) {
		throw duckdb::IOException("duckdb worker: describe-relation channel detached");
	}
	if (tag == FrameTag::DescribeRelResult) {
		return DeserializeRelationDesc(reply.data(), reply.size());
	}
	throw duckdb::CatalogException(reply); /* DescribeRelError */
}

namespace {

/* Send a metadata request on the result queue and wait for the reply on the channel's
 * metadata lane. Safe from any worker thread: sends are serialized, the reply comes
 * off the demuxed metadata lane, and the meta-request mutex keeps a single metadata
 * round-trip in flight per channel (replies carry no request id). */
duckdb::unique_ptr<duckdb::QueryResult>
MetadataRoundTrip(SessionChannel &channel, FrameTag request, const std::string &sql) {
	std::lock_guard<std::mutex> request_lock(channel.MetaRequestMutex());
	if (!channel.SerializedSendResult(request, sql.c_str(), sql.size())) {
		return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
		    duckdb::ErrorData(duckdb::ExceptionType::IO, "duckdb worker: metadata channel detached"));
	}
	FrameTag tag = FrameTag::Error;
	std::string reply;
	if (channel.RecvMetaReply(&tag, &reply) != FrameResult::Ok) {
		return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
		    duckdb::ErrorData(duckdb::ExceptionType::IO, "duckdb worker: metadata channel detached"));
	}
	if (tag == FrameTag::MetaResult) {
		return DeserializeQueryResult(reply.data(), reply.size());
	}
	if (tag == FrameTag::MetaError) {
		/* payload: [uint8 duckdb exception type][message] */
		auto etype = duckdb::ExceptionType::IO;
		std::string msg;
		if (!reply.empty()) {
			etype = (duckdb::ExceptionType)(uint8_t)reply[0];
			msg = reply.substr(1);
		}
		return duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::ErrorData(etype, msg));
	}
	return duckdb::make_uniq<duckdb::MaterializedQueryResult>(
	    duckdb::ErrorData(duckdb::ExceptionType::IO, "duckdb worker: unexpected metadata reply"));
}

} // namespace

duckdb::unique_ptr<duckdb::QueryResult>
WorkerMetadataQuery(SessionChannel &channel, const std::string &sql) {
	return MetadataRoundTrip(channel, FrameTag::MetaQuery, sql);
}

duckdb::unique_ptr<duckdb::QueryResult>
WorkerMetadataExec(SessionChannel &channel, const std::string &sql) {
	return MetadataRoundTrip(channel, FrameTag::MetaExec, sql);
}

void
SendMetadataReply(SessionChannel &channel, duckdb::QueryResult &result) {
	if (result.HasError()) {
		auto &err = result.GetErrorObject();
		SendMetadataError(channel, err.Type(), err.RawMessage());
		return;
	}
	duckdb::MemoryStream stream;
	SerializeQueryResult(result, stream);
	channel.SendControl(FrameTag::MetaResult, reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

void
SendMetadataError(SessionChannel &channel, duckdb::ExceptionType type, const std::string &msg) {
	std::string payload;
	payload.push_back((char)(uint8_t)type);
	payload += msg;
	channel.SendControl(FrameTag::MetaError, payload.data(), payload.size());
}

} // namespace pgddb
