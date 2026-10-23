#include "pgddb/worker/worker_protocol.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include "pgddb/utility/cpp_only_file.hpp" // Must be last include.

namespace pgddb {

namespace {

using NamedValues = std::vector<std::pair<std::string, duckdb::Value>>;

// Appends one root object whose properties `write` adds; returns the whole stream.
template <class FUNC>
std::string
Serialize(duckdb::MemoryStream &stream, FUNC write) {
	duckdb::BinarySerializer serializer(stream);
	serializer.Begin();
	write(serializer);
	serializer.End();
	return std::string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

// Reads one root object from `data`; `read` consumes its properties.
template <class FUNC>
void
Deserialize(const char *data, size_t len, FUNC read) {
	// Non-owning stream over the caller's bytes; deserializing never writes.
	duckdb::MemoryStream stream(reinterpret_cast<duckdb::data_ptr_t>(const_cast<char *>(data)), len);
	duckdb::BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	read(deserializer);
	deserializer.End();
}

template <class FUNC>
std::string
EncodeFrame(FrameTag tag, FUNC write) {
	duckdb::MemoryStream stream(duckdb::Allocator::DefaultAllocator());
	stream.Write<uint8_t>(static_cast<uint8_t>(tag));
	return Serialize(stream, write);
}

void
ExpectTag(FrameTag expected, const char *data, size_t len) {
	auto tag = DecodeFrameTag(data, len);
	if (tag != expected) {
		throw duckdb::SerializationException("Failed to deserialize frame: expected tag %d, got %d",
		                                     static_cast<int>(expected), static_cast<int>(tag));
	}
}

template <class FUNC>
void
DecodeFrame(FrameTag expected, const char *data, size_t len, FUNC read) {
	ExpectTag(expected, data, len);
	Deserialize(data + 1, len - 1, read);
}

std::string
PayloadFrame(FrameTag tag, const std::string &payload) {
	return std::string(1, static_cast<char>(tag)) + payload;
}

// Each pair is an object {0: name, 1: value}, DuckDB's own pair encoding.
void
WriteNamedValues(duckdb::Serializer &serializer, duckdb::field_id_t id, const char *tag, const NamedValues &values) {
	serializer.WriteList(id, tag, values.size(),
	                     [&](duckdb::Serializer::List &list, duckdb::idx_t i) { list.WriteElement(values[i]); });
}

NamedValues
ReadNamedValues(duckdb::Deserializer &deserializer, duckdb::field_id_t id, const char *tag) {
	NamedValues values;
	deserializer.ReadList(id, tag, [&](duckdb::Deserializer::List &list, duckdb::idx_t) {
		values.push_back(list.ReadElement<NamedValues::value_type>());
	});
	return values;
}

} // namespace

std::string
EncodeQueryFrame(const QueryFrame &frame) {
	return EncodeFrame(FrameTag::QUERY, [&](duckdb::Serializer &s) {
		s.WriteProperty(100, "sql", frame.sql);
		WriteNamedValues(s, 101, "params", frame.params);
		WriteNamedValues(s, 102, "settings", frame.settings);
	});
}

std::string
EncodeRpcReplyFrame(const std::string &payload) {
	return PayloadFrame(FrameTag::RPC_REPLY, payload);
}

std::string
EncodeSchemaFrame(const duckdb::vector<duckdb::LogicalType> &types) {
	return EncodeFrame(FrameTag::SCHEMA, [&](duckdb::Serializer &s) { s.WriteProperty(100, "types", types); });
}

std::string
EncodeChunkFrame(duckdb::DataChunk &chunk) {
	return EncodeFrame(FrameTag::CHUNK, [&](duckdb::Serializer &s) { chunk.Serialize(s); });
}

std::string
EncodeDoneFrame() {
	return std::string(1, static_cast<char>(FrameTag::DONE));
}

std::string
EncodeErrorFrame(duckdb::ExceptionType type, const std::string &message) {
	return EncodeFrame(FrameTag::QUERY_ERROR, [&](duckdb::Serializer &s) {
		s.WriteProperty(100, "type", static_cast<uint8_t>(type));
		s.WriteProperty(101, "message", message);
	});
}

std::string
EncodeRpcFrame(const std::string &payload) {
	return PayloadFrame(FrameTag::RPC, payload);
}

FrameTag
DecodeFrameTag(const char *data, size_t len) {
	if (len == 0) {
		throw duckdb::SerializationException("Failed to deserialize frame: empty frame");
	}
	auto tag = static_cast<FrameTag>(static_cast<uint8_t>(data[0]));
	switch (tag) {
	case FrameTag::QUERY:
	case FrameTag::RPC_REPLY:
	case FrameTag::CHUNK_FREE:
	case FrameTag::SCHEMA:
	case FrameTag::CHUNK:
	case FrameTag::DONE:
	case FrameTag::QUERY_ERROR:
	case FrameTag::RPC:
	case FrameTag::CHUNK_REF:
		return tag;
	}
	throw duckdb::SerializationException("Failed to deserialize frame: unknown tag %d", static_cast<int>(tag));
}

QueryFrame
DecodeQueryFrame(const char *data, size_t len) {
	QueryFrame frame;
	DecodeFrame(FrameTag::QUERY, data, len, [&](duckdb::Deserializer &d) {
		frame.sql = d.ReadProperty<std::string>(100, "sql");
		frame.params = ReadNamedValues(d, 101, "params");
		frame.settings = ReadNamedValues(d, 102, "settings");
	});
	return frame;
}

std::string
DecodePayloadFrame(const char *data, size_t len) {
	auto tag = DecodeFrameTag(data, len);
	if (tag != FrameTag::RPC && tag != FrameTag::RPC_REPLY) {
		throw duckdb::SerializationException("Failed to deserialize frame: expected an RPC tag, got %d",
		                                     static_cast<int>(tag));
	}
	return std::string(data + 1, len - 1);
}

duckdb::vector<duckdb::LogicalType>
DecodeSchemaFrame(const char *data, size_t len) {
	duckdb::vector<duckdb::LogicalType> types;
	DecodeFrame(FrameTag::SCHEMA, data, len, [&](duckdb::Deserializer &d) {
		types = d.ReadProperty<duckdb::vector<duckdb::LogicalType>>(100, "types");
	});
	return types;
}

duckdb::unique_ptr<duckdb::DataChunk>
DecodeChunkFrame(const char *data, size_t len) {
	// DataChunk::Deserialize initializes the chunk on the default allocator, so it owns its data.
	auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
	DecodeFrame(FrameTag::CHUNK, data, len, [&](duckdb::Deserializer &d) { chunk->Deserialize(d); });
	return chunk;
}

ErrorFrame
DecodeErrorFrame(const char *data, size_t len) {
	ErrorFrame frame;
	DecodeFrame(FrameTag::QUERY_ERROR, data, len, [&](duckdb::Deserializer &d) {
		frame.type = static_cast<duckdb::ExceptionType>(d.ReadProperty<uint8_t>(100, "type"));
		frame.message = d.ReadProperty<std::string>(101, "message");
	});
	return frame;
}

std::string
EncodeQueryResult(duckdb::QueryResult &result) {
	duckdb::MemoryStream stream(duckdb::Allocator::DefaultAllocator());
	if (result.HasError()) {
		auto &error = result.GetErrorObject();
		auto &message = error.RawMessage().empty() ? error.Message() : error.RawMessage();
		return Serialize(stream, [&](duckdb::Serializer &s) {
			s.WriteProperty(100, "has_error", true);
			s.WriteProperty(101, "error_type", static_cast<uint8_t>(error.Type()));
			s.WriteProperty(102, "error_message", message);
		});
	}
	duckdb::vector<duckdb::unique_ptr<duckdb::DataChunk>> chunks;
	while (auto chunk = result.Fetch()) {
		if (chunk->size() == 0) {
			break;
		}
		chunks.push_back(std::move(chunk));
	}
	return Serialize(stream, [&](duckdb::Serializer &s) {
		s.WriteProperty(100, "has_error", false);
		s.WriteProperty(103, "names", result.names);
		s.WriteProperty(104, "types", result.types);
		s.WriteList(105, "chunks", chunks.size(), [&](duckdb::Serializer::List &list, duckdb::idx_t i) {
			list.WriteObject([&](duckdb::Serializer &object) { chunks[i]->Serialize(object); });
		});
	});
}

duckdb::unique_ptr<duckdb::QueryResult>
DecodeQueryResult(const std::string &payload) {
	duckdb::unique_ptr<duckdb::QueryResult> result;
	Deserialize(payload.data(), payload.size(), [&](duckdb::Deserializer &d) {
		if (d.ReadProperty<bool>(100, "has_error")) {
			auto type = static_cast<duckdb::ExceptionType>(d.ReadProperty<uint8_t>(101, "error_type"));
			auto message = d.ReadProperty<std::string>(102, "error_message");
			result = duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::ErrorData(type, message));
			return;
		}
		auto names = d.ReadProperty<duckdb::vector<std::string>>(103, "names");
		auto types = d.ReadProperty<duckdb::vector<duckdb::LogicalType>>(104, "types");
		auto collection =
		    duckdb::make_uniq<duckdb::ColumnDataCollection>(duckdb::Allocator::DefaultAllocator(), std::move(types));
		d.ReadList(105, "chunks", [&](duckdb::Deserializer::List &list, duckdb::idx_t) {
			list.ReadObject([&](duckdb::Deserializer &object) {
				duckdb::DataChunk chunk;
				chunk.Deserialize(object);
				collection->Append(chunk);
			});
		});
		result = duckdb::make_uniq<duckdb::MaterializedQueryResult>(duckdb::StatementType::SELECT_STATEMENT,
		                                                            duckdb::StatementProperties(), std::move(names),
		                                                            std::move(collection), duckdb::ClientProperties());
	});
	return result;
}

} // namespace pgddb
