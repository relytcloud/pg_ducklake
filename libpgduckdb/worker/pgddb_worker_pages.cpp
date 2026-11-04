#include "pgddb/worker/worker_protocol.hpp"

#include <cstring>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"

namespace pgddb {

namespace {

constexpr size_t ALIGN = 8;

enum ColumnKind : uint32_t { FIXED = 0, VARLEN = 1 };

struct PageHeader {
	uint32_t rows;
	uint32_t columns;
};

// Offsets are relative to the page start; 0 means "none" (never a valid offset).
struct PageColumn {
	uint32_t kind;
	uint32_t data_off;     // FIXED: rows * width bytes; VARLEN: the string bytes
	uint32_t validity_off; // rows bits as validity_t words, 0 when every row is valid
	uint32_t offsets_off;  // VARLEN: rows + 1 uint32 offsets into the data region
};

size_t
AlignUp(size_t n) {
	return (n + ALIGN - 1) & ~(ALIGN - 1);
}

size_t
HeaderBytes(idx_t columns) {
	return AlignUp(sizeof(PageHeader) + columns * sizeof(PageColumn));
}

size_t
ValidityBytes(idx_t rows) {
	return AlignUp(duckdb::ValidityMask::EntryCount(rows) * sizeof(duckdb::validity_t));
}

std::string
EncodePageFrame(FrameTag tag, uint32_t page) {
	std::string frame(1 + sizeof(page), '\0');
	frame[0] = static_cast<char>(tag);
	memcpy(&frame[1], &page, sizeof(page));
	return frame;
}

uint32_t
DecodePageFrame(FrameTag tag, const char *data, size_t len) {
	if (len != 1 + sizeof(uint32_t) || static_cast<FrameTag>(data[0]) != tag) {
		throw duckdb::SerializationException("malformed arena page frame");
	}
	uint32_t page;
	memcpy(&page, data + 1, sizeof(page));
	return page;
}

bool
ColumnSupported(const duckdb::LogicalType &type) {
	auto physical = type.InternalType();
	return duckdb::TypeIsConstantSize(physical) || physical == duckdb::PhysicalType::VARCHAR;
}

} // namespace

std::string
EncodeChunkRefFrame(uint32_t page) {
	return EncodePageFrame(FrameTag::CHUNK_REF, page);
}

uint32_t
DecodeChunkRefFrame(const char *data, size_t len) {
	return DecodePageFrame(FrameTag::CHUNK_REF, data, len);
}

std::string
EncodeChunkFreeFrame(uint32_t page) {
	return EncodePageFrame(FrameTag::CHUNK_FREE, page);
}

uint32_t
DecodeChunkFreeFrame(const char *data, size_t len) {
	return DecodePageFrame(FrameTag::CHUNK_FREE, data, len);
}

bool
PageLayoutSupported(const duckdb::vector<duckdb::LogicalType> &types) {
	for (auto &type : types) {
		if (!ColumnSupported(type)) {
			return false;
		}
	}
	return true;
}

duckdb::idx_t
RowsThatFitPage(duckdb::DataChunk &chunk, duckdb::idx_t start, size_t capacity) {
	idx_t columns = chunk.ColumnCount();
	idx_t remaining = chunk.size() - start;
	size_t fixed_per_row = 0;
	idx_t varlen_columns = 0;
	for (idx_t col = 0; col < columns; col++) {
		auto physical = chunk.data[col].GetType().InternalType();
		if (physical == duckdb::PhysicalType::VARCHAR) {
			varlen_columns++;
		} else {
			fixed_per_row += duckdb::GetTypeIdSize(physical);
		}
	}
	size_t bytes = 0; // string bytes of the rows taken so far
	idx_t rows = 0;
	while (rows < remaining) {
		idx_t row = start + rows;
		size_t row_bytes = 0;
		for (idx_t col = 0; col < columns; col++) {
			auto &vec = chunk.data[col];
			if (vec.GetType().InternalType() != duckdb::PhysicalType::VARCHAR) {
				continue;
			}
			if (duckdb::FlatVector::Validity(vec).RowIsValid(row)) {
				row_bytes += duckdb::FlatVector::GetData<duckdb::string_t>(vec)[row].GetSize();
			}
		}
		idx_t next = rows + 1;
		// Every region is aligned separately; bound the total from above.
		size_t total = HeaderBytes(columns) + columns * (ValidityBytes(next) + ALIGN) + AlignUp(fixed_per_row * next) +
		               varlen_columns * AlignUp((next + 1) * sizeof(uint32_t)) + AlignUp(bytes + row_bytes);
		if (total > capacity) {
			break;
		}
		bytes += row_bytes;
		rows = next;
	}
	return rows;
}

void
WritePage(duckdb::DataChunk &chunk, duckdb::idx_t start, duckdb::idx_t count, char *page) {
	idx_t columns = chunk.ColumnCount();
	auto *header = reinterpret_cast<PageHeader *>(page);
	header->rows = static_cast<uint32_t>(count);
	header->columns = static_cast<uint32_t>(columns);
	auto *descs = reinterpret_cast<PageColumn *>(page + sizeof(PageHeader));
	size_t cursor = HeaderBytes(columns);

	for (idx_t col = 0; col < columns; col++) {
		auto &vec = chunk.data[col];
		auto &desc = descs[col];
		auto &validity = duckdb::FlatVector::Validity(vec);
		desc.validity_off = 0;
		if (!validity.AllValid()) {
			// Rebuild the mask for rows [start, start + count): bits are not
			// byte aligned to `start` in the source mask.
			desc.validity_off = static_cast<uint32_t>(cursor);
			auto *words = reinterpret_cast<duckdb::validity_t *>(page + cursor);
			idx_t entries = duckdb::ValidityMask::EntryCount(count);
			memset(words, 0xff, entries * sizeof(duckdb::validity_t));
			duckdb::ValidityMask mask(words, count);
			for (idx_t i = 0; i < count; i++) {
				if (!validity.RowIsValid(start + i)) {
					mask.SetInvalid(i);
				}
			}
			cursor += ValidityBytes(count);
		}

		auto physical = vec.GetType().InternalType();
		if (physical == duckdb::PhysicalType::VARCHAR) {
			desc.kind = VARLEN;
			desc.offsets_off = static_cast<uint32_t>(cursor);
			auto *offsets = reinterpret_cast<uint32_t *>(page + cursor);
			cursor += AlignUp((count + 1) * sizeof(uint32_t));
			desc.data_off = static_cast<uint32_t>(cursor);
			auto *strings = duckdb::FlatVector::GetData<duckdb::string_t>(vec);
			uint32_t written = 0;
			for (idx_t i = 0; i < count; i++) {
				offsets[i] = written;
				if (validity.RowIsValid(start + i)) {
					auto &str = strings[start + i];
					memcpy(page + cursor + written, str.GetData(), str.GetSize());
					written += static_cast<uint32_t>(str.GetSize());
				}
			}
			offsets[count] = written;
			cursor += AlignUp(written);
		} else {
			desc.kind = FIXED;
			desc.offsets_off = 0;
			desc.data_off = static_cast<uint32_t>(cursor);
			size_t width = duckdb::GetTypeIdSize(physical);
			memcpy(page + cursor, duckdb::FlatVector::GetData(vec) + start * width, count * width);
			cursor += AlignUp(count * width);
		}
	}
}

duckdb::unique_ptr<duckdb::DataChunk>
ReadPage(const char *page, size_t capacity, const duckdb::vector<duckdb::LogicalType> &types,
         duckdb::vector<duckdb::unsafe_unique_array<duckdb::string_t>> &strings) {
	auto *header = reinterpret_cast<const PageHeader *>(page);
	if (header->columns != types.size() || HeaderBytes(header->columns) > capacity) {
		throw duckdb::SerializationException("arena page does not match the result schema");
	}
	idx_t rows = header->rows;
	if (rows > STANDARD_VECTOR_SIZE) {
		throw duckdb::SerializationException("arena page holds more rows than a chunk");
	}
	auto *descs = reinterpret_cast<const PageColumn *>(page + sizeof(PageHeader));
	auto check = [&](size_t off, size_t bytes) {
		if (off % ALIGN != 0 || off > capacity || bytes > capacity - off) {
			throw duckdb::SerializationException("arena page region out of bounds");
		}
	};

	auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
	chunk->InitializeEmpty(types);
	strings.clear();
	for (idx_t col = 0; col < types.size(); col++) {
		auto &desc = descs[col];
		auto physical = types[col].InternalType();
		bool varlen = physical == duckdb::PhysicalType::VARCHAR;
		if (desc.kind != (varlen ? VARLEN : FIXED)) {
			throw duckdb::SerializationException("arena page column does not match the result schema");
		}
		duckdb::data_ptr_t data;
		if (varlen) {
			check(desc.offsets_off, (rows + 1) * sizeof(uint32_t));
			auto *offsets = reinterpret_cast<const uint32_t *>(page + desc.offsets_off);
			check(desc.data_off, offsets[rows]);
			auto array = duckdb::make_unsafe_uniq_array<duckdb::string_t>(rows);
			const char *bytes = page + desc.data_off;
			for (idx_t i = 0; i < rows; i++) {
				if (offsets[i + 1] < offsets[i]) {
					throw duckdb::SerializationException("arena page offsets out of order");
				}
				array[i] = duckdb::string_t(bytes + offsets[i], offsets[i + 1] - offsets[i]);
			}
			data = reinterpret_cast<duckdb::data_ptr_t>(array.get());
			strings.push_back(std::move(array));
		} else {
			check(desc.data_off, rows * duckdb::GetTypeIdSize(physical));
			data = reinterpret_cast<duckdb::data_ptr_t>(const_cast<char *>(page + desc.data_off));
		}
		duckdb::Vector vec(types[col], data);
		if (desc.validity_off != 0) {
			check(desc.validity_off, ValidityBytes(rows));
			auto *words = reinterpret_cast<duckdb::validity_t *>(const_cast<char *>(page + desc.validity_off));
			duckdb::ValidityMask mask(words, rows);
			duckdb::FlatVector::SetValidity(vec, mask);
		}
		chunk->data[col].Reference(vec);
	}
	chunk->SetCardinality(rows);
	return chunk;
}

} // namespace pgddb
