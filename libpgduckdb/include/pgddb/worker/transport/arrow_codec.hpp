#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "pgddb/pg/declarations.hpp"

#include <duckdb/common/types.hpp>

// The arrow codec: direct PG-heap -> Apache Arrow producer and the matching consumer
// import, for the duckdb worker's zero-copy scan transport. The producer writes Arrow buffers
// for a record batch straight into a shared pool page (no DuckDB DataChunk on the
// backend); the consumer builds an ArrowArray over the page and imports it into a
// DuckDB DataChunk, referencing the page for fixed-width columns (no per-value copy).
namespace duckdb {
class ClientContext;
class DataChunk;
} // namespace duckdb

namespace pgddb {

// Arrow physical encoding chosen per column. Fixed-width codes store the value
// inline; Utf8 uses int32 offsets + a data buffer.
enum class ArrowColCode : uint8_t {
	Unsupported = 0,
	Int16 = 1,
	Int32 = 2,
	Int64 = 3,
	Float = 4,
	Double = 5,
	Date32 = 6,
	Bool = 7, // bit-packed like a validity bitmap
	Utf8 = 8,
	Decimal32 = 9,
	Decimal64 = 10,
	Decimal128 = 11,
	TimestampUs = 12,   // microseconds since the unix epoch, no time zone
	TimestampTzUs = 13, // microseconds since the unix epoch, UTC
	Binary = 14,        // like Utf8: int32 offsets + a data buffer (bytea / BLOB)
};

// Per-column entry in the batch descriptor sent over the control queue.
struct ArrowColDesc {
	uint8_t code; // ArrowColCode
	uint8_t precision;
	uint8_t scale;
	uint8_t pad;
	int64_t null_count;
	uint32_t validity_off, validity_len;
	uint32_t buf1_off, buf1_len; // fixed values, or utf8 int32 offsets
	uint32_t buf2_off, buf2_len; // utf8 data (0 otherwise)
};

struct ArrowBatchHeader {
	uint32_t page_index;
	uint32_t nrows;
	uint32_t ncols;
};

// Per-column value writer, resolved once per batch (mirrors GetPostgresToDuckValueFn).
using ArrowAppendValueFn = void (*)(char *values, int row, Datum value);

// Choose the Arrow encoding for a PG column given its type and the DuckDB type the
// scan bound it to. Returns false (Unsupported) if the column can't take the Arrow
// fast path, in which case the whole batch falls back to the serialize transport.
bool ArrowColumnSpec(Oid pg_type, const duckdb::LogicalType &duck_type, ArrowColCode &code, uint8_t &precision,
                     uint8_t &scale);

// Builds one Arrow record batch into a page. Fixed buffers (validity, values, utf8
// offsets, bool bitmap) are written directly into the page at reserved offsets;
// utf8 data is staged and placed contiguously per column at Finalize.
class ArrowBatchBuilder {
public:
	ArrowBatchBuilder(char *page, std::size_t capacity, const std::vector<ArrowColCode> &codes,
	                  const std::vector<uint8_t> &precisions, const std::vector<uint8_t> &scales, int max_rows);
	ArrowBatchBuilder(const ArrowBatchBuilder &) = delete;
	ArrowBatchBuilder &operator=(const ArrowBatchBuilder &) = delete;
	bool
	LayoutOk() const {
		return layout_ok_;
	}
	// Append one row (all columns) from a deformed slot (slot_getallattrs already
	// called). Returns false if the page would overflow -> caller falls back.
	bool AppendSlot(TupleTableSlot *slot);
	int
	Rows() const {
		return nrows_;
	}
	// Place staged utf8 data into the page and write the descriptor (header + col
	// descs) into `out`; returns descriptor byte length.
	std::size_t Finalize(int page_index, char *out, std::size_t out_cap);

private:
	char *page_;
	std::size_t capacity_;
	std::vector<ArrowColCode> codes_;
	std::vector<uint8_t> precisions_;
	std::vector<uint8_t> scales_;
	int max_rows_;
	int nrows_ = 0;
	bool layout_ok_ = true;

	std::vector<uint32_t> validity_off_, values_off_; // per column, into page
	std::vector<uint32_t> elem_width_;                // fixed-width element size (bytes)
	std::vector<ArrowAppendValueFn> append_fn_;       // per-column writer, resolved once
	std::vector<uint8_t> is_varlen_;
	std::vector<int64_t> null_count_;
	std::vector<std::string> utf8_data_; // staged per-column utf8 bytes
	std::size_t fixed_end_ = 0;          // page offset where utf8 data may be placed
};

// Consumer: import a batch (page + descriptor) into `out`, typed to `output_types`.
// `on_release` is invoked (once) when DuckDB drops the last vector referencing the
// page, so the caller can return the page to the pool.
void ImportArrowBatch(duckdb::ClientContext &context, char *page, const char *descriptor, std::size_t desc_len,
                      const duckdb::vector<duckdb::LogicalType> &output_types, duckdb::DataChunk &out,
                      std::function<void()> on_release);

} // namespace pgddb
