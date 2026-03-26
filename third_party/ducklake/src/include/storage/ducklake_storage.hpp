//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class DuckLakeStorageExtension : public StorageExtension {
public:
	DuckLakeStorageExtension();
};

} // namespace duckdb
