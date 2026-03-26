//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_encryption.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class DuckLakeEncryption { AUTOMATIC, ENCRYPTED, UNENCRYPTED };

} // namespace duckdb
