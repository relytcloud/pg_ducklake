//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_secret.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

class DuckLakeSecret {
public:
	static constexpr const char *DEFAULT_SECRET = "__default_ducklake";

public:
	static unique_ptr<BaseSecret> CreateDuckLakeSecretFunction(ClientContext &context, CreateSecretInput &input);
	static unique_ptr<SecretEntry> GetSecret(ClientContext &context, const string &secret_name);

	static bool PathIsSecret(const string &path);

	static SecretType GetSecretType();
	static CreateSecretFunction GetFunction();
};

} // namespace duckdb
