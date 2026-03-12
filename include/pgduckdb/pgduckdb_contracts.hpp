#pragma once

extern "C" {
#include "postgres.h"

#include "access/tableam.h"
}

namespace duckdb {
class DuckDB;
}

// Upstream function — keep extern "C" linkage
extern "C" bool RegisterDuckdbTableAm(const char *name, const TableAmRoutine *am);

// Our C++ additions — all in namespace pgduckdb
namespace pgduckdb {

typedef void (*DuckDBLoadExtension)(duckdb::DuckDB &db);
typedef bool (*DuckdbExternalTableCheck)(Oid relid);
typedef char *(*DuckdbRelationNameCallback)(Oid relid);

bool RegisterDuckdbLoadExtension(DuckDBLoadExtension extension);
bool RegisterDuckdbExternalTableCheck(DuckdbExternalTableCheck callback);
void RegisterDuckdbRelationNameCallback(DuckdbRelationNameCallback callback);
bool DuckdbIsAlterTableInProgress(void);
bool DuckdbIsInitialized(void);
void DuckdbUnsafeSetNextExpectedCommandId(uint32_t command_id);
// Allow a PostgreSQL internal subtransaction while a DuckDB transaction is active.
// pg_ducklake uses this to wrap metadata commit writes so DuckLake's retry loop
// can catch and recover from constraint conflicts without crashing the backend.
void DuckdbAllowSubtransaction(bool allow);
void DuckdbLockGlobalProcess(void);
void DuckdbUnlockGlobalProcess(void);
bool DuckdbSetForceExecution(bool value);
bool DuckdbEnsureCacheValid(void);
void RegisterDuckdbOnlyExtension(const char *extension_name);
void RegisterDuckdbOnlyFunction(const char *function_name);

} // namespace pgduckdb
