#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "access/tableam.h"

typedef void (*DuckDBLoadExtension)(void *db, void *context);
typedef bool (*DuckdbExternalTableCheck)(Oid relid);
typedef char *(*DuckdbRelationNameCallback)(Oid relid);

bool RegisterDuckdbLoadExtension(DuckDBLoadExtension extension);
bool RegisterDuckdbTableAm(const char *name, const TableAmRoutine *am);
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

#ifdef __cplusplus
} // extern "C"
#endif
