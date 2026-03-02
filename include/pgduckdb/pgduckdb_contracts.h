#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "access/tableam.h"

typedef void (*DuckDBLoadExtension)(void *db, void *context);

bool RegisterDuckdbLoadExtension(DuckDBLoadExtension extension);
bool RegisterDuckdbTableAm(const char *name, const TableAmRoutine *am);
bool DuckdbIsAlterTableInProgress(void);
void DuckdbUnsafeSetNextExpectedCommandId(uint32_t command_id);
// Allow a PostgreSQL internal subtransaction while a DuckDB transaction is active.
// pg_ducklake uses this to wrap metadata commit writes so DuckLake's retry loop
// can catch and recover from constraint conflicts without crashing the backend.
void DuckdbAllowSubtransaction(bool allow);

#ifdef __cplusplus
} // extern "C"
#endif
