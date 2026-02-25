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

#ifdef __cplusplus
} // extern "C"
#endif
