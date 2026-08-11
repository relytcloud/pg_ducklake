#pragma once

#include "pgddb/pg/declarations.hpp"

extern "C" {
void EnsureDuckLakeTable(Oid relid);
}

namespace pgducklake {

/* Preserve a CTAS query before PostgreSQL's planner destructively rewrites it.
 * The utility hook pushes it and the create-table event trigger consumes it. */
void PushPendingCtasQuery(Query *query);
Query *TakePendingCtasQuery();
void RemovePendingCtasQuery(Query *query);

/* Caller must have an active SPI connection. */
void SyncNewTables(const char *snapshot_id);
void SyncDroppedTables(const char *snapshot_id);

} // namespace pgducklake
