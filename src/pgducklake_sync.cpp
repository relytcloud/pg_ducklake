/*
 * pgducklake_sync.cpp -- Reverse sync framework: DuckDB metadata -> PG catalog.
 *
 * @scope backend: syncing_from_metadata guard bool
 * @scope duckdb-instance: ducklake_snapshot_trigger
 *
 * ducklake_snapshot_trigger fires on INSERT into ducklake.ducklake_snapshot
 * and calls all sync_handlers in order.  Add new handlers to the static
 * sync_handlers array.
 */

#include "pgducklake/pgducklake_defs.hpp"
#include "pgducklake/pgducklake_guc.hpp"
#include "pgducklake/pgducklake_sync.hpp"

#include <duckdb/common/error_data.hpp> /* must precede postgres.h (FATAL macro) */

#include "pgducklake/pgducklake_sorted_by.hpp"
#include "pgducklake/pgducklake_table.hpp"

#include "pgducklake/utility/cpp_wrapper.hpp"

#include <string>

extern "C" {
#include "postgres.h"

#include "commands/trigger.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
}

namespace pgducklake {

bool syncing_from_metadata = false;
bool skip_snapshot_sync = false;

} // namespace pgducklake

namespace {

/* Sync handlers called by ducklake_snapshot_trigger in order.
 * Add new handlers here. */
pgducklake::SyncHandler sync_handlers[] = {
    pgducklake::SyncNewTables,
    pgducklake::SyncDroppedTables,
    pgducklake::SyncSortKeys,
};

} // anonymous namespace

/* ================================================================
 * Snapshot trigger
 * ================================================================ */

extern "C" {

/*
 * ducklake_snapshot_trigger -- AFTER INSERT trigger on ducklake.ducklake_snapshot.
 *
 * Detects tables created/dropped by external DuckDB clients (which write
 * directly to the ducklake metadata tables) and creates/drops corresponding
 * pg_class entries so the tables become visible from PostgreSQL.
 * Calls all sync_handlers in order.
 */
DECLARE_PG_FUNCTION(ducklake_snapshot_trigger) {
  if (!CALLED_AS_TRIGGER(fcinfo))
    elog(ERROR, "not fired by trigger manager");

  TriggerData *trigdata = (TriggerData *)fcinfo->context;

  /* Skip sync when:
   *  - skip_snapshot_sync is set (direct insert / ExecuteCommit paths
   *    have no DDL changes to reverse-sync; running sync handlers on
   *    a DuckDB worker thread would also crash because PG's
   *    InterruptHoldoffCount is not thread-safe), or
   *  - enable_metadata_sync GUC is off (user opts out of the
   *    per-commit sync overhead when all DDL goes through PG). */
  if (pgducklake::skip_snapshot_sync || !pgducklake::enable_metadata_sync) {
    return PointerGetDatum(trigdata->tg_trigtuple);
  }

  /* Extract snapshot_id from the NEW row */
  bool isnull;
  int64 snapshot_id = DatumGetInt64(SPI_getbinval(trigdata->tg_trigtuple, trigdata->tg_relation->rd_att, 1, &isnull));
  if (isnull)
    elog(ERROR, "snapshot_id is NULL");

  SPI_connect();

  auto save_nestlevel = NewGUCNestLevel();
  SetConfigOption("duckdb.force_execution", "false", PGC_USERSET, PGC_S_SESSION);

  pgducklake::syncing_from_metadata = true;

  PG_TRY();
  {
    std::string sid = std::to_string(snapshot_id);
    for (auto handler : sync_handlers)
      handler(sid.c_str());
  }
  PG_FINALLY();
  {
    pgducklake::syncing_from_metadata = false;
  }
  PG_END_TRY();

  AtEOXact_GUC(false, save_nestlevel);
  SPI_finish();
  return PointerGetDatum(trigdata->tg_trigtuple);
}
}
