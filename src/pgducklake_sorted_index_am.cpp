/*
 * pgducklake_sorted_index_am.cpp -- Dummy index AM for DuckLake sorted tables.
 *
 * Provides a minimal IndexAmRoutine so that CREATE INDEX ... USING
 * ducklake_sorted registers a real pg_class entry. The index stores no data
 * and is never used by the planner; it exists only as a catalog marker that
 * the utility hook translates into ALTER TABLE ... SET SORTED BY in DuckDB.
 */

extern "C" {
#include "postgres.h"

#include "access/amapi.h"
#include "access/reloptions.h"
#include "nodes/pathnodes.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"

PG_FUNCTION_INFO_V1(ducklake_sorted_am_handler);

/* ---- ambuild / ambuildempty ---- */

static IndexBuildResult *sorted_ambuild(Relation heap, Relation index,
                                        IndexInfo *indexInfo) {
  IndexBuildResult *result = (IndexBuildResult *)palloc0(sizeof(IndexBuildResult));
  result->heap_tuples = 0;
  result->index_tuples = 0;
  return result;
}

static void sorted_ambuildempty(Relation index) {
  /* no-op */
}

/* ---- aminsert ---- */

static bool sorted_aminsert(Relation rel, Datum *values, bool *isnull,
                            ItemPointer ht_ctid, Relation heapRel,
                            IndexUniqueCheck checkUnique,
                            bool indexUnchanged,
                            IndexInfo *indexInfo) {
  /* no-op: inserts skip this index */
  return false;
}

/* ---- ambulkdelete / amvacuumcleanup ---- */

static IndexBulkDeleteResult *sorted_ambulkdelete(IndexVacuumInfo *info,
                                                  IndexBulkDeleteResult *stats,
                                                  IndexBulkDeleteCallback callback,
                                                  void *callback_state) {
  return stats;
}

static IndexBulkDeleteResult *sorted_amvacuumcleanup(IndexVacuumInfo *info,
                                                     IndexBulkDeleteResult *stats) {
  return stats;
}

/* ---- amcostestimate ---- */

static void sorted_amcostestimate(PlannerInfo *root, IndexPath *path,
                                  double loop_count,
                                  Cost *indexStartupCost,
                                  Cost *indexTotalCost,
                                  Selectivity *indexSelectivity,
                                  double *indexCorrelation,
                                  double *indexPages) {
  /* Return infinity so the planner never picks this index */
  *indexStartupCost = 1.0e10;
  *indexTotalCost = 1.0e10;
  *indexSelectivity = 1.0;
  *indexCorrelation = 0.0;
  *indexPages = 0;
}

/* ---- amoptions ---- */

static bytea *sorted_amoptions(Datum reloptions, bool validate) {
  /* No options supported */
  return NULL;
}

/* ---- amvalidate ---- */

static bool sorted_amvalidate(Oid opclassoid) {
  return true;
}

/* ---- scan stubs ---- */

static IndexScanDesc sorted_ambeginscan(Relation rel, int nkeys, int norderbys) {
  return RelationGetIndexScan(rel, nkeys, norderbys);
}

static void sorted_amrescan(IndexScanDesc scan, ScanKey keys, int nkeys,
                            ScanKey orderbys, int norderbys) {
  /* no-op */
}

static void sorted_amendscan(IndexScanDesc scan) {
  /* no-op */
}

/* ---- handler ---- */

Datum ducklake_sorted_am_handler(PG_FUNCTION_ARGS) {
  IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

  amroutine->amstrategies = 0;
  amroutine->amsupport = 0;
  amroutine->amoptsprocnum = 0;
  amroutine->amcanorder = true;
  amroutine->amcanorderbyop = false;
  amroutine->amcanbackward = false;
  amroutine->amcanunique = false;
  amroutine->amcanmulticol = true;
  amroutine->amoptionalkey = false;
  amroutine->amsearcharray = false;
  amroutine->amsearchnulls = false;
  amroutine->amstorage = false;
  amroutine->amclusterable = false;
  amroutine->ampredlocks = false;
  amroutine->amcanparallel = false;
#if PG_VERSION_NUM >= 170000
  amroutine->amcanbuildparallel = false;
#endif
  amroutine->amcaninclude = false;
  amroutine->amusemaintenanceworkmem = false;
#if PG_VERSION_NUM >= 180000
  amroutine->amsummarizing = false;
#else
  amroutine->amparallelvacuumoptions = 0;
#endif
  amroutine->amkeytype = InvalidOid;

  amroutine->ambuild = sorted_ambuild;
  amroutine->ambuildempty = sorted_ambuildempty;
  amroutine->aminsert = sorted_aminsert;
  amroutine->ambulkdelete = sorted_ambulkdelete;
  amroutine->amvacuumcleanup = sorted_amvacuumcleanup;
  amroutine->amcanreturn = NULL;
  amroutine->amcostestimate = sorted_amcostestimate;
  amroutine->amoptions = sorted_amoptions;
  amroutine->amproperty = NULL;
  amroutine->ambuildphasename = NULL;
  amroutine->amvalidate = sorted_amvalidate;
  amroutine->ambeginscan = sorted_ambeginscan;
  amroutine->amrescan = sorted_amrescan;
  amroutine->amgettuple = NULL;
  amroutine->amgetbitmap = NULL;
  amroutine->amendscan = sorted_amendscan;
  amroutine->ammarkpos = NULL;
  amroutine->amrestrpos = NULL;
  amroutine->amestimateparallelscan = NULL;
  amroutine->aminitparallelscan = NULL;
  amroutine->amparallelrescan = NULL;

  PG_RETURN_POINTER(amroutine);
}

} /* extern "C" */
