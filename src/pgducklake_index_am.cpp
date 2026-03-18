/*
 * pgducklake_index_am.cpp -- Shared dummy index AM routines.
 *
 * Provides minimal IndexAmRoutine implementations for ducklake_sorted and
 * ducklake_partitioned.  Both AMs store no data and are never used by the
 * planner; they exist only as catalog markers.  The actual DuckDB commands
 * are issued by HandleCreateSortedIndex / HandleCreatePartitionedIndex in
 * their respective translation units.
 */

extern "C" {
#include "postgres.h"

#include "access/amapi.h"
#include "access/relation.h"
#include "nodes/pathnodes.h"
#include "utils/selfuncs.h"

/* ================================================================
 * Index AM routines (shared by both AMs)
 * ================================================================ */

PG_FUNCTION_INFO_V1(ducklake_sorted_am_handler);
PG_FUNCTION_INFO_V1(ducklake_partitioned_am_handler);

static IndexBuildResult *dummy_ambuild(Relation heap, Relation index,
                                       IndexInfo *indexInfo) {
  IndexBuildResult *result = (IndexBuildResult *)palloc0(sizeof(IndexBuildResult));
  result->heap_tuples = 0;
  result->index_tuples = 0;
  return result;
}

static void dummy_ambuildempty(Relation index) {}

static bool dummy_aminsert(Relation rel, Datum *values, bool *isnull,
                           ItemPointer ht_ctid, Relation heapRel,
                           IndexUniqueCheck checkUnique,
                           bool indexUnchanged,
                           IndexInfo *indexInfo) {
  return false;
}

static IndexBulkDeleteResult *dummy_ambulkdelete(IndexVacuumInfo *info,
                                                 IndexBulkDeleteResult *stats,
                                                 IndexBulkDeleteCallback callback,
                                                 void *callback_state) {
  return stats;
}

static IndexBulkDeleteResult *dummy_amvacuumcleanup(IndexVacuumInfo *info,
                                                    IndexBulkDeleteResult *stats) {
  return stats;
}

static void dummy_amcostestimate(PlannerInfo *root, IndexPath *path,
                                 double loop_count,
                                 Cost *indexStartupCost,
                                 Cost *indexTotalCost,
                                 Selectivity *indexSelectivity,
                                 double *indexCorrelation,
                                 double *indexPages) {
  *indexStartupCost = 1.0e10;
  *indexTotalCost = 1.0e10;
  *indexSelectivity = 1.0;
  *indexCorrelation = 0.0;
  *indexPages = 0;
}

static bytea *dummy_amoptions(Datum reloptions, bool validate) {
  return NULL;
}

static bool dummy_amvalidate(Oid opclassoid) {
  return true;
}

static IndexScanDesc dummy_ambeginscan(Relation rel, int nkeys, int norderbys) {
  return RelationGetIndexScan(rel, nkeys, norderbys);
}

static void dummy_amrescan(IndexScanDesc scan, ScanKey keys, int nkeys,
                           ScanKey orderbys, int norderbys) {}

static void dummy_amendscan(IndexScanDesc scan) {}

static void fill_common_amroutine(IndexAmRoutine *amroutine) {
  amroutine->amstrategies = 0;
  amroutine->amsupport = 0;
  amroutine->amoptsprocnum = 0;
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

  amroutine->ambuild = dummy_ambuild;
  amroutine->ambuildempty = dummy_ambuildempty;
  amroutine->aminsert = dummy_aminsert;
  amroutine->ambulkdelete = dummy_ambulkdelete;
  amroutine->amvacuumcleanup = dummy_amvacuumcleanup;
  amroutine->amcanreturn = NULL;
  amroutine->amcostestimate = dummy_amcostestimate;
  amroutine->amoptions = dummy_amoptions;
  amroutine->amproperty = NULL;
  amroutine->ambuildphasename = NULL;
  amroutine->amvalidate = dummy_amvalidate;
  amroutine->ambeginscan = dummy_ambeginscan;
  amroutine->amrescan = dummy_amrescan;
  amroutine->amgettuple = NULL;
  amroutine->amgetbitmap = NULL;
  amroutine->amendscan = dummy_amendscan;
  amroutine->ammarkpos = NULL;
  amroutine->amrestrpos = NULL;
  amroutine->amestimateparallelscan = NULL;
  amroutine->aminitparallelscan = NULL;
  amroutine->amparallelrescan = NULL;
}

Datum ducklake_sorted_am_handler(PG_FUNCTION_ARGS) {
  IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);
  fill_common_amroutine(amroutine);
  amroutine->amcanorder = true;
  PG_RETURN_POINTER(amroutine);
}

Datum ducklake_partitioned_am_handler(PG_FUNCTION_ARGS) {
  IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);
  fill_common_amroutine(amroutine);
  amroutine->amcanorder = false;
  PG_RETURN_POINTER(amroutine);
}

} /* extern "C" */
