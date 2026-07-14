#include "pgddb/pg/relations.hpp"

#include "pgddb/pgddb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h" // GETSTRUCT
#include "access/relation.h"     // relation_open and relation_close
#include "catalog/namespace.h"   // makeRangeVarFromNameList, RangeVarGetRelid
#include "optimizer/plancat.h"   // estimate_rel_size
#include "storage/bufmgr.h"      // RelationGetNumberOfBlocks
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/resowner.h"    // CurrentResourceOwner and TopTransactionResourceOwner
#include "executor/tuptable.h" // TupIsNull
#include "utils/syscache.h"    // RELOID
}

namespace pgddb {

#undef RelationGetDescr

#if PG_VERSION_NUM < 150000
// clang-format off

/*
 * Relation kinds with a table access method (rd_tableam).  Although sequences
 * use the heap table AM, they are enough of a special case in most uses that
 * they are not included here.  Likewise, partitioned tables can have an access
 * method defined so that their partitions can inherit it, but they do not set
 * rd_tableam; hence, this is handled specially outside of this macro.
 */
#define RELKIND_HAS_TABLE_AM(relkind) \
	((relkind) == RELKIND_RELATION || \
	 (relkind) == RELKIND_TOASTVALUE || \
	 (relkind) == RELKIND_MATVIEW)

// clang-format on
#endif

TupleDesc
RelationGetDescr(Relation rel) {
	return rel->rd_att;
}

int
GetTupleDescNatts(const TupleDesc tupleDesc) {
	return tupleDesc->natts;
}

const char *
GetAttName(const Form_pg_attribute att) {
	return NameStr(att->attname);
}

bool
AttIsDropped(const Form_pg_attribute att) {
	return att->attisdropped;
}

Form_pg_attribute
GetAttr(const TupleDesc tupleDesc, int i) {
	return TupleDescAttr(tupleDesc, i);
}

bool
TupleIsNull(TupleTableSlot *slot) {
	return TupIsNull(slot);
}

void
SlotGetAllAttrs(TupleTableSlot *slot) {
	// Safe without PostgresFunctionGuard: no allocations, no errors for minimal slots.
	slot_getallattrs(slot);
}

TupleTableSlot *
ExecStoreMinimalTupleUnsafe(MinimalTuple minmal_tuple, TupleTableSlot *slot, bool shouldFree) {
	// Safe without PostgresFunctionGuard when the slot isn't tuple-owned (TTS_SHOULDFREE false): no allocations, and
	// its only error is a programming bug (non-minimal slot).
	return ::ExecStoreMinimalTuple(minmal_tuple, slot, shouldFree);
}

Relation
OpenRelation(Oid relationId) {
	// Open & close under TopTransactionResourceOwner: PG forbids opening with one resource owner and closing with
	// another, and it may switch owners between the two calls.
	ResourceOwner saveResourceOwner = CurrentResourceOwner;
	CurrentResourceOwner = TopTransactionResourceOwner;
	auto rel = PostgresFunctionGuard(relation_open, relationId, AccessShareLock);
	CurrentResourceOwner = saveResourceOwner;
	return rel;
}

void
CloseRelation(Relation rel) {
	// Open & close under TopTransactionResourceOwner: PG forbids opening with one resource owner and closing with
	// another, and it may switch owners between the two calls.
	ResourceOwner saveResourceOwner = CurrentResourceOwner;
	CurrentResourceOwner = TopTransactionResourceOwner;
	PostgresFunctionGuard(relation_close, rel, NoLock);

	CurrentResourceOwner = saveResourceOwner;
}

uint32_t
RelationBlockCount(Relation rel) {
	return (uint32_t)PostgresFunctionGuard(RelationGetNumberOfBlocksInFork, rel, MAIN_FORKNUM);
}

bool
RelationIsTemporary(Relation rel) {
	return rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP;
}

double
EstimateRelSize(Relation rel) {
	Cardinality cardinality = 0;

	if (RELKIND_HAS_TABLE_AM(rel->rd_rel->relkind) || rel->rd_rel->relkind == RELKIND_INDEX) {
		BlockNumber pages;
		double allvisfrac;
		PostgresFunctionGuard(estimate_rel_size, rel, nullptr, &pages, &cardinality, &allvisfrac);
	}

	return cardinality;
}

static Oid
PGGetRelidFromSchemaAndTable(const char *schema_name, const char *entry_name) {
	List *name_list = NIL;
	name_list = lappend(name_list, makeString(pstrdup(schema_name)));
	name_list = lappend(name_list, makeString(pstrdup(entry_name)));
	RangeVar *table_range_var = makeRangeVarFromNameList(name_list);
	return RangeVarGetRelid(table_range_var, AccessShareLock, true);
}

Oid
GetRelidFromSchemaAndTable(const char *schema_name, const char *entry_name) {
	return PostgresFunctionGuard(PGGetRelidFromSchemaAndTable, schema_name, entry_name);
}

bool
IsValidOid(Oid oid) {
	return oid != InvalidOid;
}

bool
IsValidBlockNumber(BlockNumber block_number) {
	return block_number != InvalidBlockNumber;
}

// Schema-qualified display name for a relation.
static char *
GenerateQualifiedRelationName_Unsafe(Relation rel) {
	char *nspname = get_namespace_name_or_temp(rel->rd_rel->relnamespace);
	if (!nspname)
		elog(ERROR, "cache lookup failed for namespace %u", rel->rd_rel->relnamespace);

	return quote_qualified_identifier(nspname, NameStr(rel->rd_rel->relname));
}

char *
GenerateQualifiedRelationName(Relation rel) {
	return PostgresFunctionGuard(GenerateQualifiedRelationName_Unsafe, rel);
}

const char *
QuoteIdentifier(const char *ident) {
	return PostgresFunctionGuard(quote_identifier, ident);
}

const char *
GetRelationName(Relation rel) {
	return RelationGetRelationName(rel);
}

Oid
GetOid(Form_pg_class rel) {
	return rel->oid;
}

namespace pg {

Form_pg_attribute
GetAttributeByName(TupleDesc tupdesc, const char *colname) {
	for (int i = 0; i < tupdesc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (strcmp(NameStr(attr->attname), colname) == 0) {
			return attr;
		}
	}
	return NULL;
}

} // namespace pg

} // namespace pgddb
