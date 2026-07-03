#include "pgducklake/constants.hpp"
#include "pgducklake/ducklake_types.hpp"

#include <string>
#include <vector>

#include "pgddb/pgddb_types.hpp"

#include <duckdb/parser/keyword_helper.hpp>

extern "C" {
#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "pgddb/pgddb_ruleutils.h"
}

#include "pgddb/pgddb_subscript.h"
#include "pgddb/utility/cpp_wrapper.hpp"

namespace pgducklake {

// OID of the `ducklake` schema, cached after first resolution; InvalidOid before CREATE EXTENSION.
Oid
DucklakeNamespaceOid() {
	static Oid cached = InvalidOid;
	if (OidIsValid(cached))
		return cached;
	Oid nsp = get_namespace_oid(PGDUCKLAKE_PG_SCHEMA, /*missing_ok=*/true);
	if (OidIsValid(nsp))
		cached = nsp;
	return nsp;
}

static Oid
LookupDucklakeType(const char *type_name, Oid *cache) {
	if (OidIsValid(*cache))
		return *cache;
	Oid nsp = DucklakeNamespaceOid();
	if (!OidIsValid(nsp))
		return InvalidOid;
	Oid type_oid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, PointerGetDatum(type_name), ObjectIdGetDatum(nsp));
	if (OidIsValid(type_oid))
		*cache = type_oid;
	return type_oid;
}

Oid
DuckdbRowOid() {
	static Oid cached = InvalidOid;
	return LookupDucklakeType("row", &cached);
}

Oid
UnresolvedTypeOid() {
	static Oid cached = InvalidOid;
	return LookupDucklakeType("unresolved_type", &cached);
}

Oid
DuckdbStructOid() {
	static Oid cached = InvalidOid;
	return LookupDucklakeType("struct", &cached);
}

Oid
VariantOid() {
	static Oid cached = InvalidOid;
	return LookupDucklakeType("variant", &cached);
}

// libpgddb type hooks: each returns false to decline so the kernel falls through to the next hook.

// ducklake.variant is a varlena text on the PG side. Mapping it to DuckDB's
// VARIANT logical type makes DuckLake store it as variant in its catalog;
// IMPORT FOREIGN SCHEMA queries duckdb_columns() data_type to recover the
// "variant" string (DuckDB's prepared-statement layer returns VARCHAR
// regardless).
static bool
ConvertPostgresToBaseDuckColumnTypeHook(Oid pg_oid, duckdb::LogicalType &out) {
	if (OidIsValid(pg_oid) && pg_oid == VariantOid()) {
		out = duckdb::LogicalType::VARIANT();
		return true;
	}
	return false;
}

static bool
GetPostgresDuckDBTypeHook(const duckdb::LogicalType &type, Oid &out) {
	switch (type.id()) {
	case duckdb::LogicalTypeId::STRUCT:
	case duckdb::LogicalTypeId::UNION:
	case duckdb::LogicalTypeId::MAP: {
		Oid struct_oid = DuckdbStructOid();
		if (OidIsValid(struct_oid)) {
			out = struct_oid;
			return true;
		}
		return false;
	}
	case duckdb::LogicalTypeId::VARIANT: {
		Oid variant_oid = VariantOid();
		if (OidIsValid(variant_oid)) {
			out = variant_oid;
			return true;
		}
		return false;
	}
	default:
		return false;
	}
}

static bool
ConvertDuckToPostgresValueHook(Oid pg_oid, duckdb::Value &value, TupleTableSlot *slot, uint64_t col) {
	if (OidIsValid(pg_oid) && (pg_oid == DuckdbStructOid() || pg_oid == VariantOid())) {
		slot->tts_values[col] = pgddb::ConvertToStringDatum(value);
		return true;
	}
	return false;
}

// pgddb_ruleutils.h deparse hooks: each declines for non-pg_ducklake nodes so the kernel falls through.

static bool
IsFakeTypeHook(Oid type_oid) {
	return OidIsValid(type_oid) && (type_oid == DuckdbRowOid() || type_oid == UnresolvedTypeOid() ||
	                                type_oid == DuckdbStructOid() || type_oid == VariantOid());
}

// Returning -1 suppresses the "::ducklake.variant" cast get_const_expr would
// otherwise emit on variant literals, which DuckDB can't resolve.
static int
ShowTypeHook(Const *constval, int original_showtype) {
	if (constval && IsFakeTypeHook(constval->consttype))
		return -1;
	return original_showtype;
}

static bool
VarIsRowHook(Var *var) {
	return var && var->vartype == DuckdbRowOid();
}

static bool
FuncReturnsRowHook(RangeTblFunction *rtfunc) {
	if (rtfunc && rtfunc->funcexpr && IsA(rtfunc->funcexpr, FuncExpr)) {
		FuncExpr *fexpr = castNode(FuncExpr, rtfunc->funcexpr);
		if (fexpr->funcresulttype == DuckdbRowOid())
			return true;
	}
	return false;
}

// Deparse `r['col']` on a duckdb_row Var as `r.col` for DuckDB, where r is a
// FROM-clause function alias expanding to real columns. Strips the first index
// (trailing nested subscripts still print as `[...]`); returns sbsref unchanged to decline.
static SubscriptingRef *
StripFirstSubscriptHook(SubscriptingRef *sbsref, StringInfo buf) {
	if (!sbsref || !IsA(sbsref->refexpr, Var)) {
		return sbsref;
	}
	Var *var = (Var *)sbsref->refexpr;
	if (var->vartype != DuckdbRowOid()) {
		return sbsref;
	}
	if (sbsref->refupperindexpr == NIL) {
		return sbsref;
	}
	Const *first = castNode(Const, linitial(sbsref->refupperindexpr));
	Oid typoutput;
	bool typIsVarlena;
	getTypeOutputInfo(first->consttype, &typoutput, &typIsVarlena);
	char *colname = OidOutputFunctionCall(typoutput, first->constvalue);
	appendStringInfo(buf, ".%s", quote_identifier(colname));

	SubscriptingRef *shorter = (SubscriptingRef *)copyObjectImpl(sbsref);
	shorter->refupperindexpr = list_delete_first(shorter->refupperindexpr);
	if (shorter->reflowerindexpr) {
		shorter->reflowerindexpr = list_delete_first(shorter->reflowerindexpr);
	}
	return shorter;
}

// Map ducklake.variant to "VARIANT" in the CREATE TABLE deparser so DuckDB stores
// LogicalTypeId::VARIANT instead of falling back to VARCHAR. A DuckdbRuleutils virtual
// override (not a registration hook) since the deparser is invoked directly via pgducklake::Ruleutils.
char *
Ruleutils::column_type_name(Oid type_oid, int32_t /*typemod*/) {
	if (OidIsValid(type_oid) && type_oid == VariantOid()) {
		return pstrdup("VARIANT");
	}
	return NULL;
}

// Render a CALL argument Datum as a DuckDB SQL literal. Numeric types print
// bare; everything else is single-quoted via DuckDB's KeywordHelper.
static std::string
DatumToSqlLiteral(Datum value, Oid type_oid, bool isnull) {
	if (isnull)
		return "NULL";

	Oid typoutput;
	bool typisvarlena;
	getTypeOutputInfo(type_oid, &typoutput, &typisvarlena);
	char *val_str = OidOutputFunctionCall(typoutput, value);

	std::string result;
	switch (type_oid) {
	case BOOLOID:
	case INT2OID:
	case INT4OID:
	case INT8OID:
	case FLOAT4OID:
	case FLOAT8OID:
	case NUMERICOID:
		result = val_str;
		break;
	default:
		result = duckdb::KeywordHelper::WriteQuoted(val_str);
		break;
	}

	pfree(val_str);
	return result;
}

// Deparse a CALL of a ducklake-only procedure into the DuckDB statement
// "CALL pgducklake.<proc>(args, named => ...)". See Ruleutils in the header.
std::string
Ruleutils::get_calldef(CallStmt *call) {
	FuncExpr *funcexpr = call->funcexpr;

	char *proc_name = get_func_name(funcexpr->funcid);
	if (!proc_name)
		elog(ERROR, "could not find procedure with OID %u", funcexpr->funcid);

	std::vector<std::string> positional_args;
	std::string named_params;
	ListCell *lc;

	foreach (lc, funcexpr->args) {
		Node *arg = (Node *)lfirst(lc);

		if (!IsA(arg, Const))
			ereport(ERROR,
			        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("non-constant arguments are not supported in "
			                                                        "DuckDB-routed procedures")));

		Const *c = (Const *)arg;

		if (c->consttype == REGCLASSOID && !c->constisnull) {
			Oid relid = DatumGetObjectId(c->constvalue);
			char *table_name = get_rel_name(relid);
			if (!table_name)
				elog(ERROR, "could not find relation with OID %u", relid);
			char *schema_name = get_namespace_name(get_rel_namespace(relid));
			if (!schema_name)
				elog(ERROR, "could not find namespace for relation with OID %u", relid);

			named_params += ", table_name => " + duckdb::KeywordHelper::WriteQuoted(table_name);
			named_params += ", schema => " + duckdb::KeywordHelper::WriteQuoted(schema_name);
		} else if (c->consttype == REGNAMESPACEOID && !c->constisnull) {
			Oid nspid = DatumGetObjectId(c->constvalue);
			char *schema_name = get_namespace_name(nspid);
			if (!schema_name)
				elog(ERROR, "could not find namespace with OID %u", nspid);

			named_params += ", schema => " + duckdb::KeywordHelper::WriteQuoted(schema_name);
		} else {
			positional_args.push_back(DatumToSqlLiteral(c->constvalue, c->consttype, c->constisnull));
		}
	}

	std::string args_joined;
	for (size_t i = 0; i < positional_args.size(); i++) {
		if (i > 0)
			args_joined += ", ";
		args_joined += positional_args[i];
	}

	return "CALL " PGDUCKLAKE_DUCKDB_CATALOG "." + duckdb::KeywordHelper::WriteOptionallyQuoted(proc_name) + "(" +
	       args_joined + named_params + ")";
}

void
InitTypeHooks() {
	pgddb::Register_ConvertPostgresToBaseDuckColumnType(ConvertPostgresToBaseDuckColumnTypeHook);
	pgddb::Register_GetPostgresDuckDBType(GetPostgresDuckDBTypeHook);
	pgddb::Register_ConvertDuckToPostgresValue(ConvertDuckToPostgresValueHook);

	// DuckLake catalog queries return unsupported-precision NUMERICs (e.g. SUM()
	// aggregates in the count(*) row-count optimization) that must map to DOUBLE.
	pgddb::convert_unsupported_numeric_to_double = true;

	Register_pgddb_is_fake_type(IsFakeTypeHook);
	Register_pgddb_show_type(ShowTypeHook);
	Register_pgddb_var_is_duckdb_row(VarIsRowHook);
	Register_pgddb_func_returns_duckdb_row(FuncReturnsRowHook);
	Register_pgddb_strip_first_subscript(StripFirstSubscriptHook);

	// Subscripting any ducklake container (row['col'], struct['f']) resolves to
	// ducklake.unresolved_type, which carries the casts so r['col']::int parses.
	pgddb::pg::subscript_refrestype_hook = [](Oid) {
		return UnresolvedTypeOid();
	};
}

} // namespace pgducklake

extern "C" {

DECLARE_PG_FUNCTION(ducklake_row_in) {
	elog(ERROR, "Creating the ducklake.row type is not supported");
}

DECLARE_PG_FUNCTION(ducklake_row_out) {
	elog(ERROR, "Converting a ducklake.row to a string is not supported");
}

DECLARE_PG_FUNCTION(ducklake_row_subscript) {
	PG_RETURN_POINTER(&pgddb::pg::duckdb_row_subscript_routines);
}

DECLARE_PG_FUNCTION(ducklake_unresolved_type_in) {
	return textin(fcinfo);
}

DECLARE_PG_FUNCTION(ducklake_unresolved_type_out) {
	return textout(fcinfo);
}

DECLARE_PG_FUNCTION(ducklake_unresolved_type_subscript) {
	PG_RETURN_POINTER(&pgddb::pg::duckdb_unresolved_type_subscript_routines);
}

DECLARE_PG_FUNCTION(ducklake_unresolved_type_operator) {
	elog(ERROR, "ducklake.unresolved_type values can only be used in DuckDB execution");
}

DECLARE_PG_FUNCTION(ducklake_struct_in) {
	elog(ERROR, "Creating the ducklake.struct type is not supported");
}

DECLARE_PG_FUNCTION(ducklake_struct_out) {
	return textout(fcinfo);
}

DECLARE_PG_FUNCTION(ducklake_struct_subscript) {
	PG_RETURN_POINTER(&pgddb::pg::duckdb_struct_subscript_routines);
}

DECLARE_PG_FUNCTION(ducklake_variant_in) {
	return DirectFunctionCall1(textin, PG_GETARG_DATUM(0));
}

DECLARE_PG_FUNCTION(ducklake_variant_out) {
	return DirectFunctionCall1(textout, PG_GETARG_DATUM(0));
}
}
