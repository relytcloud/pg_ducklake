/*
 * pgducklake_variant.cpp -- I/O functions and OID cache for ducklake.variant.
 *
 * @scope backend: variant type OID cache, I/O functions
 *
 * The variant type is a DuckDB-only column type for ducklake tables.
 * PG stores values as text (varlena); DuckDB handles the actual VARIANT data.
 */

#include "pgducklake/pgducklake_defs.hpp"

/* DuckDB exception.hpp must be included before cpp_wrapper.hpp (which
 * pulls in postgres.h). The #pragma once in exception.hpp then prevents
 * re-inclusion after postgres.h defines the conflicting FATAL macro. */
#include <duckdb/common/exception.hpp>

#include "pgducklake/utility/cpp_wrapper.hpp"

extern "C" {
#include "catalog/namespace.h"
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
}

#include "pgducklake/pgducklake_variant.hpp"

namespace pgducklake {

static Oid variant_type_oid = InvalidOid;

Oid GetVariantTypeOid() {
  if (!OidIsValid(variant_type_oid)) {
    Oid nsp_oid = get_namespace_oid(PGDUCKLAKE_PG_SCHEMA, true);
    if (OidIsValid(nsp_oid)) {
      variant_type_oid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid,
                                         CStringGetDatum("variant"),
                                         ObjectIdGetDatum(nsp_oid));
    }
  }
  return variant_type_oid;
}

} // namespace pgducklake

extern "C" {

DECLARE_PG_FUNCTION(ducklake_variant_in) {
  return DirectFunctionCall1(textin, PG_GETARG_DATUM(0));
}

DECLARE_PG_FUNCTION(ducklake_variant_out) {
  return DirectFunctionCall1(textout, PG_GETARG_DATUM(0));
}

} // extern "C"
