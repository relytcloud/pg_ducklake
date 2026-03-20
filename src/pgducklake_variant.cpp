/*
 * pgducklake_variant.cpp -- I/O functions for the ducklake.variant type.
 *
 * The variant type is a DuckDB-only column type for ducklake tables.
 * PG stores values as text (varlena); DuckDB handles the actual VARIANT data.
 */

#include <duckdb/common/exception.hpp>

#include "pgducklake/utility/cpp_wrapper.hpp"

extern "C" {
#include "utils/builtins.h"
}

extern "C" {

DECLARE_PG_FUNCTION(ducklake_variant_in) {
  return DirectFunctionCall1(textin, PG_GETARG_DATUM(0));
}

DECLARE_PG_FUNCTION(ducklake_variant_out) {
  return DirectFunctionCall1(textout, PG_GETARG_DATUM(0));
}

} // extern "C"
