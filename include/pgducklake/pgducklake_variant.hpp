/*
 * pgducklake_variant.hpp
 *
 * Shared helpers for the ducklake.variant type.
 *
 * Uses postgres_ext.h (not postgres.h) for Oid so this header can be
 * safely included alongside DuckDB headers without macro conflicts.
 */

#pragma once

extern "C" {
#include "postgres_ext.h"
}

namespace pgducklake {

/* Returns the OID of ducklake.variant, cached after first lookup. */
Oid GetVariantTypeOid();

} // namespace pgducklake
