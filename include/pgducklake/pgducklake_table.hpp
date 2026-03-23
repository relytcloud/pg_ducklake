/*
 * pgducklake_table.hpp -- Table lifecycle: AM handler + DDL triggers.
 *
 * Declares EnsureDuckLakeTable, used by partition and sorted_by procs.
 */
#pragma once

extern "C" {
#include "postgres.h"

/* Validates that relid uses the ducklake table AM. Errors if not. */
void EnsureDuckLakeTable(Oid relid);
}
