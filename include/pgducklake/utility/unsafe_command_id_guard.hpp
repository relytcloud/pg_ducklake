#pragma once

extern "C" {
#include "postgres.h"

#include "access/xact.h"

void DuckdbUnsafeSetNextExpectedCommandId(uint32_t command_id);
}

namespace pgducklake {

/*
 * UnsafeCommandIdGuard - RAII guard that makes PostgreSQL command counter
 * changes within its scope invisible to pg_duckdb's mixed-write detection.
 *
 * pg_duckdb tracks next_expected_command_id to detect when both PostgreSQL
 * and DuckDB write in the same transaction (which is normally unsupported).
 * DuckLake metadata operations write to PostgreSQL tables (ducklake_*) via
 * SPI as part of DuckDB operations, which falsely triggers this detection.
 *
 * This guard synchronizes next_expected_command_id with PostgreSQL's current
 * command counter on construction and destruction, making metadata writes
 * transparent to the mixed-write check.
 *
 * WARNING: Intentionally named "Unsafe" because it bypasses a safety check.
 * Only use for DuckLake internal metadata operations.
 */
class UnsafeCommandIdGuard {
public:
  UnsafeCommandIdGuard() {}

  ~UnsafeCommandIdGuard() {
    DuckdbUnsafeSetNextExpectedCommandId(
        static_cast<uint32_t>(GetCurrentCommandId(false)));
  }

  UnsafeCommandIdGuard(const UnsafeCommandIdGuard &) = delete;
  UnsafeCommandIdGuard &operator=(const UnsafeCommandIdGuard &) = delete;
};

} // namespace pgducklake
