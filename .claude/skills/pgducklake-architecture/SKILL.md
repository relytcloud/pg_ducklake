---
name: pgducklake-architecture
description: Architecture guide for pg_ducklake. Use when changing extension lifecycle, metadata manager behavior, DDL trigger flow, DuckDB bridge logic, or adding/moving static state between scopes.
user-invocable: false
---

Use this skill when a task touches architecture or behavior across PostgreSQL and DuckDB boundaries.

## Focus

- Extension lifecycle and initialization order
- Object lifetime scopes (backend, DuckDB instance, extension, transaction)
- Metadata manager responsibilities and SPI routing
- DDL trigger responsibilities and constraints
- `DucklakeMetadataManager` API boundaries

## Workflow

1. Read [architecture-overview.md](architecture-overview.md) first.
2. If the change involves static state, registration, or object lifetime, read [lifecycle-scopes.md](lifecycle-scopes.md) to determine which scope the state belongs to.
3. If the change touches metadata writes or DDL-triggered DuckDB operations, also read [mixed-write-guard.md](mixed-write-guard.md).
4. Preserve current contracts unless the user explicitly asks for behavior changes.
5. Update the relevant reference file if architecture changes are made.

## Guardrails

- Keep `pgducklake_duckdb.cpp` PostgreSQL-header-free.
- Keep metadata query routing through SPI unless intentionally redesigning it.
- Prefer additive changes to extension lifecycle.
- When adding static state, verify which lifecycle scope it belongs to using the decision guide in [lifecycle-scopes.md](lifecycle-scopes.md).
